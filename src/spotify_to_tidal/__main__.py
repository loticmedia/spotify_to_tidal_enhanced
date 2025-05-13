import yaml
import argparse
import sys
import asyncio
import time
from pathlib import Path
from collections import defaultdict
import datetime
import sqlalchemy
from sqlalchemy import Table, Column, String, DateTime, MetaData, insert, select, update

from . import sync as _sync
from . import auth as _auth
from .type.spotify import get_saved_tracks

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn, BarColumn, TaskProgressColumn

# --- Unified Review Database ---
class ReviewDatabase:
    def __init__(self, filename='.cache.db'):
        self.engine = sqlalchemy.create_engine(f"sqlite:///{filename}")
        self.meta = MetaData()
        self.table = Table(
            'review_log', self.meta,
            Column('track_key', String, primary_key=True),
            Column('status', String),
            Column('insert_time', DateTime),
            Column('next_retry', DateTime)
        )
        self.meta.create_all(self.engine)

    def reset(self):
        with self.engine.begin() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS review_log"))
        self.__init__()

    def _compute_next_retry(self, insert_time):
        interval = 2 * (datetime.datetime.now() - insert_time)
        return datetime.datetime.now() + interval

    def set_approved(self, track_key):
        with self.engine.connect() as conn:
            with conn.begin():
                stmt = select(self.table).where(self.table.c.track_key == track_key)
                existing = conn.execute(stmt).fetchone()
                if existing:
                    upd = update(self.table).where(self.table.c.track_key == track_key).values(status='approved', insert_time=datetime.datetime.now(), next_retry=None)
                    conn.execute(upd)
                else:
                    conn.execute(insert(self.table), {
                        'track_key': track_key,
                        'status': 'approved',
                        'insert_time': datetime.datetime.now(),
                        'next_retry': None
                    })

    def set_unapproved(self, track_key):
        with self.engine.connect() as conn:
            with conn.begin():
                stmt = select(self.table).where(self.table.c.track_key == track_key)
                existing = conn.execute(stmt).fetchone()
                now = datetime.datetime.now()
                if existing:
                    next_retry = self._compute_next_retry(existing.insert_time or now)
                    conn.execute(update(self.table).where(self.table.c.track_key == track_key).values(status='unapproved', insert_time=now, next_retry=next_retry))
                else:
                    conn.execute(insert(self.table), {
                        'track_key': track_key,
                        'status': 'unapproved',
                        'insert_time': now,
                        'next_retry': now + datetime.timedelta(days=7)
                    })

    def get_status(self, track_key):
        stmt = select(self.table.c.status).where(self.table.c.track_key == track_key)
        with self.engine.connect() as conn:
            row = conn.execute(stmt).fetchone()
            return row.status if row else 'none'

    def should_retry(self, track_key):
        stmt = select(self.table.c.next_retry).where(self.table.c.track_key == track_key)
        with self.engine.connect() as conn:
            row = conn.execute(stmt).fetchone()
            return bool(row and row.next_retry <= datetime.datetime.now())

review_db = ReviewDatabase()

# --- Helpers ---
def normalize(s):
    return s.strip().lower()

def get_all_tidal_favorite_tracks(user, limit=100):
    all_tracks = []
    offset = 0
    while True:
        page = user.favorites.tracks(limit=limit, offset=offset)
        if not page:
            break
        all_tracks.extend(page)
        offset += len(page)
    return all_tracks

def group_tracks_by_artist(tracks):
    artist_map = defaultdict(list)
    for item in tracks:
        track = item.get('track')
        if not track or not track.get('artists'):
            continue
        artist = track['artists'][0].get('name') or 'Unknown'
        artist_map[artist].append(track)
    return artist_map

async def add_track_async(session, tid):
    await asyncio.to_thread(session.user.favorites.add_track, tid)

async def auto_add_albums_with_multiple_tracks_async(tracks, tidal_session, artist_name):
    album_counts = defaultdict(list)
    for t in tracks:
        album = t.get('album') or {}
        key = normalize(album.get('name', ''))
        album_counts[key].append(t)

    async def add_album(album_name, track_list):
        if len(track_list) < 3:
            return
        print(f"\U0001F4BF Adding album '{album_name}' to TIDAL favorites... ({len(track_list)} tracks)")

        def normalize_artist_name(name: str) -> str:
            name = name.lower().replace(" and ", " & ").replace("&", " and ")
            return normalize(name)

        try:
            results = tidal_session.search(album_name) or {}
            albums = results.get('albums', [])
            matches = [
                a for a in albums
                if normalize_artist_name(a.artist.name) == normalize_artist_name(artist_name)
            ]
            if matches:
                await asyncio.to_thread(tidal_session.user.favorites.add_album, matches[0].id)
            else:
                print(f"⚠️ No match for '{album_name}' by {artist_name}")
        except Exception as e:
            print(f"❌ Error adding album '{album_name}': {e}")


    await asyncio.gather(*(add_album(name, tracks) for name, tracks in album_counts.items()))

async def migrate_saved_tracks(spotify_session, tidal_session):
    print("Fetching saved Spotify tracks...")
    saved_tracks = get_saved_tracks(spotify_session)
    artist_groups = group_tracks_by_artist(saved_tracks)

    console = Console()
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        transient=True,
        console=console
    ) as progress:
        task = progress.add_task("📡 Fetching saved TIDAL tracks...", start=True)
        existing = await asyncio.to_thread(get_all_tidal_favorite_tracks, tidal_session.user)
        progress.update(task, completed=1)

    existing_titles = {f"{normalize(t.name)}|{normalize(t.artist.name)}" for t in existing}
    print(f"✅ Loaded {len(existing_titles)} TIDAL favorites.")

    for artist in sorted(artist_groups):
        tracks = artist_groups[artist]
        keys = [f"{normalize(t['name'])}|{normalize(artist)}" for t in tracks]

        all_in_existing = all(k in existing_titles for k in keys)
        all_approved_or_existing = all(k in existing_titles or review_db.get_status(k) == 'approved' for k in keys)
        all_unapproved_or_existing = all(k in existing_titles or review_db.get_status(k) == 'unapproved' for k in keys)
        all_skipped_and_not_retry = all(review_db.get_status(k) == 'skipped' and not review_db.should_retry(k) for k in keys)

        if all_in_existing:
            print(f"⏭️  Skipping {artist}: all tracks already in TIDAL")
            await auto_add_albums_with_multiple_tracks_async(tracks, tidal_session, artist)
            continue
        if all_approved_or_existing:
            print(f"⏭️  Skipping {artist}: all tracks approved or already in TIDAL")
            await auto_add_albums_with_multiple_tracks_async(tracks, tidal_session, artist)
            continue
        if all_unapproved_or_existing:
            print(f"⏭️  Skipping {artist}: all tracks unapproved or already in TIDAL")
            continue
        if all_skipped_and_not_retry:
            print(f"⏭️  Skipping {artist}: all tracks skipped and not due for retry")
            continue


        print(f"\n🎤 Artist: {artist} ({len(tracks)} tracks)")
        for t in tracks[:5]:
            print(f"  • '{t['name']}' — {t['album']['name']}")
        resp = input(f"Approve and add 🎹 {artist.upper()}? [y/N]: ").strip().lower()
        if resp == 'y':
            print(f"🔄 Syncing {artist}...")
            _sync.populate_track_match_cache(tracks, [])

            await _sync.search_new_tracks_on_tidal(tidal_session, tracks, artist, {})

            matched = _sync.get_tracks_for_new_tidal_playlist(tracks)
            await asyncio.gather(*(add_track_async(tidal_session, tid) for tid in matched))
            await auto_add_albums_with_multiple_tracks_async(tracks, tidal_session, artist)
            existing_titles.update(keys)
            for k in keys:
                review_db.set_approved(k)
        elif resp == 'n':
            for k in keys:
                review_db.set_unapproved(k)

# --- Main Entrypoint ---
def main():
    parser = argparse.ArgumentParser(description="Sync or migrate Spotify data to TIDAL")
    parser.add_argument('--config', default='config.yml')
    parser.add_argument('--sync-favorites', action=argparse.BooleanOptionalAction)
    parser.add_argument('--migrate-saved-tracks', action='store_true')
    parser.add_argument('--reset-db', action='store_true')
    args = parser.parse_args()

    if args.reset_db:
        print("⚠️ Resetting the review database...")
        review_db.reset()
        print("✅ Review database has been reset.")
        return

    with open(args.config, 'r') as f:
        cfg = yaml.safe_load(f)

    print("Opening Spotify session")
    spotify_session = _auth.open_spotify_session(cfg['spotify'])
    print("Opening TIDAL session")
    tidal_session = _auth.open_tidal_session()
    if not tidal_session.check_login():
        sys.exit("Could not connect to TIDAL")

    if not args.migrate_saved_tracks:
        _sync.sync_playlists_wrapper(spotify_session, tidal_session, _sync.get_user_playlist_mappings(spotify_session, tidal_session, cfg), cfg)
        if args.sync_favorites is None and cfg.get('sync_favorites_default', True):
            _sync.sync_favorites_wrapper(spotify_session, tidal_session, cfg)
    else:
        asyncio.run(migrate_saved_tracks(spotify_session, tidal_session))

if __name__ == '__main__':
    main()
