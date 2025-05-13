import yaml
import argparse
import sys
import asyncio
import time
from pathlib import Path
from collections import defaultdict
import datetime
import sqlalchemy
from sqlalchemy import Table, Column, String, DateTime, MetaData, insert, select, update, delete

from . import sync as _sync
from . import auth as _auth
from .type.spotify import get_saved_tracks

# --- Unified Review Database: stores track statuses (approved/skipped) and retry info ---
class ReviewDatabase:
    """
    SQLite database of track review statuses: 'approved', 'skipped', or 'unapproved'.
    For skipped tracks, next_retry dictates when to retry.
    """
    def __init__(self, filename='.cache.db'):
        self.engine = sqlalchemy.create_engine(f"sqlite:///{filename}")
        meta = MetaData()
        self.table = Table(
            'review_log', meta,
            Column('track_key', String, primary_key=True),
            Column('status', String),  # 'approved', 'skipped', or 'unapproved'
            Column('insert_time', DateTime),
            Column('next_retry', DateTime),
            sqlite_autoincrement=False
        )
        meta.create_all(self.engine)

    def _compute_next_retry(self, insert_time: datetime.datetime) -> datetime.datetime:
        # exponential backoff: double the elapsed since insert
        interval = 2 * (datetime.datetime.now() - insert_time)
        return datetime.datetime.now() + interval

    def set_approved(self, track_key: str):
        """Mark track as approved."""
        with self.engine.connect() as conn:
            with conn.begin():
                stmt = select(self.table).where(self.table.c.track_key == track_key)
                existing = conn.execute(stmt).fetchone()
                if existing:
                    upd = update(self.table).where(
                        self.table.c.track_key == track_key
                    ).values(status='approved', timestamp=datetime.datetime.now(), next_retry=None)
                    conn.execute(upd)
                else:
                    ins = insert(self.table)
                    conn.execute(ins, {
                        'track_key': track_key,
                        'status': 'approved',
                        'insert_time': datetime.datetime.now(),
                        'next_retry': None
                    })

    def set_skipped(self, track_key: str):
        """Mark track as skipped and compute next retry."""
        with self.engine.connect() as conn:
            with conn.begin():
                stmt = select(self.table).where(self.table.c.track_key == track_key)
                existing = conn.execute(stmt).fetchone()
                now = datetime.datetime.now()
                if existing:
                    next_retry = self._compute_next_retry(existing.insert_time or now)
                    upd = update(self.table).where(
                        self.table.c.track_key == track_key
                    ).values(status='skipped', timestamp=now, next_retry=next_retry)
                    conn.execute(upd)
                else:
                    ins = insert(self.table)
                    conn.execute(ins, {
                        'track_key': track_key,
                        'status': 'skipped',
                        'insert_time': now,
                        'next_retry': now + datetime.timedelta(days=7)
                    })

    def set_unapproved(self, track_key: str):
        """Mark track as unapproved and compute next retry."""
        with self.engine.connect() as conn:
            with conn.begin():
                stmt = select(self.table).where(self.table.c.track_key == track_key)
                existing = conn.execute(stmt).fetchone()
                now = datetime.datetime.now()
                if existing:
                    next_retry = self._compute_next_retry(existing.insert_time or now)
                    upd = update(self.table).where(
                        self.table.c.track_key == track_key
                    ).values(status='unapproved', timestamp=now, next_retry=next_retry)
                    conn.execute(upd)
                else:
                    ins = insert(self.table)
                    conn.execute(ins, {
                        'track_key': track_key,
                        'status': 'unapproved',
                        'insert_time': now,
                        'next_retry': now + datetime.timedelta(days=7)
                    })

    def get_status(self, track_key: str) -> str:
        """Return status ('approved', 'skipped', or 'unapproved')."""
        stmt = select(self.table.c.status).where(self.table.c.track_key == track_key)
        with self.engine.connect() as conn:
            row = conn.execute(stmt).fetchone()
            return row.status if row else 'none'

    def should_retry(self, track_key: str) -> bool:
        """Return True if skipped and next_retry <= now."""
        stmt = select(self.table.c.next_retry).where(self.table.c.track_key == track_key)
        with self.engine.connect() as conn:
            row = conn.execute(stmt).fetchone()
            return bool(row and row.next_retry <= datetime.datetime.now())

# Singleton instance
review_db = ReviewDatabase()

# --- Helpers ---

def normalize(s: str) -> str:
    return s.strip().lower()


def get_all_tidal_favorite_tracks(user, limit: int = 100) -> list:
    all_tracks = []
    offset = 0
    while True:
        page = user.favorites.tracks(limit=limit, offset=offset)
        if not page:
            break
        all_tracks.extend(page)
        offset += len(page)
    return all_tracks


def group_tracks_by_artist(tracks: list) -> dict:
    artist_map = defaultdict(list)
    for item in tracks:
        track = item.get('track')
        if not track or not track.get('artists'):
            continue
        artist = track['artists'][0].get('name') or 'Unknown'
        artist_map[artist].append(track)
    return artist_map


def auto_add_albums_with_multiple_tracks(tracks: list, tidal_session, artist_name: str) -> None:
    album_counts = defaultdict(list)
    for t in tracks:
        album = t.get('album') or {}
        key = normalize(album.get('name', ''))
        album_counts[key].append(t)
    for album_name, track_list in album_counts.items():
        if len(track_list) < 3:
            continue
        print(f"💿 Adding album '{album_name}' to TIDAL favorites... ({len(track_list)} tracks)")
        try:
            results = tidal_session.search(album_name) or {}
            albums = results.get('albums', [])
            matches = [a for a in albums if normalize(a.artist.name) == normalize(artist_name)]
            if matches:
                tidal_session.user.favorites.add_album(matches[0].id)
            else:
                print(f"⚠️ No match for '{album_name}' by {artist_name}")
        except Exception as e:
            print(f"❌ Error adding album '{album_name}': {e}")

# --- Migrate Saved Tracks Path ---

def migrate_saved_tracks(spotify_session, tidal_session) -> None:
    print("Fetching saved Spotify tracks...")
    saved_tracks = get_saved_tracks(spotify_session)
    artist_groups = group_tracks_by_artist(saved_tracks)

    print("📡 Fetching saved TIDAL tracks...")
    existing = get_all_tidal_favorite_tracks(tidal_session.user)
    existing_titles = {f"{normalize(t.name)}|{normalize(t.artist.name)}" for t in existing}
    print(f"✅ Loaded {len(existing_titles)} TIDAL favorites.")

    for artist in sorted(artist_groups):
        tracks = artist_groups[artist]
        keys = [f"{normalize(t['name'])}|{normalize(artist)}" for t in tracks]
        # skip if already approved or still waiting to retry skipped
        if all(review_db.get_status(k) == 'approved' or k in existing_titles for k in keys):
            continue
        if all(review_db.get_status(k) == 'unapproved' or k in existing_titles for k in keys):
            continue
        if all(review_db.get_status(k) == 'skipped' and not review_db.should_retry(k) for k in keys):
            continue

        print(f"\n🎤 Artist: {artist} ({len(tracks)} tracks)")
        for t in tracks[:5]:
            print(f"  • '{t['name']}' — {t['album']['name']}")
        resp = input("Approve and add? [y/N]: ").strip().lower()
        if resp == 'y':
            print(f"🔄 Syncing {artist}...")
            _sync.populate_track_match_cache(tracks, [])
            asyncio.run(_sync.search_new_tracks_on_tidal(tidal_session, tracks, artist, {}))
            matched = _sync.get_tracks_for_new_tidal_playlist(tracks)
            for tid in matched:
                tidal_session.user.favorites.add_track(tid)
            auto_add_albums_with_multiple_tracks(tracks, tidal_session, artist)
            existing_titles.update(keys)
            for k in keys:
                review_db.set_approved(k)
        elif resp == "n":
            for k in keys:
                review_db.set_unapproved(k)

# --- Main Entrypoint ---

def main() -> None:
    parser = argparse.ArgumentParser(description="Sync or migrate Spotify data to TIDAL")
    parser.add_argument('--config', default='config.yml', help='Path to config YAML')
    parser.add_argument('--sync-favorites', action=argparse.BooleanOptionalAction, help='Synchronize saved tracks as favorites')
    parser.add_argument('--migrate-saved-tracks', action='store_true', help='Review and migrate saved Spotify tracks')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        cfg = yaml.safe_load(f)

    print("Opening Spotify session")
    spotify_session = _auth.open_spotify_session(cfg['spotify'])
    print("Opening TIDAL session")
    tidal_session = _auth.open_tidal_session()
    if not tidal_session.check_login():
        sys.exit("Could not connect to TIDAL")

    if not args.migrate_saved_tracks:
        _sync.sync_playlists_wrapper(
            spotify_session, tidal_session,
            _sync.get_user_playlist_mappings(spotify_session, tidal_session, cfg), cfg
        )
        if args.sync_favorites is None and cfg.get('sync_favorites_default', True):
            _sync.sync_favorites_wrapper(spotify_session, tidal_session, cfg)
    else:
        migrate_saved_tracks(spotify_session, tidal_session)

if __name__ == '__main__':
    main()
