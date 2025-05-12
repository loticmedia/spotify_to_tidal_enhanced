import yaml
import argparse
import sys
import asyncio
import json
import time
import threading
import itertools
from pathlib import Path
from collections import defaultdict
import datetime
import sqlalchemy
from sqlalchemy import Table, Column, String, DateTime, MetaData, insert, select, update, delete

from . import sync as _sync
from . import auth as _auth
from . import tidalapi_patch
from .type.spotify import get_saved_tracks

# Database for storing match failures (skips)
class MatchFailureDatabase:
    """
    sqlite database of match failures which persists between runs
    this can be used concurrently between multiple processes
    """
    def __init__(self, filename='.cache.db'):
        self.engine = sqlalchemy.create_engine(f"sqlite:///{filename}")
        meta = MetaData()
        self.match_failures = Table('match_failures', meta,
                                    Column('track_id', String, primary_key=True),
                                    Column('insert_time', DateTime),
                                    Column('next_retry', DateTime),
                                    sqlite_autoincrement=False)
        meta.create_all(self.engine)

    def _get_next_retry_time(self, insert_time: datetime.datetime | None = None) -> datetime.datetime:
        if insert_time:
            interval = 2 * (datetime.datetime.now() - insert_time)
        else:
            interval = datetime.timedelta(days=7)
        return datetime.datetime.now() + interval

    def cache_match_failure(self, track_id: str):
        stmt = select(self.match_failures).where(self.match_failures.c.track_id == track_id)
        with self.engine.connect() as conn:
            with conn.begin():
                existing = conn.execute(stmt).fetchone()
                if existing:
                    upd = update(self.match_failures).where(
                        self.match_failures.c.track_id == track_id
                    ).values(next_retry=self._get_next_retry_time(existing.insert_time))
                    conn.execute(upd)
                else:
                    ins = insert(self.match_failures)
                    conn.execute(ins, {
                        'track_id': track_id,
                        'insert_time': datetime.datetime.now(),
                        'next_retry': self._get_next_retry_time()
                    })

    def has_match_failure(self, track_id: str) -> bool:
        stmt = select(self.match_failures.c.next_retry).where(
            self.match_failures.c.track_id == track_id
        )
        with self.engine.connect() as conn:
            row = conn.execute(stmt).fetchone()
            if row:
                return row.next_retry > datetime.datetime.now()
            return False

    def remove_match_failure(self, track_id: str):
        stmt = delete(self.match_failures).where(
            self.match_failures.c.track_id == track_id
        )
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(stmt)

# singleton instance for skip tracking
failure_cache = MatchFailureDatabase()
failure_cache = MatchFailureDatabase()

class Spinner:
    def __init__(self, message="Loading..."):
        self.message = message
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._spin)

    def _spin(self):
        spinner = itertools.cycle(["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"])
        sys.stdout.write(f"{self.message} ")
        while not self._stop_event.is_set():
            sys.stdout.write(next(spinner))
            sys.stdout.flush()
            time.sleep(0.1)
            sys.stdout.write("\b")
        sys.stdout.write("✔\n")

    def start(self): self._thread.start()
    def stop(self):
        self._stop_event.set()
        self._thread.join()


def normalize(s: str) -> str:
    return s.strip().lower()


def get_all_tidal_favorite_tracks(user, limit=1000):
    print("📡 Fetching all saved TIDAL tracks with pagination...")
    offset = 0
    all_tracks = []
    while True:
        page = user.favorites.tracks(limit=limit, offset=offset)
        if not page:
            break
        all_tracks.extend(page)
        offset += limit
        print(f"Retrieved {len(all_tracks)} tracks so far...")
    return all_tracks


def group_tracks_by_artist(tracks):
    artist_map = defaultdict(list)
    for item in tracks:
        track = item['track']
        artist = track['artists'][0]['name']
        artist_map[artist].append(track)
    return artist_map


def auto_add_albums_with_multiple_tracks(tracks, tidal_session, artist_name):
    album_counts = defaultdict(list)
    for t in tracks:
        album = t['album']
        key = normalize(album['name'])
        album_counts[key].append(t)
    for album_name, track_list in album_counts.items():
        if len(track_list) >= 3:
            print(f"💿 Found {len(track_list)} tracks from album \"{album_name}\" — adding to TIDAL favorites...")
            try:
                res = tidal_session.search(album_name)
                albums = res.get('albums', [])
                matches = [a for a in albums if normalize(a.artist.name)==normalize(artist_name)]
                if matches:
                    tidal_session.user.favorites.add_album(matches[0].id)
                    print(f"✅ Album \"{album_name}\" added to favorites.\n")
                else:
                    print(f"⚠️ Could not find album \"{album_name}\" by {artist_name}.\n")
            except Exception as e:
                print(f"❌ Failed to add album \"{album_name}\": {e}")


def migrate_saved_tracks(spotify_session, tidal_session):
    print("Fetching saved Spotify tracks...")
    saved = get_saved_tracks(spotify_session)
    artist_groups = group_tracks_by_artist(saved)

    spinner = Spinner("📡 Fetching saved TIDAL tracks...")
    spinner.start()
    start = time.time()
    existing_titles = {f"{normalize(t.name)}|{normalize(t.artist.name)}" for t in get_all_tidal_favorite_tracks(tidal_session.user)}
    spinner.stop()
    elapsed = time.time()-start
    print(f"✅ Loaded {len(existing_titles)} saved TIDAL tracks in {elapsed:.1f} seconds.\n")

    for artist in sorted(artist_groups):
        tracks = artist_groups[artist]
        all_keys = [f"{normalize(t['name'])}|{normalize(artist)}" for t in tracks]

        in_tidal = all(k in existing_titles for k in all_keys)
        skipped = all(failure_cache.has_match_failure(k) for k in all_keys)

        if in_tidal:
            print(f"⏩ Skipping {artist} (✅ Already in TIDAL)")
            continue
        if skipped:
            print(f"⏩ Skipping {artist} (❌ Previously skipped)")
            continue

        print(f"\n🎤 Artist: {artist} — {len(tracks)} tracks")
        for t in tracks[:5]:
            print(f"  • \"{t['name']}\" — {t['album']['name']}")

        resp = input(f"\n❓ Approve and add tracks by '{artist}'? [y/N]: ").strip().lower()
        if resp=='y':
            print(f"✔ Syncing {artist}...")
            _sync.populate_track_match_cache(tracks, [])
            # run search as async to ensure matching
            asyncio.run(_sync.search_new_tracks_on_tidal(tidal_session, tracks, artist, {}))
            matched = _sync.get_tracks_for_new_tidal_playlist(tracks)
            for tid in matched:
                tidal_session.user.favorites.add_track(tid)
            auto_add_albums_with_multiple_tracks(tracks, tidal_session, artist)
            print(f"✅ Added {len(matched)} tracks to your TIDAL favorites.")
            for k in all_keys:
                failure_cache.remove_match_failure(k)
        else:
            print(f"❌ Skipped {artist}")
            for k in all_keys:
                failure_cache.cache_match_failure(k)


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--config',default='config.yml')
    p.add_argument('--uri')
    p.add_argument('--sync-favorites',action=argparse.BooleanOptionalAction)
    p.add_argument('--migrate-saved-tracks',action='store_true')
    args=p.parse_args()

    cfg=yaml.safe_load(open(args.config))
    print("Opening Spotify session")
    sp=_auth.open_spotify_session(cfg['spotify'])
    print("Opening TIDAL session")
    ts=_auth.open_tidal_session()
    if not ts.check_login(): sys.exit("Could not connect to TIDAL")

    if args.migrate_saved_tracks:
        migrate_saved_tracks(sp, ts)
        return

    if args.uri:
        pl=sp.playlist(args.uri)
        tpls=_sync.get_tidal_playlists_wrapper(ts)
        tp=_sync.pick_tidal_playlist_for_spotify_playlist(pl, tpls)
        _sync.sync_playlists_wrapper(sp, ts, [tp], cfg)
        sf=args.sync_favorites
    elif args.sync_favorites:
        sf=True
    elif cfg.get('sync_playlists'):
        _sync.sync_playlists_wrapper(sp, ts, _sync.get_playlists_from_config(sp, ts, cfg), cfg)
        sf = args.sync_favorites is None and cfg.get('sync_favorites_default',True)
    else:
        _sync.sync_playlists_wrapper(sp, ts, _sync.get_user_playlist_mappings(sp, ts, cfg), cfg)
        sf = args.sync_favorites is None and cfg.get('sync_favorites_default',True)
    if sf:
        _sync.sync_favorites_wrapper(sp, ts, cfg)

if __name__=='__main__':
    main()
    sys.exit(0)
