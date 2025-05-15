import yaml  # YAML parsing for configuration files
import argparse  # Command-line argument parsing
import sys  # System-specific functions and exit
import asyncio  # Asynchronous I/O support
import time  # Time utilities
from pathlib import Path  # Filesystem path handling
from collections import defaultdict  # Convenient dictionary for grouping
import datetime  # Date and time operations
import sqlalchemy  # SQL toolkit and ORM
from sqlalchemy import Table, Column, String, DateTime, MetaData, insert, select, update  # Core SQL constructs

from . import sync as _sync  # Internal module for sync operations
from . import auth as _auth  # Internal module for authentication
from .type.spotify import get_saved_tracks  # Function to fetch saved Spotify tracks
from .tidalapi_patch import get_all_playlists, get_all_playlist_tracks  # Async helpers for Tidal pagination

from rich.console import Console  # Rich console for styled output
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn, BarColumn, TaskProgressColumn  # Rich progress bars

# --- Unified Review Database ---
class ReviewDatabase:
    """
    Manages a SQLite table tracking each track's review status:
      - approved: user-approved or already synced
      - unapproved: user skipped, with retry schedule
    """
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
        """Drop and recreate the review_log table."""
        with self.engine.begin() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS review_log"))
        self.__init__()

    def _compute_next_retry(self, insert_time):
        """Exponential backoff: next retry interval doubles."""
        interval = 2 * (datetime.datetime.now() - insert_time)
        return datetime.datetime.now() + interval

    def set_approved(self, track_key):
        """Mark a track as approved or update its timestamp."""
        now = datetime.datetime.now()
        with self.engine.connect() as conn:
            with conn.begin():
                existing = conn.execute(
                    select(self.table).where(self.table.c.track_key == track_key)
                ).fetchone()
                if existing:
                    conn.execute(
                        update(self.table)
                        .where(self.table.c.track_key == track_key)
                        .values(status='approved', insert_time=now, next_retry=None)
                    )
                else:
                    conn.execute(
                        insert(self.table),
                        {'track_key': track_key, 'status': 'approved', 'insert_time': now, 'next_retry': None}
                    )

    def set_unapproved(self, track_key):
        """Mark a track as unapproved and schedule its next retry."""
        now = datetime.datetime.now()
        with self.engine.connect() as conn:
            with conn.begin():
                existing = conn.execute(
                    select(self.table).where(self.table.c.track_key == track_key)
                ).fetchone()
                if existing:
                    next_retry = self._compute_next_retry(existing.insert_time or now)
                    conn.execute(
                        update(self.table)
                        .where(self.table.c.track_key == track_key)
                        .values(status='unapproved', insert_time=now, next_retry=next_retry)
                    )
                else:
                    conn.execute(
                        insert(self.table),
                        {'track_key': track_key, 'status': 'unapproved', 'insert_time': now,
                         'next_retry': now + datetime.timedelta(days=7)}
                    )

    def get_status(self, track_key):
        """Retrieve a track's review status."""
        row = None
        with self.engine.connect() as conn:
            row = conn.execute(
                select(self.table.c.status).where(self.table.c.track_key == track_key)
            ).fetchone()
        return row.status if row else 'none'

    def should_retry(self, track_key):
        """Check if an unapproved track is due for retry."""
        row = None
        with self.engine.connect() as conn:
            row = conn.execute(
                select(self.table.c.next_retry).where(self.table.c.track_key == track_key)
            ).fetchone()
        return bool(row and row.next_retry <= datetime.datetime.now())

# Global review DB instance
review_db = ReviewDatabase()

# --- Helper Functions ---

def normalize(s):
    """Trim whitespace and lowercase for consistent comparisons."""
    return s.strip().lower()


def get_all_tidal_favorite_tracks(user, limit=100):
    """Paginate through all TIDAL favorites to return a list."""
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
    """Group Spotify tracks by artist for review logic."""
    artist_map = defaultdict(list)
    for item in tracks:
        track = item.get('track')
        if not track or not track.get('artists'):
            continue
        artist = track['artists'][0].get('name') or 'Unknown'
        artist_map[artist].append(track)
    return artist_map

async def add_track_async(session, tid):
    """Add a single track to TIDAL favorites asynchronously."""
    await asyncio.to_thread(session.user.favorites.add_track, tid)

async def auto_add_albums_with_multiple_tracks_async(tracks, tidal_session, artist_name):
    """Auto-favorite albums when >=3 tracks saved per album."""
    album_counts = defaultdict(list)
    for t in tracks:
        album = t.get('album') or {}
        key = normalize(album.get('name', ''))
        album_counts[key].append(t)

    def normalize_artist_name(name: str) -> str:
        return normalize(name.lower().replace(' and ', ' & ').replace('&', ' and '))

    async def add_album(album_name, track_list):
        if len(track_list) < 3:
            return
        print(f"📀 Adding album '{album_name}' to favorites ({len(track_list)} tracks)")
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
            print(f"❌ Error favoriting album '{album_name}': {e}")

    await asyncio.gather(*(add_album(name, lst) for name, lst in album_counts.items()))

async def migrate_saved_tracks(spotify_session, tidal_session):
    """Review and migrate saved Spotify tracks to TIDAL."""
    print("Fetching saved Spotify tracks...")
    saved_tracks = get_saved_tracks(spotify_session)
    artist_groups = group_tracks_by_artist(saved_tracks)

    console = Console()
    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(), transient=True, console=console
    ) as progress:
        task = progress.add_task("📡 Fetching TIDAL favorites...", start=True)
        existing = await asyncio.to_thread(get_all_tidal_favorite_tracks, tidal_session.user)
        progress.update(task, completed=len(existing))

    existing_titles = {f"{normalize(t.name)}|{normalize(t.artist.name)}" for t in existing}
    print(f"✅ Loaded {len(existing_titles)} favorites.")

    for artist, tracks in artist_groups.items():
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
        else:
            print(f"⏭️ Skipping {artist}")

# --- Feature: Convert TIDAL Playlists to Albums ---
async def convert_tidal_playlists_to_albums_async(tidal_session):
    """Favorite albums in playlists having >=3 tracks in that playlist."""
    playlists = await get_all_playlists(tidal_session.user)
    for pl in playlists:
        tracks = await get_all_playlist_tracks(pl)
        album_map = defaultdict(list)
        for tr in tracks:
            if tr.album:
                album_map[tr.album.id].append(tr)
        for album_id, group in album_map.items():
            if len(group) >= 3:
                print(f"📀 Favoriting album ID {album_id} ({len(group)} tracks)")
                await asyncio.to_thread(tidal_session.user.favorites.add_album, album_id)

# --- Main Entrypoint --- ---
def main():
    parser = argparse.ArgumentParser(description="spotify_to_tidal enhanced CLI")
    parser.add_argument('--config', default='config.yml')
    parser.add_argument('--sync-favorites', action=argparse.BooleanOptionalAction)
    parser.add_argument('--migrate-saved-tracks', action='store_true')
    parser.add_argument('--convert-tidal-playlists-to-albums', action='store_true',
                        help='Favorite albums with >=3 tracks in each playlist')
    parser.add_argument('--reset-db', action='store_true')
    args = parser.parse_args()

    if args.reset_db:
        print("⚠️ Resetting the review database...")
        review_db.reset()
        print("✅ Review database reset.")
        return

    # Load configuration
    with open(args.config, 'r') as f:
        cfg = yaml.safe_load(f)

    # Establish sessions
    spotify_session = _auth.open_spotify_session(cfg['spotify'])
    tidal_session = _auth.open_tidal_session()
    if not tidal_session.check_login():
        sys.exit("Could not connect to TIDAL")

    # Dispatch based on flags
    if args.convert_tidal_playlists_to_albums:
        asyncio.run(convert_tidal_playlists_to_albums_async(tidal_session))
    elif args.add_all_playlist_tracks_to_tidal:
        asyncio.run(add_all_playlist_tracks_to_tidal_async(tidal_session))
    elif args.review_playlists:
        asyncio.run(review_and_delete_playlists_async(tidal_session))
        asyncio.run(retry_albums_not_found_async(tidal_session))
    elif args.migrate_saved_tracks:
        asyncio.run(migrate_saved_tracks(spotify_session, tidal_session))
    elif args.count_saved_albums:
        asyncio.run(count_saved_albums_async(tidal_session))
    else:
        # Default behavior: sync playlists and favorites
        _sync.sync_playlists_wrapper(
            spotify_session, tidal_session,
            _sync.get_user_playlist_mappings(spotify_session, tidal_session, cfg), cfg
        )
        if args.sync_favorites is None and cfg.get('sync_favorites_default', True):
            _sync.sync_favorites_wrapper(spotify_session, tidal_session, cfg)

if __name__ == '__main__':
    main()
