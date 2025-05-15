import yaml  # YAML parsing for configuration files
import argparse  # Command-line argument parsing
import sys  # System-specific functions and exit handling
import asyncio  # Asynchronous I/O support for concurrent tasks
import time  # Basic time utilities
from pathlib import Path  # Filesystem path manipulation
from collections import defaultdict  # Dictionary subclass for automatic list creation
import datetime  # Advanced date and time operations
import sqlalchemy  # SQL toolkit and ORM for database interactions
from sqlalchemy import (
    Table, Column, String, DateTime, MetaData, insert, select, update
)  # Core SQL constructs for schema and queries

from . import sync as _sync  # Internal synchronization utilities
from . import auth as _auth  # Internal authentication utilities
from .type.spotify import get_saved_tracks  # Fetch saved Spotify tracks
from .tidalapi_patch import (
    get_all_playlists, get_all_playlist_tracks
)  # Pagination helpers for Tidal API

from rich.console import Console  # Console for styled terminal output
from rich.progress import (
    Progress, SpinnerColumn, TextColumn, TimeElapsedColumn,
    BarColumn, TaskProgressColumn
)  # Rich progress bar components

# --- Unified Review Database ---
class ReviewDatabase:
    """
    Manages a SQLite table tracking each track's review status:
      - approved: user-approved or already synced
      - unapproved: user skipped, with retry scheduling
    """
    def __init__(self, filename='.cache.db'):
        # Create engine and metadata for SQLite
        self.engine = sqlalchemy.create_engine(f"sqlite:///{filename}")
        self.meta = MetaData()
        # Define review_log table schema
        self.table = Table(
            'review_log', self.meta,
            Column('track_key', String, primary_key=True),
            Column('status', String),
            Column('insert_time', DateTime),
            Column('next_retry', DateTime)
        )
        # Create table if it does not exist
        self.meta.create_all(self.engine)

    def reset(self):
        """Drop and recreate the review_log table for a fresh start."""
        with self.engine.begin() as conn:
            conn.execute(sqlalchemy.text("DROP TABLE IF EXISTS review_log"))
        # Reinitialize schema
        self.__init__()

    def _compute_next_retry(self, insert_time):
        """Calculate exponential backoff interval for retries."""
        # Double the time since initial insertion
        elapsed = datetime.datetime.now() - insert_time
        return datetime.datetime.now() + (elapsed * 2)

    def set_approved(self, track_key):
        """Mark a track as approved and reset its retry schedule."""
        now = datetime.datetime.now()
        with self.engine.connect() as conn:
            with conn.begin():
                # Check if record exists
                existing = conn.execute(
                    select(self.table)
                    .where(self.table.c.track_key == track_key)
                ).fetchone()
                if existing:
                    # Update existing record
                    conn.execute(
                        update(self.table)
                        .where(self.table.c.track_key == track_key)
                        .values(status='approved', insert_time=now, next_retry=None)
                    )
                else:
                    # Insert new approved record
                    conn.execute(
                        insert(self.table),
                        {'track_key': track_key, 'status': 'approved', 'insert_time': now, 'next_retry': None}
                    )

    def set_unapproved(self, track_key):
        """Mark a track as unapproved and compute its next retry time."""
        now = datetime.datetime.now()
        with self.engine.connect() as conn:
            with conn.begin():
                existing = conn.execute(
                    select(self.table)
                    .where(self.table.c.track_key == track_key)
                ).fetchone()
                if existing:
                    # Calculate exponential backoff for retry
                    next_retry = self._compute_next_retry(existing.insert_time or now)
                    conn.execute(
                        update(self.table)
                        .where(self.table.c.track_key == track_key)
                        .values(status='unapproved', insert_time=now, next_retry=next_retry)
                    )
                else:
                    # First retry scheduled 7 days later
                    conn.execute(
                        insert(self.table),
                        {
                            'track_key': track_key,
                            'status': 'unapproved',
                            'insert_time': now,
                            'next_retry': now + datetime.timedelta(days=7)
                        }
                    )

    def get_status(self, track_key):
        """Retrieve the stored review status of a track."""
        with self.engine.connect() as conn:
            row = conn.execute(
                select(self.table.c.status)
                .where(self.table.c.track_key == track_key)
            ).fetchone()
        return row.status if row else 'none'

    def should_retry(self, track_key):
        """Determine if an unapproved track is due for another retry attempt."""
        with self.engine.connect() as conn:
            row = conn.execute(
                select(self.table.c.next_retry)
                .where(self.table.c.track_key == track_key)
            ).fetchone()
        # Only retry if next_retry timestamp has passed
        return bool(row and row.next_retry <= datetime.datetime.now())

# Single shared ReviewDatabase instance
review_db = ReviewDatabase()

# --- Helper Functions ---

def normalize(s: str) -> str:
    """Standardize strings by trimming and lowercasing."""
    return s.strip().lower()


def get_all_tidal_favorite_tracks(user, limit=100):
    """Fetch all favorite tracks from Tidal with pagination."""
    all_tracks = []
    offset = 0
    while True:
        page = user.favorites.tracks(limit=limit, offset=offset)
        if not page:
            break  # No more tracks to fetch
        all_tracks.extend(page)
        offset += len(page)
    return all_tracks


def group_tracks_by_artist(tracks):
    """Group Spotify tracks by their primary artist for batch review."""
    artist_map = defaultdict(list)
    for item in tracks:
        track = item.get('track')
        if not track or not track.get('artists'):
            continue  # Skip malformed entries
        # Use first listed artist
        artist = track['artists'][0].get('name', 'Unknown')
        artist_map[artist].append(track)
    return artist_map

async def add_track_async(session, track_id):
    """Asynchronously add a single track to Tidal favorites."""
    await asyncio.to_thread(session.user.favorites.add_track, track_id)

async def auto_add_albums_with_multiple_tracks_async(tracks, tidal_session, artist_name):
    """Automatically favorite albums when >=3 saved tracks exist per album."""
    album_counts = defaultdict(list)
    for t in tracks:
        album = t.get('album') or {}
        key = normalize(album.get('name', ''))
        album_counts[key].append(t)

    def normalize_artist_name(name: str) -> str:
        # Normalize 'and' vs '&' variants
        return normalize(name.replace(' and ', ' & ').replace('&', ' and '))

    async def add_album(album_name: str, track_list: list):
        # Only proceed if at least 3 tracks of the album exist
        if len(track_list) < 3:
            return
        print(f"📀 Adding album '{album_name}' to favorites ({len(track_list)} tracks)")
        try:
            # Search Tidal for album name
            results = tidal_session.search(album_name) or {}
            albums = results.get('albums', [])
            # Filter matches by artist normalization
            matches = [a for a in albums
                       if normalize_artist_name(a.artist.name) == normalize_artist_name(artist_name)]
            if matches:
                # Favorite the first matching album
                await asyncio.to_thread(
                    tidal_session.user.favorites.add_album,
                    matches[0].id
                )
            else:
                print(f"⚠️ No match for album '{album_name}' by {artist_name}")
        except Exception as e:
            print(f"❌ Error favoriting album '{album_name}': {e}")

    # Run add_album coroutines concurrently
    await asyncio.gather(*(add_album(name, lst) for name, lst in album_counts.items()))

async def migrate_saved_tracks(spotify_session, tidal_session):
    """Interactively review and migrate saved Spotify tracks to Tidal."""
    print("⏳ Fetching saved Spotify tracks...")
    saved_tracks = get_saved_tracks(spotify_session)
    artist_groups = group_tracks_by_artist(saved_tracks)

    console = Console()
    # Display a spinner while loading existing Tidal favorites
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        transient=True,
        console=console
    ) as progress:
        task = progress.add_task("📡 Fetching Tidal favorites...", start=True)
        existing = await asyncio.to_thread(get_all_tidal_favorite_tracks,
                                           tidal_session.user)
        # Update spinner with count
        progress.update(task, completed=len(existing))

    existing_titles = {f"{normalize(t.name)}|{normalize(t.artist.name)}" for t in existing}
    print(f"✅ Loaded {len(existing_titles)} existing favorites.")

    # Loop through each artist group for user approval
    for artist, tracks in artist_groups.items():
        print(f"\n🎤 Artist: {artist} ({len(tracks)} saved tracks)")
        # Preview first five tracks
        for t in tracks[:5]:
            print(f"  • {t['name']} — {t['album']['name']}")
        # Prompt user for approval
        resp = input(f"Approve and add 🎹 {artist.upper()}? [y/N]: ").strip().lower()
        if resp == 'y':
            print(f"🔄 Syncing tracks for {artist}...")
            # Clear and repopulate internal cache for matching
            _sync.populate_track_match_cache(tracks, [])
            await _sync.search_new_tracks_on_tidal(spotify_session=spotify_session,
                                                  tidal_session=tidal_session,
                                                  tracks=tracks,
                                                  artist=artist,
                                                  cfg={})
            matched = _sync.get_tracks_for_new_tidal_playlist(tracks)
            # Add each matched track concurrently
            await asyncio.gather(
                *(add_track_async(tidal_session, tid) for tid in matched)
            )
            # Auto-favorite related albums
            await auto_add_albums_with_multiple_tracks_async(tracks, tidal_session, artist)
        else:
            print(f"⏭️ Skipped artist {artist}.")

async def convert_tidal_playlists_to_albums_async(tidal_session):
    """Favorite albums in playlists if they contain >=3 tracks."""
    playlists = await get_all_playlists(tidal_session.user)
    for pl in playlists:
        tracks = await get_all_playlist_tracks(pl)
        album_map = defaultdict(list)
        for tr in tracks:
            if tr.album:
                album_map[tr.album.id].append(tr)
        # Favorite each album meeting the threshold
        for album_id, group in album_map.items():
            if len(group) >= 3:
                print(f"📀 Favoriting album ID {album_id} ({len(group)} tracks)")
                await asyncio.to_thread(
                    tidal_session.user.favorites.add_album,
                    album_id
                )

# --- Main Entrypoint ---
def main():
    # CLI argument definitions
    parser = argparse.ArgumentParser(
        description="spotify_to_tidal enhanced CLI"
    )
    parser.add_argument('--config', default='config.yml',
                        help='Path to configuration YAML file')
    parser.add_argument('--sync-favorites', action=argparse.BooleanOptionalAction,
                        help='Enable/disable automatic favorites sync')
    parser.add_argument('--migrate-saved-tracks', action='store_true',
                        help='Interactively migrate saved Spotify tracks to Tidal')
    parser.add_argument('--convert-tidal-playlists-to-albums',
                        action='store_true',
                        help='Favorite albums in playlists with ≥3 tracks')
    parser.add_argument('--reset-db', action='store_true',
                        help='Reset the local review database')
    args = parser.parse_args()

    # Handle database reset flag
    if args.reset_db:
        print("⚠️ Resetting the review database...")
        review_db.reset()
        print("✅ Review database reset.")
        return

    # Load YAML config into dictionary
    with open(args.config, 'r') as f:
        cfg = yaml.safe_load(f)

    # Initialize API sessions
    spotify_session = _auth.open_spotify_session(cfg['spotify'])
    tidal_session = _auth.open_tidal_session()
    if not tidal_session.check_login():
        sys.exit("❌ Could not authenticate with Tidal. Exiting.")

    # Dispatch behavior based on CLI flags
    if args.convert_tidal_playlists_to_albums:
        asyncio.run(convert_tidal_playlists_to_albums_async(tidal_session))
    elif args.migrate_saved_tracks:
        asyncio.run(migrate_saved_tracks(spotify_session, tidal_session))
    else:
        # Default: sync playlists and optionally favorites
        _sync.sync_playlists_wrapper(
            spotify_session,
            tidal_session,
            _sync.get_user_playlist_mappings(
                spotify_session, tidal_session, cfg
            ),
            cfg
        )
        # Sync favorites if enabled by flag or config default
        if args.sync_favorites is None and cfg.get('sync_favorites_default', True):
            _sync.sync_favorites_wrapper(
                spotify_session, tidal_session, cfg
            )

if __name__ == '__main__':
    main()
