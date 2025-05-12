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

from . import sync as _sync
from . import auth as _auth
from . import tidalapi_patch
from .type.spotify import get_saved_tracks

REVIEW_LOG_FILE = Path(".track_review_log.json")


class Spinner:
    def __init__(self, message="Loading..."):
        self.message = message
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._spin)

    def _spin(self):
        spinner = itertools.cycle(["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])
        sys.stdout.write(f"{self.message} ")
        while not self._stop_event.is_set():
            sys.stdout.write(next(spinner))
            sys.stdout.flush()
            time.sleep(0.1)
            sys.stdout.write("\b")
        sys.stdout.write("✔\n")

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join()


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


def load_review_log():
    if REVIEW_LOG_FILE.exists():
        with open(REVIEW_LOG_FILE, "r") as f:
            return json.load(f)
    return {}


def save_review_log(log):
    with open(REVIEW_LOG_FILE, "w") as f:
        json.dump(log, f, indent=2)


def group_tracks_by_artist(tracks):
    artist_map = defaultdict(list)
    for item in tracks:
        track = item['track']
        artist = track['artists'][0]['name']
        artist_map[artist].append(track)
    return artist_map


def get_or_create_playlist(tidal_session, name, description):
    playlists = tidal_session.user.playlists()
    for p in playlists:
        if p.name == name:
            return p
    return tidal_session.user.create_playlist(name, description)


def migrate_saved_tracks(spotify_session, tidal_session):
    print("Fetching saved Spotify tracks...")
    saved_tracks = get_saved_tracks(spotify_session)
    artist_groups = group_tracks_by_artist(saved_tracks)

    playlist_name = "Approved Saved Tracks"
    playlist = get_or_create_playlist(tidal_session, playlist_name, "Imported from Spotify with artist approval")

    spinner = Spinner("📡 Fetching saved TIDAL tracks...")
    spinner.start()
    start = time.time()

    all_saved_tidal_tracks = get_all_tidal_favorite_tracks(tidal_session.user)

    elapsed = time.time() - start
    spinner.stop()
    print(f"✅ Loaded {len(all_saved_tidal_tracks)} saved TIDAL tracks in {elapsed:.1f} seconds.\n")

    existing_titles = set(f"{t.name}|{t.artist.name}" for t in all_saved_tidal_tracks)
    review_log = load_review_log()

    for artist in sorted(artist_groups):
        tracks = artist_groups[artist]

        all_synced = all(f"{t['name']}|{artist}" in existing_titles for t in tracks)
        all_skipped = all(f"{t['name']}|{artist}" in review_log and review_log[f"{t['name']}|{artist}"] == "skipped" for t in tracks)

        if all_synced:
            print(f"⏩ Skipping {artist} (✅ Already in TIDAL library)")
            continue
        if all_skipped:
            print(f"⏩ Skipping {artist} (❌ Previously reviewed and skipped)")
            continue

        print(f"\n🎤 Artist: {artist}")
        print(f"Found {len(tracks)} saved tracks by this artist.\n")

        print("🎵 Track Previews:")
        for t in tracks[:5]:
            name = t['name']
            album = t['album']['name']
            print(f"  • \"{name}\" — {album}")

        response = input(f"\n❓ Approve and sync tracks by '{artist}'? [y/N]: ").strip().lower()
        if response == 'y':
            print(f"✔ Syncing {artist}...")

            _sync.populate_track_match_cache(tracks, [])
            asyncio.run(_sync.search_new_tracks_on_tidal(tidal_session, tracks, artist, {}))
            matched_ids = _sync.get_tracks_for_new_tidal_playlist(tracks)

            tidalapi_patch.add_multiple_tracks_to_playlist(playlist, matched_ids)
            print(f"✅ Added {len(matched_ids)} tracks to '{playlist_name}'")

            for t in tracks:
                review_log[f"{t['name']}|{artist}"] = "approved"
        else:
            print(f"❌ Skipped {artist}")
            for t in tracks:
                review_log[f"{t['name']}|{artist}"] = "skipped"

        save_review_log(review_log)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config.yml', help='location of the config file')
    parser.add_argument('--uri', help='synchronize a specific URI instead of the one in the config')
    parser.add_argument('--sync-favorites', action=argparse.BooleanOptionalAction, help='synchronize the favorites')
    parser.add_argument('--migrate-saved-tracks', action='store_true', help='Review and migrate saved Spotify tracks by artist')
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    print("Opening Spotify session")
    spotify_session = _auth.open_spotify_session(config['spotify'])

    print("Opening Tidal session")
    tidal_session = _auth.open_tidal_session()
    if not tidal_session.check_login():
        sys.exit("Could not connect to TIDAL")

    if args.migrate_saved_tracks:
        migrate_saved_tracks(spotify_session, tidal_session)
        return

    if args.uri:
        spotify_playlist = spotify_session.playlist(args.uri)
        tidal_playlists = _sync.get_tidal_playlists_wrapper(tidal_session)
        tidal_playlist = _sync.pick_tidal_playlist_for_spotify_playlist(spotify_playlist, tidal_playlists)
        _sync.sync_playlists_wrapper(spotify_session, tidal_session, [tidal_playlist], config)
        sync_favorites = args.sync_favorites
    elif args.sync_favorites:
        sync_favorites = True
    elif config.get('sync_playlists', None):
        _sync.sync_playlists_wrapper(
            spotify_session, tidal_session,
            _sync.get_playlists_from_config(spotify_session, tidal_session, config), config
        )
        sync_favorites = args.sync_favorites is None and config.get('sync_favorites_default', True)
    else:
        _sync.sync_playlists_wrapper(
            spotify_session, tidal_session,
            _sync.get_user_playlist_mappings(spotify_session, tidal_session, config), config
        )
        sync_favorites = args.sync_favorites is None and config.get('sync_favorites_default', True)

    if sync_favorites:
        _sync.sync_favorites_wrapper(spotify_session, tidal_session, config)


if __name__ == '__main__':
    main()
    sys.exit(0)
