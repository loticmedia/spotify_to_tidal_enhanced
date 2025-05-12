import yaml
import argparse
import sys
import asyncio
from collections import defaultdict

from . import sync as _sync
from . import auth as _auth
from . import tidalapi_patch
from .type.spotify import get_saved_tracks


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

    print("Fetching all tracks currently in your TIDAL library...")
    all_saved_tidal_tracks = tidal_session.user.favorites.tracks()
    existing_titles = set(f"{t.name}|{t.artist.name}" for t in all_saved_tidal_tracks)

    for artist in sorted(artist_groups):
        tracks = artist_groups[artist]

        all_synced = all(f"{t['name']}|{artist}" in existing_titles for t in tracks)
        if all_synced:
            print(f"⏩ Skipping {artist} (✅ Already in TIDAL library)")
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
            print(f"✅ Added {len(matched_ids)} tracks to '{playlist_name}'\n")
        else:
            print(f"❌ Skipped {artist}")


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

    # Original playlist/favorites syncing
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
