import argparse
import sys
import asyncio
import yaml

from .auth import open_spotify_session, open_tidal_session
from .db.review_db import review_db
from .migrate import migrate_saved_tracks
from .convert_playlists import convert_tidal_playlists_to_albums_async
from .sync import (
    sync_playlists_wrapper,
    get_user_playlist_mappings,
    sync_favorites_wrapper
)


def main():
    parser = argparse.ArgumentParser(description="spotify_to_tidal_enhanced CLI")
    parser.add_argument('--config', default='config.yml', help='Path to config file')
    parser.add_argument('--sync-favorites', action=argparse.BooleanOptionalAction,
                        help='Enable/disable automatic favorites sync')
    parser.add_argument('--migrate-saved-tracks', action='store_true',
                        help='Review & migrate saved Spotify tracks')
    parser.add_argument('--convert-tidal-playlists-to-albums', action='store_true',
                        help='Favorite albums based on playlists')
    parser.add_argument('--reset-db', action='store_true', help='Reset local review DB')
    args = parser.parse_args()

    if args.reset_db:
        print("⚠️ Resetting the review database...")
        review_db.reset()
        print("✅ Database reset.")
        return

    with open(args.config, 'r') as f:
        cfg = yaml.safe_load(f)

    spotify_session = open_spotify_session(cfg['spotify'])
    tidal_session = open_tidal_session()
    if not tidal_session.check_login():
        sys.exit("❌ Could not authenticate with Tidal.")

    if args.convert_tidal_playlists_to_albums:
        asyncio.run(convert_tidal_playlists_to_albums_async(tidal_session))
    elif args.migrate_saved_tracks:
        asyncio.run(migrate_saved_tracks(spotify_session, tidal_session))
    else:
        sync_playlists_wrapper(
            spotify_session,
            tidal_session,
            get_user_playlist_mappings(spotify_session, tidal_session, cfg),
            cfg
        )
        if args.sync_favorites is None and cfg.get('sync_favorites_default', True):
            sync_favorites_wrapper(spotify_session, tidal_session, cfg)

if __name__ == '__main__':
    main()
