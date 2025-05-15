import asyncio

from .tidal.helpers import get_all_tidal_favorite_tracks  # reuse if needed
from .tidal.helpers import auto_add_albums_with_multiple_tracks_async
from .tidalapi_patch import (
    get_all_playlists, get_all_playlist_tracks
)  # Pagination helpers for Tidal API

async def convert_tidal_playlists_to_albums_async(tidal_session):
    """Favorite albums in playlists if they contain ≥3 tracks."""
    playlists = await get_all_playlists(tidal_session.user)
    for pl in playlists:
        tracks = await get_all_playlist_tracks(pl)
        album_map = {}
        for tr in tracks:
            album_id = tr.album.id if tr.album else None
            if album_id:
                album_map.setdefault(album_id, []).append(tr)
        for album_id, group in album_map.items():
            if len(group) >= 3:
                print(f"📀 Favoriting album ID {album_id} ({len(group)} tracks)")
                await asyncio.to_thread(tidal_session.user.favorites.add_album, album_id)
