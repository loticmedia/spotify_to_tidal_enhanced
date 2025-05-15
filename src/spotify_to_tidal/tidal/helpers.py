import asyncio
from collections import defaultdict
from ..spotify.helpers import normalize

def get_all_tidal_favorite_tracks(user, limit=100):
    """Fetch all Tidal favorite tracks with pagination."""
    all_tracks = []
    offset = 0
    while True:
        page = user.favorites.tracks(limit=limit, offset=offset)
        if not page:
            break
        all_tracks.extend(page)
        offset += len(page)
    return all_tracks


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

