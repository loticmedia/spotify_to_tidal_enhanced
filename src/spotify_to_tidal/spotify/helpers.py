from collections import defaultdict

from ..type.spotify import get_saved_tracks  # fetch saved Spotify tracks


def normalize(s: str) -> str:
    """Trim whitespace and lowercase for comparisons."""
    return s.strip().lower()


def group_tracks_by_artist(tracks):
    """Group Spotify track dicts by primary artist."""
    artist_map = defaultdict(list)
    for item in tracks:
        track = item.get('track')
        # skip if missing artist metadata
        if not track or not track.get('artists'):
            continue
        artist = track['artists'][0].get('name', 'Unknown')
        artist_map[artist].append(track)
    return artist_map
