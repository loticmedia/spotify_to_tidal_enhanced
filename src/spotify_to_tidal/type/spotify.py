from spotipy import Spotify
from typing import TypedDict, List, Dict, Mapping, Literal, Optional


class SpotifyImage(TypedDict):
    url: str
    height: int
    width: int


class SpotifyFollower(TypedDict):
    href: str
    total: int


SpotifyID = str
SpotifySession = Spotify

def get_saved_tracks(sp: Spotify, limit: int = 50) -> list:
    """
    Retrieve all saved tracks from the user's Spotify library.
    """
    saved_tracks = []
    offset = 0
    while True:
        response = sp.current_user_saved_tracks(limit=limit, offset=offset)
        items = response.get("items", [])
        if not items:
            break
        saved_tracks.extend(items)
        offset += limit
        print(f"Retrieved {len(saved_tracks)} tracks so far...")
    print(f"✅ Total saved tracks fetched: {len(saved_tracks)}")
    return saved_tracks



class SpotifyArtist(TypedDict):
    external_urls: Mapping[str, str]
    followers: SpotifyFollower
    genres: List[str]
    href: str
    id: str
    images: List[SpotifyImage]
    name: str
    popularity: int
    type: str
    uri: str


class SpotifyAlbum(TypedDict):
    album_type: Literal["album", "single", "compilation"]
    total_tracks: int
    available_markets: List[str]
    external_urls: Dict[str, str]
    href: str
    id: str
    images: List[SpotifyImage]
    name: str
    release_date: str
    release_date_precision: Literal["year", "month", "day"]
    restrictions: Optional[Dict[Literal["reason"], str]]
    type: Literal["album"]
    uri: str
    artists: List[SpotifyArtist]


class SpotifyTrack(TypedDict):
    album: SpotifyAlbum
    artists: List[SpotifyArtist]
    available_markets: List[str]
    disc_number: int
    duration_ms: int
    explicit: bool
    external_ids: Dict[str, str]
    external_urls: Dict[str, str]
    href: str
    id: str
    is_playable: bool
    linked_from: Dict
    restrictions: Optional[Dict[Literal["reason"], str]]
    name: str
    popularity: int
    preview_url: str
    track_number: int
    type: Literal["track"]
    uri: str
