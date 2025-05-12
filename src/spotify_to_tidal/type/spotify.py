from spotipy import Spotify
from typing import TypedDict, List, Dict, Mapping, Literal, Optional
import json
import time
from pathlib import Path
from requests.exceptions import ReadTimeout

CACHE_FILE = Path(".spotify_saved_tracks_cache.json")


class SpotifyImage(TypedDict):
    url: str
    height: int
    width: int


class SpotifyFollower(TypedDict):
    href: str
    total: int


SpotifyID = str
SpotifySession = Spotify


def get_saved_tracks(sp: Spotify, limit: int = 50, max_retries: int = 5) -> list:
    """
    Retrieve all saved tracks from the user's Spotify library.
    Uses a local cache file unless the user requests a refresh.
    """
    if CACHE_FILE.exists():
        print("💾 A cached list of saved Spotify tracks was found.")
        response = input("❓ Use cached version? [Y/n]: ").strip().lower()
        if response in ['', 'y', 'yes']:
            print("✅ Using cached saved tracks.\n")
            return json.loads(CACHE_FILE.read_text())

    print("🔄 Fetching saved tracks from Spotify...")
    saved_tracks = []
    offset = 0

    while True:
        for attempt in range(max_retries):
            try:
                response = sp._get("me/tracks", limit=limit, offset=offset, timeout=30)
                break
            except ReadTimeout:
                wait = 2 ** attempt
                print(f"⚠️  Timeout at offset {offset}, retrying in {wait}s...")
                time.sleep(wait)
        else:
            print(f"❌ Failed to retrieve tracks after {max_retries} retries at offset {offset}")
            break

        items = response.get("items", [])
        if not items:
            break
        saved_tracks.extend(items)
        offset += limit
        print(f"Retrieved {len(saved_tracks)} tracks so far...")

    print(f"✅ Total saved tracks fetched: {len(saved_tracks)}")
    CACHE_FILE.write_text(json.dumps(saved_tracks, indent=2))
    print(f"💾 Cache updated at {CACHE_FILE.resolve()}")
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
