import asyncio
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from .spotify.helpers import get_saved_tracks, group_tracks_by_artist, normalize
from .tidal.helpers import get_all_tidal_favorite_tracks, auto_add_albums_with_multiple_tracks_async
from .sync import (
    populate_track_match_cache,
    search_new_tracks_on_tidal,
    get_tracks_for_new_tidal_playlist
)


def preview_tracks(tracks, limit=5):
    """Display a small sample of tracks for user review."""
    for t in tracks[:limit]:
        print(f"  • {t['name']} — {t['album']['name']}")


async def migrate_saved_tracks(spotify_session, tidal_session):
    """Interactively review and migrate saved Spotify tracks to your Tidal account."""
    print("⏳ Fetching saved Spotify tracks...")
    saved_tracks = get_saved_tracks(spotify_session)
    artist_groups = group_tracks_by_artist(saved_tracks)

    # Show spinner while loading existing favorites
    console = Console()
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        transient=True,
        console=console
    ) as progress:
        task = progress.add_task("📡 Fetching Tidal favorites...", start=True)
        existing = await asyncio.to_thread(
            get_all_tidal_favorite_tracks, tidal_session.user
        )
        progress.update(task, completed=len(existing))

    existing_titles = {f"{normalize(t.name)}|{normalize(t.artist.name)}" for t in existing}
    print(f"✅ Loaded {len(existing_titles)} existing favorites.")

    # Loop for each artist group in alphabetical order
    for artist in sorted(artist_groups):
        tracks = artist_groups[artist]
        print(f"\n🎤 Artist: {artist} ({len(tracks)} saved tracks)")
        preview_tracks(tracks)
        resp = input(
            f"Approve and add 🎹 {artist.upper()}? [y/N]: "
        ).strip().lower()

        if resp == 'y':
            print(f"🔄 Syncing tracks for {artist}...")
            # Prepare cache and perform search+sync
            populate_track_match_cache(tracks, [])
            # Correct call: pass tidal_session as first argument
            await search_new_tracks_on_tidal(
                tidal_session,  # use Tidal session
                tracks,         # Spotify tracks
                artist,         # artist filter
                {}              # optional config
            )
            # Collect matched track IDs and add them
            matched = get_tracks_for_new_tidal_playlist(tracks)
            await asyncio.gather(
                *(
                    asyncio.to_thread(
                        tidal_session.user.favorites.add_track, tid
                    ) for tid in matched
                )
            )
            # Auto-favorite albums with ≥3 tracks
            await auto_add_albums_with_multiple_tracks_async(
                tracks, tidal_session, artist
            )
        else:
            print(f"⏭️ Skipped artist {artist}.")
