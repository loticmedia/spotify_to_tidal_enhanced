# spotify\_to\_tidal\_enhanced CLI

A command-line application to synchronize your Spotify content with Tidal. Supports:

* **Playlist synchronization** between Spotify and Tidal
* **Favorites migration** ("Liked Songs")
* **Interactive review** of saved Spotify tracks before import
* **Automatic album favoriting** when 3 or more tracks from the same album are detected
* **Playlist-to-album conversion**: favorite albums based on your Tidal playlists
* **Local review database** with retry/backoff logic and reset capability

---

## Installation

Clone the repository, create and activate a local virtual environment, then install:

```bash
# 1. Clone the repository
git clone https://github.com/loticmedia/spotify_to_tidal_enhanced.git
cd spotify_to_tidal_enhanced

# 2. Create a Python virtual environment in `.venv`
python3 -m venv .venv

# 3. Activate the virtual environment
source .venv/bin/activate  # macOS/Linux

# 4. Install in editable mode
python3 -m pip install -e .
```

This ensures dependencies are isolated and registers the `spotify_to_tidal_enhanced` command in your environment.

## Configuration

1. Copy `example_config.yml` to `config.yml`:

   ```bash
   cp example_config.yml config.yml
   ```
2. Register a new Spotify app at [Spotify Developer Dashboard](https://developer.spotify.com/dashboard/applications).
3. In `config.yml`:

   * Paste your **Client ID** and **Client Secret** under the `spotify` section.
   * Ensure the **redirect\_uri** in the file matches a redirect URI registered in your Spotify app settings.
4. (Optional) Adjust other settings, for example:

   ```yaml
   sync_favorites_default: true    # Sync "Liked Songs" by default
   ```

## Usage

Run the primary command from your shell:

```bash
spotify_to_tidal_enhanced [OPTIONS]
```

### Common Options

| Flag                                     | Description                                                |
| ---------------------------------------- | ---------------------------------------------------------- |
| `--config PATH`                          | Path to your YAML config file (default: `config.yml`)      |
| `--sync-favorites / --no-sync-favorites` | Enable or disable automatic favorites ("Liked Songs") sync |
| `--migrate-saved-tracks`                 | Run interactive migration of saved Spotify tracks          |
| `--convert-tidal-playlists-to-albums`    | Favorite albums with ≥3 tracks in each Tidal playlist      |
| `--reset-db`                             | Reset the local review database to start fresh             |
| `-h, --help`                             | Show help and exit                                         |

### Examples

1. **Sync all playlists & liked songs (default):**

   ```bash
   spotify_to_tidal_enhanced
   ```

2. **Sync only playlists, skip liked songs:**

   ```bash
   spotify_to_tidal_enhanced --no-sync-favorites
   ```

3. **Interactively review & migrate saved tracks:**

   ```bash
   spotify_to_tidal_enhanced --migrate-saved-tracks
   ```

4. **Automatically favorite albums from your Tidal playlists:**

   ```bash
   spotify_to_tidal_enhanced --convert-tidal-playlists-to-albums
   ```

5. **Reset the internal review database:**

   ```bash
   spotify_to_tidal_enhanced --reset-db
   ```

