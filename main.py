"""
YouTube Music API powered by yt-dlp + FastAPI (Render Deployment Ready).

Endpoints:
  GET /search?q=...&type=songs|albums|artists|playlists&limit=20
  GET /album/{browse_id}              Album + tracks
  GET /artist/{channel_id}            Artist: top songs, albums, singles
  GET /home                           Tamil / India home feed (songs, playlists, albums, artists)
  GET /track/{video_id}               Track metadata
  GET /stream/{video_id}              Direct audio URL (expires after a few hours)
  GET /download/{video_id}?fmt=mp3    Download audio file (needs ffmpeg for conversion)
  GET /playlist/{playlist_id}         Playlist / album track list
  GET /mood?name=Relax                Mood feeds
  GET /health                         Health check & ffmpeg status

Deployment on Render (Docker runtime, so ffmpeg + node are available):
  Dockerfile and requirements.txt sit next to this file. Render sets $PORT.
  Health check path: /health

Why this differs from the localhost version:
  * YouTube audio URLs are locked to the IP that requested them. On localhost the
    server and browser share an IP, on Render they don't, so /stream hands back a
    URL on THIS server (/audio/{id}) which proxies the bytes (Range supported).
  * Datacenter IPs get bot-checked more, so set cookies / PO token / proxy via env.
  * If ffmpeg is missing, /download falls back to the native format instead of 500.
"""

import io
import os
import re
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache, wraps
from typing import Optional
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import ProxyHandler, Request as UrlRequest, build_opener, urlopen

import yt_dlp

try:
    from PIL import Image, ImageChops
except ImportError:  # covers still work, just without auto-cropping
    Image = ImageChops = None

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse, Response, StreamingResponse
from starlette.background import BackgroundTask

API_KEY = os.getenv("API_KEY")  # optional: set X-API-Key header in Render env vars
MUSIC_URL = "https://music.youtube.com"
VIDEO_ID_RE = re.compile(r"^[\w-]{11}$")
PLAYLIST_ID_RE = re.compile(r"^[\w-]{10,64}$")
MUSIC_SONGS_FILTER = "EgWKAQIIAWoMEA4QChADEAQQCRAF"  # YT Music "Songs" filter
ALLOWED_FORMATS = {"mp3", "m4a", "opus", "flac", "wav", "best"}

app = FastAPI(title="YouTube Music API", version="1.0.0")

# Enables CORS for frontend apps or direct browser access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)


@app.get("/", include_in_schema=False)
def gui():
    index_path = os.path.join(os.path.dirname(__file__), "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"status": "online", "message": "YouTube Music API is running."}


def ttl_cache(ttl: int = 900, maxsize: int = 300):
    """Tiny in-memory cache so repeat album/artist/playlist opens are instant."""
    def deco(fn):
        store: dict = {}

        @wraps(fn)
        def wrapper(*a, **kw):
            key = (a, tuple(sorted(kw.items())))
            hit = store.get(key)
            if hit and time.time() - hit[0] < ttl:
                return hit[1]
            val = fn(*a, **kw)  # exceptions are not cached
            store[key] = (time.time(), val)
            if len(store) > maxsize:
                store.pop(next(iter(store)))
            return val
        return wrapper
    return deco


def require_key(x_api_key: Optional[str] = Header(default=None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "Invalid or missing X-API-Key")


def base_opts(**extra) -> dict:
    """Configures yt-dlp with player clients, PO Tokens, and cookie authentication."""
    clients = os.getenv("YTDLP_PLAYER_CLIENT", "web,mweb,tv").split(",")
    youtube_args = {
        "player_client": clients,
    }

    # Pass Proof-of-Origin (PO) Token if configured in Render environment
    po_token = os.getenv("YTDLP_PO_TOKEN")
    visitor_data = os.getenv("YTDLP_VISITOR_DATA")
    if po_token:
        youtube_args["po_token"] = [po_token]
    if visitor_data:
        youtube_args["visitor_data"] = [visitor_data]

    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "socket_timeout": 20,
        "retries": 2,
        "extractor_retries": 1,
        "extractor_args": {
            "youtube": youtube_args,
        },
    }

    # YouTube now needs a JS runtime to solve its "n challenge" (otherwise: no formats / 403).
    runtimes = {name: {} for name in ("deno", "node") if shutil.which(name)}
    if runtimes:
        opts["js_runtimes"] = runtimes
    remote = os.getenv("YTDLP_REMOTE_COMPONENTS", "ejs:github")
    if remote:
        opts["remote_components"] = [c for c in remote.split(",") if c]

    # Option A: Cookie file (env path, or cookies.txt bundled next to this script).
    # Copied to /tmp because yt-dlp writes cookies back and the repo dir may be read-only.
    cookie_file = os.getenv("YTDLP_COOKIES") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies.txt")
    if os.path.exists(cookie_file):
        tmp_cookie = os.path.join(tempfile.gettempdir(), "yt_cookies_copy.txt")
        try:
            if not os.path.exists(tmp_cookie):
                shutil.copyfile(cookie_file, tmp_cookie)
            opts["cookiefile"] = tmp_cookie
        except Exception:
            opts["cookiefile"] = cookie_file

    # Option B: Raw cookies passed as text via environment variable (useful for Render)
    cookie_text = os.getenv("YTDLP_COOKIES_TEXT")
    if cookie_text and not opts.get("cookiefile"):
        clean_cookies = cookie_text.replace("\\n", "\n")
        tmp_cookie = os.path.join(tempfile.gettempdir(), "render_yt_cookies.txt")
        try:
            with open(tmp_cookie, "w", encoding="utf-8") as f:
                f.write(clean_cookies)
            opts["cookiefile"] = tmp_cookie
        except Exception:
            pass

    # Option C: Browser cookies (local environments)
    if os.getenv("YTDLP_COOKIES_FROM_BROWSER"):
        browser_spec = os.getenv("YTDLP_COOKIES_FROM_BROWSER").split(":")
        opts["cookiesfrombrowser"] = tuple(browser_spec)

    proxy = os.getenv("YTDLP_PROXY")  # e.g. http://user:pass@host:port (residential proxy)
    if proxy:
        opts["proxy"] = proxy

    opts.update(extra)
    return opts


def check_video_id(video_id: str) -> str:
    if not VIDEO_ID_RE.match(video_id):
        raise HTTPException(400, "Invalid video id")
    return video_id


# Client combos tried in order when YouTube rejects one (bot wall / "page needs to be reloaded").
CLIENT_FALLBACKS = [c for c in os.getenv(
    "YTDLP_CLIENT_FALLBACKS", "tv,web_safari;mweb;web;default"
).split(";") if c]
_RETRY_HINTS = ("reloaded", "Sign in to confirm", "not a bot", "player response", "n challenge", "Requested format")


def extract(url: str, opts: dict, download: bool = False) -> dict:
    import copy

    current = ",".join(opts.get("extractor_args", {}).get("youtube", {}).get("player_client", []))
    attempts = [None] + [c for c in CLIENT_FALLBACKS if c != current]
    last = None
    started = time.time()
    for clients in attempts:
        if last and time.time() - started > 70:   # stay under the platform request timeout
            break
        o = copy.deepcopy(opts)
        if clients:
            o.setdefault("extractor_args", {}).setdefault("youtube", {})["player_client"] = clients.split(",")
        try:
            with yt_dlp.YoutubeDL(o) as ydl:
                return ydl.extract_info(url, download=download)
        except yt_dlp.utils.DownloadError as e:
            last = re.sub(r"\x1b\[[0-9;]*m", "", str(e))
            if not any(h in last for h in _RETRY_HINTS):
                break
    raise HTTPException(502, f"yt-dlp error: {last}")


def thumb(info: dict) -> Optional[str]:
    if info.get("thumbnail"):
        return info["thumbnail"]
    thumbs = info.get("thumbnails") or []
    return thumbs[-1]["url"] if thumbs else None


def track_summary(info: dict) -> dict:
    artist = (
        info.get("artist")
        or (", ".join(info["artists"]) if info.get("artists") else None)
        or info.get("uploader")
        or info.get("channel")
    )
    if artist:
        artist = re.sub(r"\s*-\s*Topic$", "", artist)
    vid = info.get("id")
    return {
        "id": vid,
        "title": info.get("track") or info.get("title"),
        "artist": artist,
        "album": info.get("album"),
        "duration": info.get("duration"),
        "thumbnail": thumb(info) or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        "url": f"{MUSIC_URL}/watch?v={vid}",
    }


# ---- ytmusicapi (search, albums, artists, curated playlists) ------------------
try:
    from ytmusicapi import YTMusic
except ImportError:
    YTMusic = None

_ytm_client = None
ID_RE = re.compile(r"^[\w-]{5,80}$")
SEARCH_TYPES = {"songs", "albums", "artists", "playlists"}
_BAD_VIDEO_TYPES = ("MUSIC_VIDEO_TYPE_OMV", "MUSIC_VIDEO_TYPE_UGC")  # official / user music videos


def ytm():
    global _ytm_client
    if YTMusic is None:
        raise HTTPException(501, "ytmusicapi is not installed. Run: pip install ytmusicapi")
    if _ytm_client is None:
        try:
            _ytm_client = YTMusic(language="en", location="IN")
        except Exception:
            _ytm_client = YTMusic()
    return _ytm_client


def ytm_call(method: str, *args, **kw):
    try:
        return getattr(ytm(), method)(*args, **kw)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"YouTube Music error: {e}")


def check_id(x: str) -> str:
    if not ID_RE.match(x):
        raise HTTPException(400, "Invalid id")
    return x


def _best_thumb(thumbs):
    if not thumbs:
        return None
    return max(thumbs, key=lambda t: (t.get("width") or 0)).get("url")


def _artist_names(x: dict):
    names = []
    arts = x.get("artists")
    if isinstance(arts, list):
        names = [a.get("name") for a in arts if isinstance(a, dict) and a.get("name")]
    elif isinstance(arts, str):
        names = [arts]
    if not names and isinstance(x.get("artist"), str):
        names = [x["artist"]]
    return ", ".join(names) or None


def _secs(x: dict):
    if x.get("duration_seconds") is not None:
        return x["duration_seconds"]
    d = x.get("duration")
    if isinstance(d, (int, float)):
        return int(d)
    if isinstance(d, str) and re.fullmatch(r"(\d+:){1,2}\d+", d):
        n = 0
        for part in d.split(":"):
            n = n * 60 + int(part)
        return n
    return None


def _artist_list(x: dict):
    arts = x.get("artists")
    if not isinstance(arts, list):
        return []
    return [{"name": a["name"], "id": a.get("id")} for a in arts if isinstance(a, dict) and a.get("name")]


def _track(x: dict, thumb=None, artist=None, album=None, album_id=None):
    vid = x.get("videoId")
    if not vid or x.get("isAvailable") is False:
        return None
    alb = x.get("album")
    aid = alb.get("id") if isinstance(alb, dict) else None
    alb = alb.get("name") if isinstance(alb, dict) else (alb or album)
    return {
        "id": vid,
        "title": x.get("title"),
        "artist": _artist_names(x) or artist,
        "artists": _artist_list(x),
        "album": alb,
        "album_id": aid or album_id,
        "duration": _secs(x),
        "thumbnail": _best_thumb(x.get("thumbnails")) or thumb,
        "url": f"{MUSIC_URL}/watch?v={vid}",
    }


def _audio_only(items):
    """Drop music videos, keep audio tracks. If that would leave nothing, keep everything."""
    keep = [x for x in items if x.get("videoType") not in _BAD_VIDEO_TYPES]
    return keep or items


def _entity(x: dict, kind: str):
    if kind == "artist":
        id_, title, sub = x.get("browseId"), x.get("artist") or x.get("title"), "Artist"
    elif kind == "album":
        id_, title = x.get("browseId"), x.get("title")
        sub = " · ".join(str(p) for p in (x.get("type"), _artist_names(x), x.get("year")) if p)
    else:
        id_, title = x.get("browseId") or x.get("playlistId"), x.get("title")
        author = x.get("author")
        author = author if isinstance(author, str) else None
        sub = author or (str(x["itemCount"]) + " songs" if x.get("itemCount") else None)
        if id_ and id_.startswith("VL"):
            id_ = id_[2:]
    if not id_ or not title:
        return None
    return {"kind": kind, "id": id_, "title": title, "artist": sub, "thumbnail": _best_thumb(x.get("thumbnails"))}


def _search_songs_ytdlp(q: str, limit: int):
    """Fallback used only when ytmusicapi isn't installed."""
    info = extract(f"ytsearch{limit}:{q}", base_opts(extract_flat=True))
    return [track_summary(e) for e in (info.get("entries") or []) if e and e.get("id")][:limit]


@app.get("/search", dependencies=[Depends(require_key)])
def search(
    q: str = Query(..., min_length=1),
    type: str = Query("songs", description="songs, albums, artists or playlists"),
    limit: int = Query(20, ge=1, le=50),
):
    """Search YouTube Music for songs, albums, artists or playlists."""
    if type not in SEARCH_TYPES:
        raise HTTPException(400, f"type must be one of {sorted(SEARCH_TYPES)}")
    if YTMusic is None:
        if type != "songs":
            raise HTTPException(501, "Album/artist/playlist search needs ytmusicapi: pip install ytmusicapi")
        return {"query": q, "type": type, "results": _search_songs_ytdlp(q, limit)}
    raw = ytm_call("search", q, filter=type, limit=limit, ignore_spelling=True)
    if type == "songs":
        results = [t for t in map(_track, _audio_only(raw)) if t]
    else:
        kind = {"albums": "album", "artists": "artist", "playlists": "playlist"}[type]
        results = [e for e in (_entity(x, kind) for x in raw) if e]
    return {"query": q, "type": type, "results": results}


@app.get("/album/{browse_id}", dependencies=[Depends(require_key)])
@ttl_cache()
def album(browse_id: str):
    check_id(browse_id)
    a = ytm_call("get_album", browse_id)
    thumb = _best_thumb(a.get("thumbnails"))
    artist = _artist_names(a)
    arts = _artist_list(a)
    tracks = []
    for x in _audio_only(a.get("tracks") or []):
        t = _track(x, thumb=thumb, artist=artist, album=a.get("title"), album_id=browse_id)
        if t:
            t["thumbnail"] = thumb or t["thumbnail"]
            t["artists"] = t["artists"] or arts
            t["album_id"] = browse_id
            tracks.append(t)
    more = []
    if arts and arts[0].get("id"):
        try:
            ar = ytm().get_artist(arts[0]["id"])
            for key in ("albums", "singles"):
                for x in (ar.get(key) or {}).get("results") or []:
                    e = _entity(x, "album")
                    if e and e["id"] != browse_id:
                        more.append(e)
        except Exception:
            pass
    return {
        "kind": "album", "id": browse_id, "title": a.get("title"), "type": a.get("type"), "year": a.get("year"),
        "subtitle": " · ".join(str(p) for p in (artist, a.get("year"), a.get("type")) if p),
        "artists": arts, "thumbnail": thumb, "tracks": tracks, "more": more[:16],
    }


@app.get("/artist/{channel_id}", dependencies=[Depends(require_key)])
@ttl_cache()
def artist(channel_id: str):
    check_id(channel_id)
    a = ytm_call("get_artist", channel_id)
    block = a.get("songs") or {}
    raw = block.get("results") or []
    bid = block.get("browseId")
    if bid:
        try:
            full = ytm().get_playlist(bid[2:] if bid.startswith("VL") else bid, limit=50)
            if full.get("tracks"):
                raw = full["tracks"]
        except Exception:
            pass

    def ents(key):
        return [e for e in (_entity(x, "album") for x in ((a.get(key) or {}).get("results") or [])) if e]

    return {
        "kind": "artist",
        "id": channel_id,
        "name": a.get("name"),
        "subtitle": (str(a["subscribers"]) + " subscribers") if a.get("subscribers") else "Artist",
        "thumbnail": _best_thumb(a.get("thumbnails")),
        "description": a.get("description"),
        "views": a.get("views"),
        "songs": [t for t in map(_track, _audio_only(raw)) if t],
        "albums": ents("albums"),
        "singles": ents("singles"),
    }


@app.get("/track/{video_id}", dependencies=[Depends(require_key)])
def track(video_id: str):
    """Full metadata for a track."""
    check_video_id(video_id)
    info = extract(f"https://www.youtube.com/watch?v={video_id}", base_opts())
    return {
        **track_summary(info),
        "release_year": info.get("release_year"),
        "genre": info.get("genre"),
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "description": info.get("description"),
    }


AUDIO_FORMAT = "bestaudio[protocol^=http][protocol!*=m3u8]/bestaudio[protocol!*=m3u8]/bestaudio/best/ba*/b*"
_audio_cache: dict = {}
_AUDIO_TTL = 1500  # googlevideo URLs live for hours; refresh well before that


def _resolve_audio(video_id: str, force: bool = False) -> dict:
    hit = _audio_cache.get(video_id)
    if hit and not force and time.time() - hit["t"] < _AUDIO_TTL:
        return hit
    info = extract(f"https://www.youtube.com/watch?v={video_id}", base_opts(format=AUDIO_FORMAT))
    entry = {
        "t": time.time(),
        "url": info.get("url"),
        "headers": info.get("http_headers") or {},
        "protocol": info.get("protocol") or "",
        "meta": {
            **track_summary(info),
            "ext": info.get("ext"),
            "abr": info.get("abr"),
            "acodec": info.get("acodec"),
            "filesize": info.get("filesize") or info.get("filesize_approx"),
        },
    }
    _audio_cache[video_id] = entry
    if len(_audio_cache) > 200:
        _audio_cache.pop(next(iter(_audio_cache)))
    return entry


def _public_base(request: Request) -> str:
    env = os.getenv("PUBLIC_URL") or os.getenv("RENDER_EXTERNAL_URL")  # Render sets the latter
    if env:
        return env.rstrip("/")
    h = request.headers
    scheme = (h.get("x-forwarded-proto") or request.url.scheme).split(",")[0].strip()
    host = (h.get("x-forwarded-host") or h.get("host") or request.url.netloc).split(",")[0].strip()
    return f"{scheme}://{host}"


@app.get("/stream/{video_id}", dependencies=[Depends(require_key)])
def stream(video_id: str, request: Request):
    """Playable audio URL. Proxied through this server so it works from any client IP."""
    check_video_id(video_id)
    e = _resolve_audio(video_id)
    if not e["url"]:
        raise HTTPException(502, "No playable audio stream found for this track")
    if os.getenv("DIRECT_STREAM") == "1":  # localhost only: hand out the raw googlevideo URL
        audio_url = e["url"]
    else:
        audio_url = f"{_public_base(request)}/audio/{video_id}"
        if API_KEY:
            audio_url += f"?key={quote(API_KEY)}"
    return {**e["meta"], "audio_url": audio_url}


def _upstream_opener():
    proxy = os.getenv("YTDLP_PROXY")  # googlevideo URLs are tied to the proxy IP too
    return build_opener(ProxyHandler({"http": proxy, "https": proxy})) if proxy else build_opener()


_PASS_HEADERS = ("content-type", "content-length", "content-range", "accept-ranges")


@app.get("/audio/{video_id}", include_in_schema=False)
def audio(video_id: str, request: Request, key: Optional[str] = Query(default=None)):
    """Byte-range proxy for the audio stream (the <audio> element can't send X-API-Key)."""
    check_video_id(video_id)
    if API_KEY and key != API_KEY and request.headers.get("x-api-key") != API_KEY:
        raise HTTPException(401, "Invalid or missing key")
    rng = request.headers.get("range")
    up = None
    for attempt in (0, 1):
        e = _resolve_audio(video_id, force=bool(attempt))
        if not e["url"]:
            raise HTTPException(502, "No playable audio stream found for this track")
        if "m3u8" in e["protocol"] or ".m3u8" in e["url"]:
            return RedirectResponse(e["url"])          # HLS can't be byte-proxied
        h = {k: v for k, v in e["headers"].items() if k.lower() not in ("accept-encoding", "range", "host")}
        h.setdefault("User-Agent", "Mozilla/5.0")
        if rng:
            h["Range"] = rng
        try:
            up = _upstream_opener().open(UrlRequest(e["url"], headers=h), timeout=25)
            break
        except HTTPError as err:
            if err.code == 416:
                return Response(status_code=416, headers={"Content-Range": err.headers.get("Content-Range", "")})
            if err.code in (403, 404, 410) and attempt == 0:
                _audio_cache.pop(video_id, None)       # expired / blocked: re-resolve once
                continue
            raise HTTPException(502, f"Upstream audio error {err.code}")
        except Exception as err:
            raise HTTPException(502, f"Upstream audio error: {err}")

    headers = {k: up.headers[k] for k in _PASS_HEADERS if up.headers.get(k)}
    headers.setdefault("Accept-Ranges", "bytes")
    headers["Cache-Control"] = "no-store"

    def body():
        try:
            while True:
                chunk = up.read(64 * 1024)
                if not chunk:
                    break
                yield chunk
        finally:
            up.close()

    return StreamingResponse(body(), status_code=up.status, headers=headers,
                             media_type=headers.pop("content-type", None) or up.headers.get_content_type())


def _cleanup(path: str):
    shutil.rmtree(path, ignore_errors=True)


@app.get("/download/{video_id}", dependencies=[Depends(require_key)])
def download(
    video_id: str,
    fmt: str = Query("mp3", description="mp3, m4a, opus, flac, wav, or best (no conversion)"),
    quality: str = Query("192", description="Bitrate in kbps for lossy formats"),
):
    """Download audio and return it as a file. Requires ffmpeg unless fmt=best."""
    check_video_id(video_id)
    if fmt not in ALLOWED_FORMATS:
        raise HTTPException(400, f"fmt must be one of {sorted(ALLOWED_FORMATS)}")
    if not quality.isdigit():
        raise HTTPException(400, "quality must be a number")

    tmp = tempfile.mkdtemp(prefix="ytm_")
    opts = base_opts(
        skip_download=False,
        format="bestaudio[protocol!*=m3u8]/bestaudio/best/ba*/b*",
        outtmpl=os.path.join(tmp, "%(artist,uploader)s - %(title)s.%(ext)s"),
        restrictfilenames=True,
        writethumbnail=False,
    )
    if fmt != "best" and not shutil.which("ffmpeg"):
        fmt = "best"  # no ffmpeg on this host: hand back the native audio file instead of failing
    if fmt != "best":
        opts["postprocessors"] = [
            {"key": "FFmpegExtractAudio", "preferredcodec": fmt, "preferredquality": quality},
            {"key": "FFmpegMetadata", "add_metadata": True},
        ]

    try:
        extract(f"https://www.youtube.com/watch?v={video_id}", opts, download=True)
        files = os.listdir(tmp)
        if not files:
            raise HTTPException(500, "Download produced no file")
        path = os.path.join(tmp, files[0])
        return FileResponse(
            path,
            filename=files[0],
            background=BackgroundTask(_cleanup, tmp),
        )
    except Exception:
        _cleanup(tmp)
        raise


@app.get("/playlist/{playlist_id}", dependencies=[Depends(require_key)])
@ttl_cache()
def playlist(playlist_id: str, limit: int = Query(100, ge=1, le=500)):
    """Tracks in a YouTube Music playlist or album playlist."""
    if not PLAYLIST_ID_RE.match(playlist_id):
        raise HTTPException(400, "Invalid playlist id")
    if YTMusic is not None:
        try:
            p = ytm().get_playlist(playlist_id, limit=limit)
            tracks = [t for t in map(_track, _audio_only(p.get("tracks") or [])) if t]
            if tracks:
                author = p.get("author")
                author = author.get("name") if isinstance(author, dict) else author
                sub = " · ".join(str(x) for x in (author, f"{len(tracks)} songs") if x)
                return {
                    "kind": "playlist", "id": playlist_id, "title": p.get("title"), "subtitle": sub,
                    "thumbnail": _best_thumb(p.get("thumbnails")), "track_count": len(tracks), "tracks": tracks,
                }
        except Exception:
            pass
    info = extract(
        f"https://www.youtube.com/playlist?list={playlist_id}",
        base_opts(extract_flat=True, noplaylist=False, playlistend=limit),
    )
    tracks = [track_summary(e) for e in info.get("entries", []) if e]
    return {
        "kind": "playlist", "id": info.get("id"), "title": info.get("title"),
        "subtitle": f"{len(tracks)} songs", "thumbnail": None,
        "track_count": len(tracks), "tracks": tracks,
    }


# ---- Cover art (auto-cropped to a square) ------------------------------------
@lru_cache(maxsize=64)
def _fetch_thumb(vid: str):
    for name in ("maxresdefault", "sddefault", "hqdefault", "mqdefault"):
        try:
            req = UrlRequest(f"https://i.ytimg.com/vi/{vid}/{name}.jpg", headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=10) as r:
                data = r.read()
            if len(data) > 2000:
                return data
        except Exception:
            continue
    return None


def _trim_borders(img, tol: int = 28):
    start = img.size
    for _ in range(3):
        w, h = img.size
        bg = Image.new("RGB", img.size, img.getpixel((2, 2)))
        diff = ImageChops.difference(img, bg).convert("L").point(lambda p: 255 if p > tol else 0)
        box = diff.getbbox()
        if not box or box == (0, 0, w, h):
            break
        l, t, r, b = box
        if (r - l) < w * 0.3 or (b - t) < h * 0.3:
            break
        img = img.crop(box)
    if img.size != start:
        w, h = img.size
        if w > 8 and h > 8:
            img = img.crop((2, 2, w - 2, h - 2))
    return img


@lru_cache(maxsize=256)
def _cover_bytes(vid: str, size: int):
    raw = _fetch_thumb(vid)
    if raw is None or Image is None:
        return raw
    img = Image.open(io.BytesIO(raw)).convert("RGB")
    img = _trim_borders(img)
    w, h = img.size
    side = min(w, h)
    left, top = (w - side) // 2, (h - side) // 2
    img = img.crop((left, top, left + side, top + side)).resize((size, size), Image.LANCZOS)
    out = io.BytesIO()
    img.save(out, "JPEG", quality=88, optimize=True)
    return out.getvalue()


@app.get("/cover/{video_id}", include_in_schema=False)
def cover(video_id: str, s: int = Query(400, ge=64, le=1000)):
    """Square, border-free cover art."""
    check_video_id(video_id)
    data = _cover_bytes(video_id, s)
    if data is None:
        raise HTTPException(404, "No thumbnail found")
    return Response(data, media_type="image/jpeg", headers={"Cache-Control": "public, max-age=86400"})


# ---- Home feed -------------------------------------------------------------
KOLLYWOOD_HITLIST = "RDCLAK5uy_nTbyVypdXPQd00z15bTWjZr7pG-26yyQ4"
HOME_PLAN = [
    {"id": "kollywood", "type": "playlist", "title": "Kollywood Hitlist", "playlist": KOLLYWOOD_HITLIST},
    {"id": "tamil_hits", "type": "playlist_search", "query": "Tamil hits"},
    {"id": "tamil_love", "type": "playlist_search", "query": "Tamil love songs"},
    {"id": "tamil_90s", "type": "playlist_search", "query": "Tamil 90s"},
    {"id": "tamil_party", "type": "playlist_search", "query": "Tamil dance party"},
    {"id": "anirudh", "type": "songs", "title": "Anirudh Ravichander", "query": "Anirudh Ravichander"},
    {"id": "arr", "type": "songs", "title": "A. R. Rahman · Tamil", "query": "A. R. Rahman Tamil"},
    {"id": "raja", "type": "songs", "title": "Ilaiyaraaja", "query": "Ilaiyaraaja"},
    {"id": "pls", "type": "featured_playlists", "title": "Tamil playlists", "query": "Tamil"},
    {"id": "albums", "type": "albums", "title": "Tamil albums", "query": "Tamil movie soundtrack"},
    {"id": "artists", "type": "artists", "title": "Tamil artists", "query": "Tamil playback singer"},
]

HOME_FALLBACK = [("kollywood", "Kollywood Hitlist", [f"{MUSIC_URL}/playlist?list={KOLLYWOOD_HITLIST}"])]
_home_cache = {"t": 0.0, "data": None}


def _load_section(sec):
    sid, title, sources = sec
    for src in sources:
        try:
            with yt_dlp.YoutubeDL(base_opts(extract_flat=True, playlistend=25)) as ydl:
                info = ydl.extract_info(src, download=False)
            items = [track_summary(e) for e in (info.get("entries") or []) if e and e.get("id")]
            if items:
                return {"id": sid, "title": title, "tracks": items[:25]}
        except Exception:
            continue
    return {"id": sid, "title": title, "tracks": []}


def _playlist_tracks(pid: str, limit: int = 25):
    p = ytm().get_playlist(pid, limit=limit)
    return p, [t for t in map(_track, _audio_only(p.get("tracks") or [])) if t]


def _load_home_section(spec):
    sid, typ = spec["id"], spec["type"]
    empty = {"id": sid, "title": spec.get("title", ""), "tracks": []}
    try:
        if typ == "playlist":
            p, tracks = _playlist_tracks(spec["playlist"])
            return {"id": sid, "title": spec.get("title") or p.get("title"), "tracks": tracks[:25]}
        if typ == "playlist_search":
            hits = ytm().search(spec["query"], filter="featured_playlists", limit=5, ignore_spelling=True)
            for h in hits:
                ent = _entity(h, "playlist")
                if not ent:
                    continue
                try:
                    _, tracks = _playlist_tracks(ent["id"])
                except Exception:
                    continue
                if len(tracks) >= 5:
                    return {"id": sid, "title": ent["title"], "tracks": tracks[:25]}
            return empty
        if typ == "songs":
            raw = ytm().search(spec["query"], filter="songs", limit=25, ignore_spelling=True)
            return {"id": sid, "title": spec["title"], "tracks": [t for t in map(_track, _audio_only(raw)) if t]}
        kind = {"featured_playlists": "playlist", "albums": "album", "artists": "artist"}[typ]
        raw = ytm().search(spec["query"], filter=typ, limit=20, ignore_spelling=True)
        items = [e for e in (_entity(x, kind) for x in raw) if e]
        return {"id": sid, "title": spec["title"], "items": items}
    except Exception:
        return empty


@app.get("/home", dependencies=[Depends(require_key)])
def home(refresh: bool = False):
    """India / Tamil home feed. Cached for 30 minutes."""
    if not refresh and _home_cache["data"] and time.time() - _home_cache["t"] < 1800:
        return _home_cache["data"]
    note = None
    if YTMusic is None:
        sections = [_load_section(s) for s in HOME_FALLBACK]
        note = "Install ytmusicapi for the full Tamil home feed."
    else:
        with ThreadPoolExecutor(max_workers=4) as pool:
            sections = list(pool.map(_load_home_section, HOME_PLAN))
    seen, out = set(), []
    for sec in sections:
        if not (sec.get("tracks") or sec.get("items")):
            continue
        key = (sec["title"] or "").casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(sec)
    data = {"sections": out, "note": note}
    if len(out) >= 3:
        _home_cache.update(t=time.time(), data=data)
    return data


MOODS = ["Energize", "Feel good", "Relax", "Workout", "Party", "Commute", "Romance", "Sad", "Focus", "Sleep"]
_mood_cache: dict = {}


@app.get("/mood", dependencies=[Depends(require_key)])
def mood(name: str = Query(...)):
    """Tamil-leaning rows for a mood chip."""
    if name not in MOODS:
        raise HTTPException(400, f"name must be one of {MOODS}")
    hit = _mood_cache.get(name)
    if hit and time.time() - hit[0] < 1800:
        return hit[1]
    if YTMusic is None:
        raise HTTPException(501, "Moods need ytmusicapi: pip install ytmusicapi")
    low = name.lower()
    plan = [
        {"id": "a", "type": "playlist_search", "query": f"Tamil {low}"},
        {"id": "b", "type": "playlist_search", "query": f"{low} Tamil songs"},
        {"id": "c", "type": "playlist_search", "query": f"{low} Kollywood"},
        {"id": "d", "type": "featured_playlists", "title": f"{name} playlists", "query": f"Tamil {low}"},
        {"id": "e", "type": "songs", "title": f"{name} · songs", "query": f"Tamil {low} songs"},
    ]
    with ThreadPoolExecutor(max_workers=4) as pool:
        sections = list(pool.map(_load_home_section, plan))
    seen, out = set(), []
    for sec in sections:
        if not (sec.get("tracks") or sec.get("items")):
            continue
        key = (sec["title"] or "").casefold()
        if key not in seen:
            seen.add(key)
            out.append(sec)
    data = {"name": name, "sections": out}
    if out:
        _mood_cache[name] = (time.time(), data)
    return data


@app.get("/debug", dependencies=[Depends(require_key)])
def debug():
    """Shows whether cookies/JS runtime are visible to the server (no secrets returned)."""
    here = os.path.dirname(os.path.abspath(__file__))
    bundled = os.path.join(here, "cookies.txt")
    opts = base_opts()
    cf = opts.get("cookiefile")
    names, bad_lines = [], 0
    if cf and os.path.exists(cf):
        for line in open(cf, encoding="utf-8", errors="ignore"):
            if line.startswith("#") or not line.strip():
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) == 7:
                names.append(parts[5])
            else:
                bad_lines += 1
    return {
        "yt_dlp": yt_dlp.version.__version__,
        "cookies_txt_in_repo": os.path.exists(bundled),
        "cookiefile_used": bool(cf),
        "cookie_count": len(names),
        "has_SID": "SID" in names, "has_PSIDTS": "__Secure-1PSIDTS" in names,
        "malformed_cookie_lines": bad_lines,
        "player_clients": opts["extractor_args"]["youtube"]["player_client"],
        "proxy_set": bool(opts.get("proxy")),
        "deno": shutil.which("deno"), "node": shutil.which("node"),
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "yt_dlp": yt_dlp.version.__version__,
        "ffmpeg": bool(shutil.which("ffmpeg")),
    }


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
