"""
Endpoints:
  GET /search?q=...&type=songs|albums|artists|playlists&limit=20
  GET /album/{browse_id}              Album + tracks
  GET /artist/{channel_id}            Artist: top songs, albums, singles
  GET /home                           Tamil / India home feed (songs, playlists, albums, artists)
  GET /track/{video_id}               Track metadata
  GET /stream/{video_id}              Direct audio URL (expires after a few hours)
  GET /sources/{video_id}?probe=false List every source (client + format) with a network stream link each
  GET /stream/{video_id}?client=&fmt= Pin a specific source from /sources
  GET /download/{video_id}?fmt=mp3    Download audio file (needs ffmpeg for conversion)
  GET /playlist/{playlist_id}         Playlist / album track list
  GET /mood?name=Relax                Mood feeds
  GET /lyrics/{video_id}?title=&artist=&album=&isrc=&source=auto|unison|lrcred
                                      Synced lyrics (Unison + lrc.red), parsed into timed lines
  GET /import?url=...                 Import a PUBLIC / unlisted YouTube playlist (all tracks)
  GET /charts/countries               Countries available for charts
  GET /charts/{code}                  Chart playlists + top songs + top artists (ZZ = global)
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
from urllib.parse import quote, urlencode
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


# ---- Embedded YouTube cookies --------------------------------------------------
# Only the youtube.com / google.com / accounts.google.com cookies from your export
# (62 of the 7,449 in the original file), deduplicated, zlib-compressed + base64.
# SENSITIVE: this is a logged-in session for your Google account. Keep this repo
# PRIVATE, and never paste this file anywhere public. Env vars below override it.
_EMBEDDED_COOKIES_B64 = (
    ""
)


def _write_embedded_cookies() -> Optional[str]:
    import base64
    import zlib
    raw = "".join(_EMBEDDED_COOKIES_B64.split())
    if not raw:
        return None
    try:
        text = zlib.decompress(base64.b64decode(raw)).decode("utf-8")
        path = os.path.join(tempfile.gettempdir(), "yt_embedded_cookies.txt")
        with open(path, "w", encoding="utf-8") as f:  # fresh copy each boot; yt-dlp may rewrite it
            f.write(text)
        return path
    except Exception:
        return None


EMBEDDED_COOKIE_FILE = _write_embedded_cookies()


def base_opts(**extra) -> dict:
    """Configures yt-dlp with player clients, PO Tokens, and cookie authentication."""
    # Changed default from "web,mweb,tv" to mobile clients
    clients = os.getenv("YTDLP_PLAYER_CLIENT", "android,ios,mweb").split(",")
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

    # Option A2: cookies embedded in this file (used when no cookie file / path is configured)
    if not opts.get("cookiefile") and EMBEDDED_COOKIE_FILE:
        opts["cookiefile"] = EMBEDDED_COOKIE_FILE

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


AUDIO_FORMAT = "bestaudio[protocol^=http][protocol!*=m3u8]/bestaudio/best/ba*/b"
_audio_cache: dict = {}
CHUNK = 10 * 1024 * 1024  # googlevideo max ranged chunk
_FMT_RE = re.compile(r"^[\w\-\.\+]{1,60}$")
_CLIENT_RE = re.compile(r"^[\w,]{1,60}$")


def _akey(video_id: str, client: Optional[str] = None, fmt: Optional[str] = None) -> tuple:
    return (video_id, client or "", fmt or "")


def _check_src(client: Optional[str], fmt: Optional[str]):
    if client and not _CLIENT_RE.match(client):
        raise HTTPException(400, "Invalid client")
    if fmt and not _FMT_RE.match(fmt):
        raise HTTPException(400, "Invalid fmt")


_AUDIO_TTL = 1500  # googlevideo URLs live for hours; refresh well before that


# Clients tried (in order) to get a real audio-only stream that YouTube will actually serve.
# tv / android_vr usually work without a PO token; web often only exposes format 18 (video+audio) -> 403.
AUDIO_CLIENTS = [c for c in os.getenv(
    "YTDLP_AUDIO_CLIENTS", "tv;android_vr;mweb;web_safari;web,mweb,tv"
).split(";") if c]


def _upstream_opener():
    proxy = os.getenv("YTDLP_PROXY")  # googlevideo URLs are tied to the proxy IP too
    return build_opener(ProxyHandler({"http": proxy, "https": proxy})) if proxy else build_opener()


def _probe(url: str, headers: dict) -> bool:
    """Tiny ranged request: does YouTube actually serve this URL to us?"""
    h = {k: v for k, v in headers.items() if k.lower() not in ("accept-encoding", "range", "host")}
    h.setdefault("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    h["Range"] = f"bytes=0-{CHUNK - 1}"   # same shape as the real proxy request; a 2-byte probe gave false "playable"
    try:
        with _upstream_opener().open(UrlRequest(url, headers=h), timeout=12) as r:
            r.read(2)
        return True
    except Exception:
        return False


def _resolve_audio(video_id: str, force: bool = False, client: Optional[str] = None, fmt: Optional[str] = None) -> dict:
    """Auto mode picks the best playable audio. With client/fmt it resolves exactly that source."""
    key = _akey(video_id, client, fmt)
    pinned = bool(client or fmt)
    hit = _audio_cache.get(key)
    if hit and not force and time.time() - hit["t"] < _AUDIO_TTL:
        return hit
    url = f"https://www.youtube.com/watch?v={video_id}"
    audio_only = playable_mixed = any_info = None
    last, started = None, time.time()
    simple = not pinned
    if simple:   # same as main.py: one extract, falls back to the muxed mp4 when no audio-only stream exists
        any_info = extract(url, base_opts(format=AUDIO_FORMAT))
    for clients in ([] if simple else ([client] if client else AUDIO_CLIENTS)):
        if time.time() - started > 60:
            break
        opts = base_opts(format=fmt or AUDIO_FORMAT)
        opts["extractor_args"]["youtube"]["player_client"] = clients.split(",")
        if clients.split(",")[0] == "android_vr":
            opts.pop("cookiefile", None)       # android_vr doesn't support cookies; yt-dlp would skip it
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
        except yt_dlp.utils.DownloadError as e:
            last = re.sub(r"\x1b\[[0-9;]*m", "", str(e))
            continue
        if not info or not info.get("url"):
            continue
        any_info = any_info or info
        is_audio = info.get("vcodec") in (None, "none")
        ok = True if pinned else _probe(info["url"], info.get("http_headers"))
        print(f"[resolve] {video_id}: client={clients} format={info.get('format_id')} audio_only={is_audio} playable={ok}", flush=True)
        if pinned:
            audio_only = info          # caller chose this source explicitly
            break
        if ok and is_audio:
            audio_only = info
            break
        if ok and not playable_mixed:
            playable_mixed = info
    info = audio_only or playable_mixed or any_info
    if info is None:
        raise HTTPException(502, f"yt-dlp error: {last or 'no playable audio found'}")
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
            "vcodec": info.get("vcodec"),
            "format_id": info.get("format_id"),
            "protocol": info.get("protocol"),
            "has_video": (info.get("vcodec") not in (None, "none")),
            "filesize": info.get("filesize") or info.get("filesize_approx"),
        },
    }
    _audio_cache[key] = entry
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


def _audio_link(request: Request, video_id: str, client: Optional[str] = None, fmt: Optional[str] = None) -> str:
    q = {}
    if client:
        q["client"] = client
    if fmt:
        q["fmt"] = fmt
    if API_KEY:
        q["key"] = API_KEY
    return f"{_public_base(request)}/audio/{video_id}" + (f"?{urlencode(q, quote_via=quote)}" if q else "")


@app.get("/stream/{video_id}", dependencies=[Depends(require_key)])
def stream(
    video_id: str,
    request: Request,
    client: Optional[str] = Query(default=None, description="pin a player client, see /sources"),
    fmt: Optional[str] = Query(default=None, description="pin a format_id, see /sources"),
):
    """Playable audio URL. Proxied through this server so it works from any client IP."""
    check_video_id(video_id)
    _check_src(client, fmt)
    e = _resolve_audio(video_id, client=client, fmt=fmt)
    if not e["url"]:
        raise HTTPException(502, "No playable audio stream found for this track")
    if os.getenv("DIRECT_STREAM") == "1":  # localhost only: hand out the raw googlevideo URL
        audio_url = e["url"]
    else:
        audio_url = _audio_link(request, video_id, client, fmt)
    return {**e["meta"], "audio_url": audio_url, "sources_url": f"{_public_base(request)}/sources/{video_id}"}


def _list_client_formats(video_id: str, clients: str) -> dict:
    """All formats one player-client combo exposes (no format selection, so it never fails with 'not available')."""
    opts = base_opts(ignore_no_formats_error=True)
    opts["extractor_args"]["youtube"]["player_client"] = clients.split(",")
    if clients.split(",")[0] == "android_vr":
        opts.pop("cookiefile", None)
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False) or {}
    except yt_dlp.utils.DownloadError as e:
        return {"client": clients, "error": re.sub(r"\x1b\[[0-9;]*m", "", str(e)).strip(), "formats": []}
    return {"client": clients, "info": info, "formats": info.get("formats") or []}


@app.get("/sources/{video_id}", dependencies=[Depends(require_key)])
def sources(
    video_id: str,
    request: Request,
    probe: bool = Query(False, description="test each source against YouTube and report playable true/false (slower)"),
    audio_only: bool = Query(False, description="hide sources that contain video"),
    direct: bool = Query(False, description="also include the raw googlevideo URL (IP-locked to this server)"),
):
    """Every audio/video source YouTube exposes for this id, per player client, each with a network stream link.

    stream_url goes through this server (/audio) so it plays from any IP. Pass a source's
    `client` and `format_id` to /stream or /audio to pin it.
    """
    check_video_id(video_id)
    started = time.time()
    with ThreadPoolExecutor(max_workers=min(len(AUDIO_CLIENTS), 6) or 1) as ex:
        results = list(ex.map(lambda c: _list_client_formats(video_id, c), AUDIO_CLIENTS))

    out, errors, meta, seen = [], {}, None, set()
    for r in results:
        if r.get("error"):
            errors[r["client"]] = r["error"]
        if r.get("info") and not meta:
            meta = track_summary(r["info"])
        for f in r["formats"]:
            url = f.get("url")
            proto = f.get("protocol") or ""
            vc, ac = f.get("vcodec") or "none", f.get("acodec") or "none"
            if not url or (vc == "none" and ac == "none"):   # storyboards / SABR-only entries with no url
                continue
            kind = "audio" if vc == "none" else ("video" if ac == "none" else "muxed")
            if audio_only and kind != "audio":
                continue
            sid = f"{r['client']}:{f.get('format_id')}"
            if sid in seen:
                continue
            seen.add(sid)
            item = {
                "id": sid,
                "client": r["client"],
                "format_id": f.get("format_id"),
                "kind": kind,
                "ext": f.get("ext"),
                "acodec": None if ac == "none" else ac,
                "vcodec": None if vc == "none" else vc,
                "abr": f.get("abr"),
                "tbr": f.get("tbr"),
                "asr": f.get("asr"),
                "height": f.get("height"),
                "quality": f.get("format_note"),
                "language": f.get("language"),
                "protocol": proto,
                "filesize": f.get("filesize") or f.get("filesize_approx"),
                "stream_url": _audio_link(request, video_id, r["client"], f.get("format_id")),
                "_url": url,
                "_headers": f.get("http_headers"),
            }
            if direct:
                item["direct_url"] = url
            out.append(item)

    if probe:
        def run(it):
            if "m3u8" in it["protocol"]:
                it["playable"] = None        # HLS is redirected, not byte-proxied
            else:
                it["playable"] = _probe(it["_url"], it["_headers"])
        with ThreadPoolExecutor(max_workers=8) as ex:
            list(ex.map(run, out))
    for it in out:
        it.pop("_url", None)
        it.pop("_headers", None)

    # best first: playable, audio before muxed before video, then bitrate
    rank = {"audio": 0, "muxed": 1, "video": 2}
    out.sort(key=lambda i: (i.get("playable") is False, rank[i["kind"]], -(i["abr"] or i["tbr"] or 0)))
    return {
        "video_id": video_id,
        **(meta or {}),
        "auto_stream_url": _audio_link(request, video_id),
        "count": len(out),
        "probed": probe,
        "elapsed_s": round(time.time() - started, 1),
        "sources": out,
        "client_errors": errors,
    }


_PASS_HEADERS = ("content-type", "content-length", "content-range", "accept-ranges")


@app.get("/audio/{video_id}", include_in_schema=False)
def audio(
    video_id: str,
    request: Request,
    key: Optional[str] = Query(default=None),
    client: Optional[str] = Query(default=None),
    fmt: Optional[str] = Query(default=None),
):
    """Byte-range proxy for the audio stream (the <audio> element can't send X-API-Key)."""
    check_video_id(video_id)
    _check_src(client, fmt)
    if API_KEY and key != API_KEY and request.headers.get("x-api-key") != API_KEY:
        raise HTTPException(401, "Invalid or missing key")
    rng = request.headers.get("range")
    m = re.match(r"bytes=(\d*)-(\d*)", rng or "")
    start = int(m.group(1)) if m and m.group(1) else 0       # suffix ranges (bytes=-N) fall back to 0
    end_req = int(m.group(2)) if m and m.group(2) else None
    up, last_err, chunked = None, "unknown", False
    simple = not (client or fmt)
    for attempt in ((0, 1) if simple else (0, 1, 2)):
        e = _resolve_audio(video_id, force=(attempt == 1), client=client, fmt=fmt)
        if not e["url"]:
            raise HTTPException(502, "No playable audio stream found for this track")
        if "m3u8" in e["protocol"] or ".m3u8" in e["url"]:
            return RedirectResponse(e["url"])          # HLS can't be byte-proxied
        h = {k: v for k, v in e["headers"].items() if k.lower() not in ("accept-encoding", "range", "host")}
        h.setdefault("User-Agent", "Mozilla/5.0")
        # googlevideo (esp. android/ios clients) refuses open-ended or whole-file requests (403):
        # fetch it in <=10 MB ranged chunks, like yt-dlp does.
        chunked = not simple and attempt != 2 and "googlevideo.com" in e["url"]
        if chunked:
            stop = start + CHUNK - 1 if end_req is None else min(end_req, start + CHUNK - 1)
            h["Range"] = f"bytes={start}-{stop}"
        elif rng and (simple or attempt != 2):
            h["Range"] = rng
        try:
            up = _upstream_opener().open(UrlRequest(e["url"], headers=h), timeout=25)
            break
        except HTTPError as err:
            if err.code == 416:
                return Response(status_code=416, headers={"Content-Range": err.headers.get("Content-Range", "")})
            last_err = f"HTTP {err.code} from YouTube (chunked={chunked}, format={e['meta'].get('format_id')}, video={e['meta'].get('has_video')})"
            print(f"[audio] {video_id}: {last_err}", flush=True)
            if attempt == 0 and err.code in (403, 404, 410):
                _audio_cache.pop(_akey(video_id, client, fmt), None)       # expired / blocked: re-resolve
        except Exception as err:
            last_err = f"{type(err).__name__}: {err}"
            print(f"[audio] {video_id}: {last_err}", flush=True)
    if up is None:
        raise HTTPException(502, f"Upstream audio error: {last_err}")

    ctype = up.headers.get_content_type()
    cr = re.match(r"bytes (\d+)-(\d+)/(\d+)", up.headers.get("Content-Range", ""))
    if chunked and up.status == 206 and cr:
        total, first_end = int(cr.group(3)), int(cr.group(2))
        last = total - 1 if end_req is None else min(end_req, total - 1)
        headers = {"Accept-Ranges": "bytes", "Cache-Control": "no-store", "Content-Length": str(last - start + 1)}
        if rng:
            headers["Content-Range"] = f"bytes {start}-{last}/{total}"

        def chunk_body():
            cur, nxt = up, first_end + 1
            try:
                while True:
                    while True:
                        data = cur.read(64 * 1024)
                        if not data:
                            break
                        yield data
                    cur.close()
                    cur = None
                    if nxt > last:
                        return
                    h2 = {**h, "Range": f"bytes={nxt}-{min(nxt + CHUNK - 1, last)}"}
                    try:
                        cur = _upstream_opener().open(UrlRequest(e["url"], headers=h2), timeout=25)
                    except Exception as err:
                        print(f"[audio] {video_id}: chunk at {nxt} failed: {err}", flush=True)
                        return
                    nxt = min(nxt + CHUNK - 1, last) + 1
            finally:
                if cur is not None:
                    cur.close()

        return StreamingResponse(chunk_body(), status_code=206 if rng else 200, headers=headers, media_type=ctype)

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
                             media_type=headers.pop("content-type", None) or ctype)


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



# ---- Import a YouTube playlist by link ------------------------------------------
YT_LIST_RE = re.compile(r"[?&]list=([\w-]+)")
_BLOCKED_LISTS = {"WL": "Watch Later", "LL": "Liked videos", "LM": "Liked music"}


@app.get("/import", dependencies=[Depends(require_key)])
def import_playlist(
    url: str = Query(..., min_length=3, description="YouTube / YouTube Music playlist link (or id)"),
    limit: int = Query(1000, ge=1, le=5000),
):
    """Fetch EVERY track of a public/unlisted playlist so the client can copy it into its library."""
    url = url.strip()
    m = YT_LIST_RE.search(url)
    pid = m.group(1) if m else (url if PLAYLIST_ID_RE.match(url) else None)
    if not pid:
        raise HTTPException(400, "That doesn't look like a YouTube playlist link.")
    if pid in _BLOCKED_LISTS:
        raise HTTPException(403, f"{_BLOCKED_LISTS[pid]} is private and can't be imported. Copy those songs into a public playlist first.")
    private_msg = ("Couldn't read that playlist. It may be private or deleted. "
                   "Set it to Public or Unlisted on YouTube and try again.")

    title = thumbnail = None
    tracks: list = []
    if YTMusic is not None:
        try:
            p = ytm().get_playlist(pid, limit=limit)
            tracks = [t for t in map(_track, p.get("tracks") or []) if t]
            title, thumbnail = p.get("title"), _best_thumb(p.get("thumbnails"))
        except Exception:
            tracks = []
    if not tracks:  # fallback: plain yt-dlp
        try:
            info = extract(
                f"https://www.youtube.com/playlist?list={pid}",
                base_opts(extract_flat=True, noplaylist=False, playlistend=limit),
            )
        except HTTPException:
            raise HTTPException(404, private_msg)
        entries = [
            e for e in (info.get("entries") or [])
            if e and e.get("id") and not str(e.get("title") or "").startswith(("[Private", "[Deleted"))
        ]
        tracks = [track_summary(e) for e in entries]
        title = title or info.get("title")
    seen, uniq = set(), []
    for t in tracks:
        if t["id"] not in seen:
            seen.add(t["id"])
            uniq.append(t)
    if not uniq:
        raise HTTPException(404, private_msg)
    return {
        "kind": "playlist", "id": pid, "title": title or "Imported playlist",
        "thumbnail": thumbnail, "track_count": len(uniq), "tracks": uniq,
    }



# ---- Lyrics (Unison + lrc.red) ----------------------------------------------------
import json
from urllib.parse import urlencode
from xml.etree import ElementTree as ET

UNISON_URL = os.getenv("UNISON_URL", "https://unison.boidu.dev").rstrip("/")
LRCRED_URL = os.getenv("LRCRED_URL", "https://lrc.red").rstrip("/")
ISRC_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{3}\d{7}$")
_LYR_UA = {"User-Agent": "Mozilla/5.0 (compatible; Thirai/1.0)"}
_LRC_LINE = re.compile(r"^((?:\[\d+:\d+(?:[.:]\d+)?\])+)(.*)$")
_LRC_TIME = re.compile(r"\[(\d+):(\d+(?:[.:]\d+)?)\]")
_LRC_WORD = re.compile(r"<(\d+):(\d+(?:\.\d+)?)>")
_TTML_SKIP_ROLES = ("x-translation", "x-roman")


def _http_text(url: str, timeout: int = 8) -> Optional[str]:
    try:
        with urlopen(UrlRequest(url, headers=_LYR_UA), timeout=timeout) as r:
            return r.read().decode("utf-8-sig", "replace")
    except Exception:  # 404 / network / timeout all mean "no lyrics from this source"
        return None


def _clock(s) -> Optional[float]:
    """TTML clock value ('1:02.5', '00:01:02.500', '62.5s', '1200ms') -> seconds."""
    if not s:
        return None
    s = s.strip()
    try:
        if s.endswith("ms"):
            return float(s[:-2]) / 1000
        if s.endswith("s"):
            return float(s[:-1])
        sec = 0.0
        for part in s.split(":"):
            sec = sec * 60 + float(part)
        return sec
    except ValueError:
        return None


def _local(tag) -> str:
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def _collect_words(el, words: list):
    for sp in el:
        tail = sp.tail
        if tail:
            tail = " " if tail.isspace() else tail
        if _local(sp.tag) == "span":
            role = next((v for k, v in sp.attrib.items() if k.endswith("role")), "")
            if role not in _TTML_SKIP_ROLES:
                b = _clock(sp.get("begin"))
                if b is not None:
                    words.append({"t": b, "e": _clock(sp.get("end")), "w": "".join(sp.itertext())})
                elif len(sp):
                    _collect_words(sp, words)
                elif sp.text and words:
                    words[-1]["w"] += sp.text
        if tail and words:
            words[-1]["w"] += tail


def parse_ttml(text: str) -> list:
    try:
        root = ET.fromstring(text.lstrip("\ufeff").encode("utf-8"))
    except ET.ParseError:
        return []
    lines = []
    for p in root.iter():
        if _local(p.tag) != "p":
            continue
        words: list = []
        _collect_words(p, words)
        start, end = _clock(p.get("begin")), _clock(p.get("end"))
        if words:
            words[-1]["w"] = words[-1]["w"].rstrip()
            text_ = "".join(w["w"] for w in words).strip()
            start = start if start is not None else words[0]["t"]
            end = end if end is not None else words[-1]["e"]
        else:
            text_ = " ".join("".join(p.itertext()).split())
        if start is None:
            continue
        line = {"t": round(start, 3), "e": round(end, 3) if end is not None else None, "text": text_}
        if words:
            line["words"] = [
                {"t": round(w["t"], 3), "w": w["w"]} for w in words if w["w"].strip()
            ]
        lines.append(line)
    lines.sort(key=lambda l: l["t"])
    return lines


def _lrc_secs(mm: str, ss: str) -> float:
    return int(mm) * 60 + float(ss.replace(":", "."))


def parse_lrc(text: str) -> list:
    lines, offset = [], 0.0
    for raw in text.lstrip("\ufeff").splitlines():
        raw = raw.strip()
        m = re.match(r"^\[offset:\s*(-?\d+)\]", raw, re.I)
        if m:
            offset = int(m.group(1)) / 1000
            continue
        m = _LRC_LINE.match(raw)
        if not m:
            continue
        times = [_lrc_secs(a, b) for a, b in _LRC_TIME.findall(m.group(1))]
        rest = m.group(2)
        lead, words = "", []
        first = _LRC_WORD.search(rest)
        if first:  # enhanced LRC: <mm:ss.xx>word
            lead, pos, cur = rest[:first.start()], 0, None
            for wm in _LRC_WORD.finditer(rest):
                if cur is not None and rest[pos:wm.start()]:
                    words.append({"t": cur, "w": rest[pos:wm.start()]})
                cur, pos = _lrc_secs(wm.group(1), wm.group(2)), wm.end()
            if cur is not None and rest[pos:]:
                words.append({"t": cur, "w": rest[pos:]})
            rest = lead + "".join(w["w"] for w in words)
        txt = rest.strip()
        for t in times:
            line = {"t": round(t - offset, 3), "e": None, "text": txt}
            if words:
                shift = t - times[0]  # repeated timestamps (chorus) shift the word times too
                ws = [{"t": round(w["t"] + shift - offset, 3), "w": w["w"]} for w in words]
                if lead.strip():
                    ws.insert(0, {"t": round(t - offset, 3), "w": lead})
                line["words"] = ws
            lines.append(line)
    lines.sort(key=lambda l: l["t"])
    return lines


def parse_lyrics(text: str):
    """Sniff the real format (don't trust the label) -> (lines, syncType)."""
    text = (text or "").strip()
    if not text:
        return [], "plain"
    lines: list = []
    if text.startswith("<") and "<tt" in text[:3000]:
        lines = parse_ttml(text)
    elif re.search(r"^\s*\[\d+:\d+", text, re.M):
        lines = parse_lrc(text)
    if lines:
        for i, l in enumerate(lines):          # fill missing end times with the next line's start
            if l["e"] is None and i + 1 < len(lines):
                l["e"] = lines[i + 1]["t"]
        return lines, ("richsync" if any(l.get("words") for l in lines) else "linesync")
    plain = [{"t": None, "e": None, "text": ln.strip()} for ln in text.splitlines()]
    while plain and not plain[-1]["text"]:
        plain.pop()
    return plain, "plain"


def _lyrics_result(source: str, text: str, meta: dict) -> Optional[dict]:
    lines, sync = parse_lyrics(text)
    if not any(l["text"] for l in lines):
        return None
    return {"found": True, "source": source, "syncType": sync, "synced": sync != "plain", "lines": lines, "meta": meta}


def _fetch_unison(video_id: str, title: str, artist: str) -> Optional[dict]:
    def get(**params):
        raw = _http_text(f"{UNISON_URL}/lyrics?{urlencode({k: v for k, v in params.items() if v})}")
        if not raw:
            return None
        try:
            body = json.loads(raw)
        except ValueError:
            return None
        d = body.get("data") if isinstance(body, dict) and body.get("success") else None
        return d if isinstance(d, dict) and d.get("lyrics") else None

    d = get(v=video_id)
    if not d and title and artist:
        for a in dict.fromkeys([artist, artist.split(",")[0].strip()]):
            d = get(song=title, artist=a)
            if d:
                break
    if not d:
        return None
    meta = {k: d.get(k) for k in ("id", "song", "artist", "language", "confidence", "voteCount")}
    return _lyrics_result("unison", d["lyrics"], meta)


def _fetch_lrcred(isrc: str) -> Optional[dict]:
    """lrc.red is keyed by ISRC: /s/<ISRC>.ttml (word-synced) and /s/<ISRC>.lrc."""
    for ext in ("ttml", "lrc"):
        raw = _http_text(f"{LRCRED_URL}/s/{isrc}.{ext}")
        if not raw or raw.lstrip().lower().startswith(("<!doctype", "<html")):
            continue  # SPA shell / error page, not lyrics
        res = _lyrics_result("lrcred", raw, {"isrc": isrc})
        if res:
            return res
    return None


@ttl_cache(ttl=3600, maxsize=500)
def _get_lyrics(video_id: str, title: str, artist: str, isrc: str, source: str) -> dict:
    order = []
    if isrc and source in ("auto", "lrcred"):
        order.append("lrcred")
    if source in ("auto", "unison"):
        order.append("unison")
    fallback = None
    for name in order:
        res = _fetch_lrcred(isrc) if name == "lrcred" else _fetch_unison(video_id, title, artist)
        if not res:
            continue
        if res["synced"]:
            return res
        fallback = fallback or res          # keep unsynced text, but keep looking for a synced version
    if fallback:
        return fallback
    note = "lrc.red looks tracks up by ISRC. Enter this track's ISRC to use it." if source == "lrcred" and not isrc else None
    return {"found": False, "source": None, "syncType": None, "synced": False, "lines": [], "note": note}


@app.get("/lyrics/{video_id}", dependencies=[Depends(require_key)])
def lyrics(
    video_id: str,
    title: Optional[str] = Query(None, max_length=200),
    artist: Optional[str] = Query(None, max_length=200),
    isrc: Optional[str] = Query(None, max_length=20, description="Needed for lrc.red lookups"),
    source: str = Query("auto", description="auto, unison or lrcred"),
):
    """Synced lyrics as timed lines. Unison is queried by video id, lrc.red by ISRC."""
    check_video_id(video_id)
    if source not in ("auto", "unison", "lrcred"):
        raise HTTPException(400, "source must be auto, unison or lrcred")
    isrc = re.sub(r"[\s-]", "", isrc or "").upper()
    if isrc and not ISRC_RE.match(isrc):
        raise HTTPException(400, "That doesn't look like an ISRC (2 letters, 3 letters/digits, 7 digits).")
    return _get_lyrics(video_id, (title or "").strip(), (artist or "").strip(), isrc, source)


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



# ---- Charts (per country) ----------------------------------------------------------
CHART_COUNTRIES = {
    "ZZ": "Global", "US": "United States", "GB": "United Kingdom", "IN": "India", "CA": "Canada",
    "AU": "Australia", "NZ": "New Zealand", "IE": "Ireland", "DE": "Germany", "FR": "France",
    "ES": "Spain", "IT": "Italy", "NL": "Netherlands", "SE": "Sweden", "NO": "Norway",
    "DK": "Denmark", "FI": "Finland", "PL": "Poland", "TR": "Turkey", "BR": "Brazil",
    "MX": "Mexico", "AR": "Argentina", "CO": "Colombia", "CL": "Chile", "JP": "Japan",
    "KR": "South Korea", "ID": "Indonesia", "TH": "Thailand", "VN": "Vietnam", "PH": "Philippines",
    "MY": "Malaysia", "SG": "Singapore", "ZA": "South Africa", "NG": "Nigeria", "EG": "Egypt",
    "SA": "Saudi Arabia", "AE": "United Arab Emirates",
}
_chart_cache: dict = {}


def _chart_block(ch: dict, *keys):
    """ytmusicapi has changed the shape of get_charts() over time; accept dict or list blocks."""
    for k in keys:
        b = ch.get(k)
        if isinstance(b, dict) and (b.get("items") or b.get("playlist")):
            return b.get("playlist"), (b.get("items") or [])
        if isinstance(b, list) and b:
            return None, b
    return None, []


@app.get("/charts/countries", dependencies=[Depends(require_key)])
def chart_countries():
    return {"countries": [{"code": c, "name": n} for c, n in CHART_COUNTRIES.items()]}


@app.get("/charts/{code}", dependencies=[Depends(require_key)])
def charts(code: str):
    """Chart playlists, ranked top songs and top artists for a country (ZZ = global)."""
    code = code.upper()
    if code not in CHART_COUNTRIES:
        raise HTTPException(400, "Unknown country code")
    hit = _chart_cache.get(code)
    if hit and time.time() - hit[0] < 3600:
        return hit[1]
    if YTMusic is None:
        raise HTTPException(501, "Charts need ytmusicapi: pip install ytmusicapi")
    name = CHART_COUNTRIES[code]

    def official():
        try:
            return ytm().get_charts(code) or {}
        except Exception:
            return {}

    def searched():
        out = []
        for q in (f"Top 100 {name}", f"{name} top songs chart"):
            try:
                for h in ytm().search(q, filter="featured_playlists", limit=6, ignore_spelling=True):
                    e = _entity(h, "playlist")
                    if e:
                        out.append(e)
            except Exception:
                continue
        return out

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1, f2 = pool.submit(official), pool.submit(searched)
        ch, found = f1.result(), f2.result()

    pid, items = _chart_block(ch, "songs", "videos")
    tracks = [t for t in map(_track, items) if t]
    if pid and not tracks:
        try:
            _, tracks = _playlist_tracks(pid, 50)
        except Exception:
            pass

    playlists, seen = [], set()
    if pid:
        seen.add(pid)
        playlists.append({
            "kind": "playlist", "id": pid, "title": f"Top songs · {name}", "artist": "YouTube Charts",
            "thumbnail": tracks[0]["thumbnail"] if tracks else None,
        })
    for e in found:
        if e["id"] not in seen:
            seen.add(e["id"])
            playlists.append(e)
    if not tracks and playlists:
        try:
            _, tracks = _playlist_tracks(playlists[0]["id"], 50)
        except Exception:
            pass

    _, aitems = _chart_block(ch, "artists")
    artists = [e for e in (_entity(x, "artist") for x in aitems if isinstance(x, dict)) if e]
    data = {"code": code, "name": name, "tracks": tracks[:50], "playlists": playlists[:12], "artists": artists[:20]}
    if tracks or playlists:
        _chart_cache[code] = (time.time(), data)
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
