"""
YouTube Music API powered by yt-dlp + FastAPI.

Endpoints
  GET /search?q=...&type=songs|albums|artists|playlists&limit=20
  GET /album/{browse_id}              Album + tracks
  GET /artist/{channel_id}            Artist: top songs, albums, singles
  GET /home                           Tamil / India home feed (songs, playlists, albums, artists)
  GET /track/{video_id}               Track metadata
  GET /stream/{video_id}              Direct audio URL (expires after a few hours)
  GET /download/{video_id}?fmt=mp3    Download audio file (needs ffmpeg for conversion)
  GET /playlist/{playlist_id}         Playlist / album track list

Run:  uvicorn main:app --reload
Docs: http://127.0.0.1:8000/docs
"""

import io
import os
import re
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from urllib.request import Request, urlopen
from typing import Optional
from urllib.parse import quote

import yt_dlp

try:
    from PIL import Image, ImageChops
except ImportError:  # covers still work, just without auto-cropping
    Image = ImageChops = None
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from starlette.background import BackgroundTask

API_KEY = os.getenv("API_KEY")  # optional: set to require X-API-Key header
MUSIC_URL = "https://music.youtube.com"
VIDEO_ID_RE = re.compile(r"^[\w-]{11}$")
PLAYLIST_ID_RE = re.compile(r"^[\w-]{10,64}$")
MUSIC_SONGS_FILTER = "EgWKAQIIAWoMEA4QChADEAQQCRAF"  # YT Music "Songs" filter
ALLOWED_FORMATS = {"mp3", "m4a", "opus", "flac", "wav", "best"}

app = FastAPI(title="YouTube Music API", version="1.0.0")

# Lets index.html work even when opened straight from disk (file://).
# Tighten allow_origins if you expose this beyond localhost.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)


@app.get("/", include_in_schema=False)
def gui():
    return FileResponse(os.path.join(os.path.dirname(__file__), "index.html"))


def require_key(x_api_key: Optional[str] = Header(default=None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "Invalid or missing X-API-Key")


def base_opts(**extra) -> dict:
    # Use iOS/Mobile player clients by default to bypass YouTube bot detection
    clients = os.getenv("YTDLP_PLAYER_CLIENT", "ios,mweb,android").split(",")
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "extractor_args": {
            "youtube": {
                "player_client": clients,
            }
        },
    }
    # Optional: export YTDLP_COOKIES=/path/to/cookies.txt
    if os.getenv("YTDLP_COOKIES"):
        opts["cookiefile"] = os.getenv("YTDLP_COOKIES")
    # Optional: export YTDLP_COOKIES_FROM_BROWSER=chrome or firefox
    if os.getenv("YTDLP_COOKIES_FROM_BROWSER"):
        browser_spec = os.getenv("YTDLP_COOKIES_FROM_BROWSER").split(":")
        opts["cookiesfrombrowser"] = tuple(browser_spec)

    opts.update(extra)
    return opts


def check_video_id(video_id: str) -> str:
    if not VIDEO_ID_RE.match(video_id):
        raise HTTPException(400, "Invalid video id")
    return video_id


def extract(url: str, opts: dict, download: bool = False) -> dict:
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=download)
    except yt_dlp.utils.DownloadError as e:
        raise HTTPException(502, f"yt-dlp error: {re.sub(r'\x1b\[[0-9;]*m', '', str(e))}")


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
    """Search YouTube Music for songs (audio tracks, not videos), albums, artists or playlists."""
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
            t["thumbnail"] = thumb or t["thumbnail"]  # every track shows the album art
            t["artists"] = t["artists"] or arts
            t["album_id"] = browse_id
            tracks.append(t)
    more = []
    if arts and arts[0].get("id"):  # other releases by the same artist
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
def artist(channel_id: str):
    check_id(channel_id)
    a = ytm_call("get_artist", channel_id)
    block = a.get("songs") or {}
    raw = block.get("results") or []
    bid = block.get("browseId")
    if bid:  # the artist's full "top songs" list instead of just the first few
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
    info = extract(f"{MUSIC_URL}/watch?v={video_id}", base_opts())
    return {
        **track_summary(info),
        "release_year": info.get("release_year"),
        "genre": info.get("genre"),
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "description": info.get("description"),
    }


@app.get("/stream/{video_id}", dependencies=[Depends(require_key)])
def stream(video_id: str):
    """Best direct audio stream URL (temporary; tied to the requesting IP)."""
    check_video_id(video_id)
    info = extract(
        f"{MUSIC_URL}/watch?v={video_id}", base_opts(format="bestaudio/best")
    )
    return {
        **track_summary(info),
        "audio_url": info.get("url"),
        "ext": info.get("ext"),
        "abr": info.get("abr"),
        "acodec": info.get("acodec"),
        "filesize": info.get("filesize") or info.get("filesize_approx"),
    }


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
        format="bestaudio/best",
        outtmpl=os.path.join(tmp, "%(artist,uploader)s - %(title)s.%(ext)s"),
        restrictfilenames=True,
        writethumbnail=False,
    )
    if fmt != "best":
        if not shutil.which("ffmpeg"):
            _cleanup(tmp)
            raise HTTPException(500, "ffmpeg not found on server; use fmt=best or install ffmpeg")
        opts["postprocessors"] = [
            {"key": "FFmpegExtractAudio", "preferredcodec": fmt, "preferredquality": quality},
            {"key": "FFmpegMetadata", "add_metadata": True},
        ]

    try:
        extract(f"{MUSIC_URL}/watch?v={video_id}", opts, download=True)
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
def playlist(playlist_id: str, limit: int = Query(100, ge=1, le=500)):
    """Tracks in a YouTube Music playlist (incl. official RDCLAK5uy_... ones) or album playlist."""
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
            pass  # fall through to yt-dlp
    info = extract(
        f"{MUSIC_URL}/playlist?list={playlist_id}",
        base_opts(extract_flat=True, noplaylist=False, playlistend=limit),
    )
    tracks = [track_summary(e) for e in info.get("entries", []) if e]
    return {
        "kind": "playlist", "id": info.get("id"), "title": info.get("title"),
        "subtitle": f"{len(tracks)} songs", "thumbnail": None,
        "track_count": len(tracks), "tracks": tracks,
    }


# ---- Cover art (auto-cropped to a square) ------------------------------------
@lru_cache(maxsize=128)
def _fetch_thumb(vid: str):
    for name in ("maxresdefault", "sddefault", "hqdefault", "mqdefault"):
        try:
            req = Request(f"https://i.ytimg.com/vi/{vid}/{name}.jpg", headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=10) as r:
                data = r.read()
            if len(data) > 2000:
                return data
        except Exception:
            continue
    return None


def _trim_borders(img, tol: int = 28):
    """Strip uniform-colour bars (letterbox / side bars), a few passes since
    the top/bottom and left/right bars are often different colours."""
    start = img.size
    for _ in range(3):
        w, h = img.size
        bg = Image.new("RGB", img.size, img.getpixel((2, 2)))
        diff = ImageChops.difference(img, bg).convert("L").point(lambda p: 255 if p > tol else 0)
        box = diff.getbbox()
        if not box or box == (0, 0, w, h):
            break
        l, t, r, b = box
        if (r - l) < w * 0.3 or (b - t) < h * 0.3:  # looks like we'd eat the artwork; bail
            break
        img = img.crop(box)
    if img.size != start:  # shave a couple of px to drop JPEG fringing at the edge
        w, h = img.size
        if w > 8 and h > 8:
            img = img.crop((2, 2, w - 2, h - 2))
    return img


@lru_cache(maxsize=1024)
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
    """Square, border-free cover art. Public (no API key) so <img> tags can load it."""
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


def _load_section(sec):  # yt-dlp fallback loader
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
        note = "Install ytmusicapi (pip install ytmusicapi) for the full Tamil home feed."
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
    """Tamil-leaning rows for a mood chip (Relax, Party, Romance, ...)."""
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


@app.get("/health")
def health():
    return {"status": "ok", "yt_dlp": yt_dlp.version.__version__, "ffmpeg": bool(shutil.which("ffmpeg"))}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
