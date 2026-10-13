"""GIF subcommands — search Discord's GIF picker and keep a local learned-GIF library.

Search and trending call the same authenticated endpoints as the Discord
client's GIF picker (currently backed by Klipy).  The picker sends a result's
``url`` (e.g. ``https://klipy.com/gifs/<slug>``) as plain message content and
Discord unfurls it into an animated ``gifv`` embed, so that is the URL we print.

The library is a plain JSON file shared by all accounts.  ``learn`` scans a
channel for GIFs other people posted and prints them with surrounding messages
so an agent can infer their meaning and ``save`` them with tags.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import re
import sys
import time
import urllib.parse

from src import api
from src.accounts import CONFIG_ROOT, _write_secure_json
from src.parse import parse_message
from src.resolve import parse_message_link, resolve_channel, resolve_guild


LIBRARY_FILE = CONFIG_ROOT / "gifs.json"

# Mirrors the client's picker requests: GET /gifs/search?q&media_format&locale&limit.
# Discord rejects limit < 20 and returns at most 100 results.
_LOCALE = "en-US"
_MEDIA_FORMAT = "mp4"
_API_MIN_LIMIT = 20
MAX_RESULTS = 100

# Page URLs Discord unfurls into animated gifv embeds when sent as message content.
_GIF_PAGE_RE = re.compile(
    r"^https?://(?:www\.)?(?:klipy\.com/gifs/|tenor\.com/(?:[a-z-]+/)?view/|giphy\.com/gifs/)",
    re.IGNORECASE,
)
_URL_RE = re.compile(r"https?://[^\s<>|]+")


# ─── Discord GIF picker ─────────────────────────────────────────────────────

def _api_limit(n):
    return str(min(max(n, _API_MIN_LIMIT), MAX_RESULTS))


def _parse_result(item):
    return {
        "id": str(item.get("id") or ""),
        "title": str(item.get("title") or "").strip(),
        "url": str(item.get("url") or ""),
        "media_url": str(item.get("src") or ""),
        "width": item.get("width"),
        "height": item.get("height"),
    }


def search_gifs(query, limit=10):
    data = api.get(
        "/gifs/search",
        params={"q": query, "media_format": _MEDIA_FORMAT, "locale": _LOCALE, "limit": _api_limit(limit)},
    )
    return [_parse_result(item) for item in (data or [])[:limit] if item.get("url")]


def trending_gifs(limit=10):
    data = api.get(
        "/gifs/trending-gifs",
        params={"media_format": _MEDIA_FORMAT, "locale": _LOCALE, "limit": _api_limit(limit)},
    )
    return [_parse_result(item) for item in (data or [])[:limit] if item.get("url")]


def _format_results(results):
    if not results:
        return "  No GIFs found."
    lines = []
    for i, gif in enumerate(results, start=1):
        lines.append(f"  {i:>2}. {gif['title'] or gif['id']}")
        lines.append(f"      {gif['url']}")
    return "\n".join(lines)


def _count(value):
    n = int(value)
    if not 1 <= n <= MAX_RESULTS:
        raise argparse.ArgumentTypeError(f"must be between 1 and {MAX_RESULTS}")
    return n


def search(argv):
    p = argparse.ArgumentParser(
        prog="discord gifs search",
        description="Search GIFs like Discord's GIF picker. Send a result's URL as message text to post it.",
    )
    p.add_argument("query", nargs="+", help="Search terms")
    p.add_argument("-n", "--limit", type=_count, default=10, help=f"Number of results (default: 10, max: {MAX_RESULTS})")
    p.add_argument("--json", action="store_true", help="Print JSON")
    args = p.parse_args(argv)
    results = search_gifs(" ".join(args.query), args.limit)
    print(json.dumps(results, indent=2) if args.json else _format_results(results))


def trending(argv):
    p = argparse.ArgumentParser(prog="discord gifs trending", description="Show trending GIFs from Discord's GIF picker.")
    p.add_argument("-n", "--limit", type=_count, default=10, help=f"Number of results (default: 10, max: {MAX_RESULTS})")
    p.add_argument("--json", action="store_true", help="Print JSON")
    args = p.parse_args(argv)
    results = trending_gifs(args.limit)
    print(json.dumps(results, indent=2) if args.json else _format_results(results))


# ─── Library ────────────────────────────────────────────────────────────────

def _empty_library():
    return {"version": 1, "gifs": []}


def load_library():
    if not LIBRARY_FILE.exists():
        return _empty_library()
    try:
        data = json.loads(LIBRARY_FILE.read_text())
    except (OSError, json.JSONDecodeError) as e:
        raise RuntimeError(f"Failed to read GIF library {LIBRARY_FILE}: {e}") from e
    if not isinstance(data, dict) or not isinstance(data.get("gifs"), list):
        raise RuntimeError(f"Invalid GIF library: {LIBRARY_FILE}")
    return data


@contextlib.contextmanager
def _locked_library():
    """Read-modify-write the library under an exclusive lock."""
    LIBRARY_FILE.parent.mkdir(parents=True, exist_ok=True)
    lock_path = LIBRARY_FILE.with_suffix(LIBRARY_FILE.suffix + ".lock")
    with open(lock_path, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        library = load_library()
        yield library
        _write_secure_json(LIBRARY_FILE, library)


def normalize_tags(values):
    tags = []
    for value in values or []:
        for part in str(value).split(","):
            tag = re.sub(r"\s+", " ", part.strip().lstrip("#").lower())
            if tag and tag not in tags:
                tags.append(tag)
    return tags


def _validate_url(parser, url):
    url = url.strip()
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        parser.error(f"not an http(s) URL: {url!r}")
    return url


def _parse_source(parser, value):
    if not value:
        return None
    guild_id, channel_id, message_id = parse_message_link(value.strip())
    if not channel_id:
        parser.error("--source must be CHANNEL_ID/MESSAGE_ID or a Discord message link")
    source = {"channel_id": channel_id, "message_id": message_id}
    if guild_id:
        source["guild_id"] = guild_id
    return source


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def save(argv):
    p = argparse.ArgumentParser(
        prog="discord gifs save",
        description="Save (or update) a GIF URL in the local library with tags describing when to use it.",
        epilog="Saving an existing URL merges tags and replaces --title/--note/--source when given.",
    )
    p.add_argument("url", help="GIF URL as it should be sent (e.g. https://klipy.com/gifs/...)")
    p.add_argument("-t", "--tags", action="append", required=True, metavar="A,B",
                   help="Comma-separated tags: meaning, mood, when to use it (repeatable)")
    p.add_argument("--title", help="Short description of what the GIF shows")
    p.add_argument("--note", help="Context: what it meant where it was seen, when to reuse it")
    p.add_argument("--source", metavar="CHANNEL/MESSAGE", help="Message where it was seen (IDs or message link)")
    args = p.parse_args(argv)

    url = _validate_url(p, args.url)
    tags = normalize_tags(args.tags)
    if not tags:
        p.error("at least one non-empty tag is required")
    source = _parse_source(p, args.source)
    if not _GIF_PAGE_RE.match(url) and not urllib.parse.urlparse(url).path.lower().endswith(".gif"):
        print(f"Warning: {url} is not a known GIF page or .gif URL; Discord may not animate it.", file=sys.stderr)

    with _locked_library() as library:
        entry = next((g for g in library["gifs"] if g.get("url") == url), None)
        if entry is None:
            entry = {"url": url, "tags": [], "added_at": _now()}
            library["gifs"].append(entry)
            verb = "Saved"
        else:
            verb = "Updated"
        entry["tags"] = normalize_tags(entry.get("tags", []) + tags)
        if args.title is not None:
            entry["title"] = args.title.strip()
        if args.note is not None:
            entry["note"] = args.note.strip()
        if source is not None:
            entry["source"] = source
        entry["updated_at"] = _now()
    print(f"{verb} {url}  [{', '.join(entry['tags'])}]")


def remove(argv):
    p = argparse.ArgumentParser(prog="discord gifs remove", description="Remove a GIF from the local library.")
    p.add_argument("url", help="Saved GIF URL")
    args = p.parse_args(argv)
    url = args.url.strip()
    with _locked_library() as library:
        before = len(library["gifs"])
        library["gifs"] = [g for g in library["gifs"] if g.get("url") != url]
        removed = before - len(library["gifs"])
    if not removed:
        raise RuntimeError(f"{url} is not in the GIF library")
    print(f"Removed {url}")


def _terms(values):
    return [t for t in (re.sub(r"\s+", " ", v.strip().lower()) for v in ",".join(values).split(",")) if t]


def _score(entry, term):
    tags = [t.lower() for t in entry.get("tags", [])]
    if term in tags:
        return 3
    if any(term in tag for tag in tags):
        return 2
    text = f"{entry.get('title', '')} {entry.get('note', '')}".lower()
    return 1 if term in text else 0


def find_gifs(library, query_terms):
    """Rank saved GIFs by how many terms they match, then by match strength."""
    ranked = []
    for entry in library["gifs"]:
        scores = [_score(entry, term) for term in query_terms]
        matched = sum(1 for s in scores if s)
        if matched:
            ranked.append(((matched, sum(scores), entry.get("updated_at", "")), entry))
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    return [entry for _, entry in ranked]


def _format_entries(entries):
    if not entries:
        return "  No saved GIFs."
    lines = []
    for entry in entries:
        title = f"  — {entry['title']}" if entry.get("title") else ""
        lines.append(f"  {entry['url']}{title}")
        lines.append(f"      tags: {', '.join(entry.get('tags', []))}")
        if entry.get("note"):
            lines.append(f"      note: {entry['note']}")
    return "\n".join(lines)


def list_cmd(argv):
    p = argparse.ArgumentParser(prog="discord gifs list", description="List saved GIFs, most recently updated first.")
    p.add_argument("-n", "--limit", type=int, help="Show at most N entries")
    p.add_argument("--json", action="store_true", help="Print JSON")
    args = p.parse_args(argv)
    entries = sorted(load_library()["gifs"], key=lambda g: g.get("updated_at", ""), reverse=True)
    if args.limit is not None:
        entries = entries[:args.limit]
    print(json.dumps(entries, indent=2) if args.json else _format_entries(entries))


def find(argv):
    p = argparse.ArgumentParser(
        prog="discord gifs find",
        description="Find saved GIFs by tags or words in title/note. Matches any term; best matches first.",
    )
    p.add_argument("query", nargs="+", help="Tags or words (comma-separated or separate arguments)")
    p.add_argument("-n", "--limit", type=int, default=10, help="Max results (default: 10)")
    p.add_argument("--json", action="store_true", help="Print JSON")
    args = p.parse_args(argv)
    entries = find_gifs(load_library(), _terms(args.query))[:args.limit]
    if args.json:
        print(json.dumps(entries, indent=2))
    elif not entries:
        print("  No saved GIFs match. Try 'discord gifs search' or 'discord gifs tags'.")
    else:
        print(_format_entries(entries))


def tags(argv):
    p = argparse.ArgumentParser(prog="discord gifs tags", description="List tags in the GIF library with counts.")
    p.parse_args(argv)
    counts = {}
    for entry in load_library()["gifs"]:
        for tag in entry.get("tags", []):
            counts[tag] = counts.get(tag, 0) + 1
    if not counts:
        print("  No saved GIFs.")
        return
    for tag, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"  {count:>4}  {tag}")


# ─── Learning from channel history ──────────────────────────────────────────

def gifs_in_message(msg):
    """Return GIFs in a raw message that can be re-sent as plain message content.

    Uploaded .gif attachments are skipped: their CDN URLs are signed and expire.
    """
    found = []
    seen = set()

    def add(url, title="", provider="", rendered=True):
        if url and url not in seen:
            seen.add(url)
            found.append({"url": url, "title": title, "provider": provider, "rendered": rendered})

    for embed in msg.get("embeds") or []:
        url = embed.get("url") or ""
        provider = (embed.get("provider") or {}).get("name") or ""
        if embed.get("type") == "gifv":
            add(url, embed.get("title") or "", provider)
        elif embed.get("type") == "image" and urllib.parse.urlparse(url).path.lower().endswith(".gif"):
            add(url, embed.get("title") or "", provider)
    if found:
        return found
    for url in _URL_RE.findall(msg.get("content") or ""):
        url = url.rstrip(".,;:!?)>]*`")
        if _GIF_PAGE_RE.match(url) or urllib.parse.urlparse(url).path.lower().endswith(".gif"):
            add(url, rendered=False)  # no gifv/image embed: dead link or not yet unfurled
    return found


def _fetch_history(channel_id, count, before=None):
    messages = []
    while len(messages) < count:
        page = api.get_messages(channel_id, limit=min(100, count - len(messages)), before=before)
        if not page:
            break
        messages.extend(page)
        before = page[-1]["id"]
        if len(page) < 100:
            break
    messages.reverse()  # chronological
    return messages


def find_learnable(messages, context=3):
    """Pair each GIF in chronological ``messages`` with its surrounding messages."""
    items = []
    for i, msg in enumerate(messages):
        for gif in gifs_in_message(msg):
            items.append({
                **gif,
                "message_id": msg["id"],
                "channel_id": msg.get("channel_id", ""),
                "index": i,
                "before": messages[max(0, i - context):i],
                "after": messages[i + 1:i + 1 + context],
            })
    return items


def _line(msg, marker="  "):
    parsed = parse_message(msg)
    text = parsed["content"].replace("\n", " ")
    if len(text) > 200:
        text = text[:200] + "…"
    if not text and parsed["attachments"]:
        text = "📎 " + ", ".join(a["filename"] for a in parsed["attachments"])
    reply = ""
    if parsed["reply_to"]:
        reply = f"(↩ {parsed['reply_to']['author']['display_name']}) "
    return f"    {marker}{parsed['author']['display_name']}  {parsed['timestamp_fmt']}: {reply}{text}"


def _compact(msg):
    parsed = parse_message(msg)
    return {
        "id": parsed["id"],
        "author": parsed["author"]["display_name"],
        "timestamp": parsed["timestamp"],
        "content": parsed["content"],
    }


def learn(argv):
    p = argparse.ArgumentParser(
        prog="discord gifs learn",
        description=(
            "Scan recent channel history for GIFs and print each with surrounding messages, "
            "so you can infer what it means and save it with 'discord gifs save'."
        ),
        epilog="Read-only: messages are not acknowledged or modified.",
    )
    p.add_argument("channel", help="Channel name or ID")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server name or ID (required if using channel name)")
    p.add_argument("-n", "--limit", type=int, default=100, help="Messages to scan (default: 100, max: 1000)")
    p.add_argument("-C", "--context", type=int, default=3, help="Messages of context before/after each GIF (default: 3)")
    p.add_argument("--before", help="Scan messages before this message ID")
    p.add_argument("--all", action="store_true", help="Include GIFs already in the library")
    p.add_argument("--json", action="store_true", help="Print JSON")
    args = p.parse_args(argv)
    if not 1 <= args.limit <= 1000:
        p.error("--limit must be between 1 and 1000")
    if args.context < 0:
        p.error("--context must be >= 0")

    guild_id = resolve_guild(args.guild)["id"] if args.guild else None
    ch = resolve_channel(args.channel, guild_id)
    messages = _fetch_history(ch["id"], args.limit, before=args.before)
    saved = {g.get("url"): g for g in load_library()["gifs"]}
    items = find_learnable(messages, context=args.context)
    if not args.all:
        items = [item for item in items if item["url"] not in saved]

    if args.json:
        print(json.dumps([
            {
                **{k: v for k, v in item.items() if k not in {"before", "after", "index"}},
                "channel_id": item["channel_id"] or ch["id"],
                "saved_tags": saved.get(item["url"], {}).get("tags"),
                "before": [_compact(m) for m in item["before"]],
                "message": _compact(messages[item["index"]]),
                "after": [_compact(m) for m in item["after"]],
            }
            for item in items
        ], indent=2))
        return

    if not items:
        print(f"  No {'' if args.all else 'new '}GIFs in the last {len(messages)} message(s).")
        return
    blocks = []
    for n, item in enumerate(items, start=1):
        label = f" — {item['title']}" if item["title"] else ""
        provider = f" ({item['provider']})" if item["provider"] else ""
        status = "" if item["rendered"] else "  [no embed: may be dead]"
        lines = [f"  GIF {n}/{len(items)}  {item['url']}{label}{provider}{status}"]
        if item["url"] in saved:
            lines.append(f"    saved tags: {', '.join(saved[item['url']].get('tags', []))}")
        lines.append(f"    source: {item['channel_id'] or ch['id']}/{item['message_id']}")
        lines.extend(_line(m) for m in item["before"])
        lines.append(_line(messages[item["index"]], "▶ "))
        lines.extend(_line(m) for m in item["after"])
        blocks.append("\n".join(lines))
    print("\n\n".join(blocks))
    print(
        "\nSave one with: discord gifs save URL --tags meaning,mood --title '...' "
        "--note 'when to use it' --source CHANNEL/MESSAGE"
    )


# ─── Dispatch ───────────────────────────────────────────────────────────────

USAGE = """usage: discord gifs <subcommand> [args]

Discord GIF picker (Klipy-backed; read-only):
  search QUERY [-n N] [--json]     Find GIFs; send a result URL as message text to post it
  trending [-n N] [--json]         Trending GIFs

Learned GIF library (local file, shared by all accounts):
  learn CHANNEL [-g SERVER] [-n N] [-C N]
                                   Show GIFs seen in recent history with context to tag
  save URL --tags A,B [--title T] [--note N] [--source CHANNEL/MESSAGE]
                                   Save or update a GIF with tags
  find TERMS... [-n N] [--json]    Find saved GIFs by tag/title/note (best matches first)
  list [-n N] [--json]             List saved GIFs
  tags                             List tags with counts
  remove URL                       Remove a saved GIF

Run 'discord gifs <subcommand> --help' for details."""

_COMMANDS = {
    "search": search,
    "trending": trending,
    "learn": learn,
    "save": save,
    "find": find,
    "list": list_cmd,
    "ls": list_cmd,
    "tags": tags,
    "remove": remove,
    "rm": remove,
}


def dispatch(cmd, argv):
    if not argv or argv[0] in {"-h", "--help", "help"}:
        print(USAGE)
        return
    fn = _COMMANDS.get(argv[0])
    try:
        if fn is None:
            raise RuntimeError(f"unknown subcommand '{argv[0]}'. Run 'discord gifs --help'.")
        fn(argv[1:])
    except RuntimeError as error:
        raise SystemExit(f"discord {cmd} {argv[0]}: {error}") from error
