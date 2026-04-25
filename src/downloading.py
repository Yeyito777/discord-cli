"""Download subcommand — fetch attachments from a Discord message."""

import argparse
import os
import re
import shutil
import tempfile
import urllib.error
import urllib.request

from pathlib import Path

from src import api
from src.resolve import resolve_guild, resolve_channel


DEFAULT_OUT_DIR = Path(__file__).resolve().parents[3] / "config" / "storage" / "discord"


_CDN_HEADERS = {
    # A normal browser UA plus Discord referer avoids a surprising number of CDN 403s.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://discord.com/",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
}


def _truncate_filename(name, max_bytes=240):
    """Trim a filename to a conservative byte length, preserving the extension."""
    if len(name.encode("utf-8", errors="ignore")) <= max_bytes:
        return name

    root, ext = os.path.splitext(name)
    ext_bytes = ext.encode("utf-8", errors="ignore")[:40]
    ext = ext_bytes.decode("utf-8", errors="ignore")
    max_root_bytes = max(1, max_bytes - len(ext.encode("utf-8", errors="ignore")))
    root = root.encode("utf-8", errors="ignore")[:max_root_bytes].decode("utf-8", errors="ignore")
    return (root.rstrip(" .") or "attachment") + ext


def _safe_filename(name, fallback):
    """Return a path-safe filename while preserving readable names when possible."""
    name = os.path.basename(name or "")
    name = name.replace("\x00", "")
    name = re.sub(r"[\x00-\x1f\x7f]", "_", name)
    name = re.sub(r"[\\/]+", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"[^\w .()@+=,\[\]{}-]", "_", name, flags=re.UNICODE)
    name = name.strip(" .")

    if not name or name in {".", ".."}:
        name = fallback
    if name.startswith("-") or name.startswith("."):
        name = "_" + name

    # Leave room for collision suffixes on common filesystems.
    return _truncate_filename(name)


def _unique_path(out_dir, filename):
    path = os.path.join(out_dir, filename)
    if not os.path.exists(path):
        return path

    root, ext = os.path.splitext(filename)
    for i in range(1, 10000):
        suffix = f" ({i})"
        max_bytes = 240 - len((suffix + ext).encode("utf-8", errors="ignore"))
        safe_root = root.encode("utf-8", errors="ignore")[:max(1, max_bytes)]
        safe_root = safe_root.decode("utf-8", errors="ignore").rstrip(" .")
        candidate_name = _truncate_filename((safe_root or "attachment") + suffix + ext)
        candidate = os.path.join(out_dir, candidate_name)
        if not os.path.exists(candidate):
            return candidate
    raise RuntimeError(f"Could not choose a unique filename for {filename!r}")


def _attachment_urls(attachment):
    urls = []
    for key in ("url", "proxy_url"):
        url = attachment.get(key)
        if url and url not in urls:
            urls.append(url)
    return urls


def _download_url(url, dest_path):
    req = urllib.request.Request(url, headers=_CDN_HEADERS)
    with urllib.request.urlopen(req, timeout=60) as resp:
        fd, tmp_path = tempfile.mkstemp(
            prefix=".discord-download-",
            suffix=".part",
            dir=os.path.dirname(dest_path) or ".",
        )
        try:
            with os.fdopen(fd, "wb") as f:
                shutil.copyfileobj(resp, f)
            os.replace(tmp_path, dest_path)
            try:
                os.chmod(dest_path, 0o644)
            except OSError:
                pass
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


def _download_attachment(attachment, out_dir, ordinal):
    attachment_id = attachment.get("id") or str(ordinal)
    fallback = f"attachment-{attachment_id}"
    filename = _safe_filename(attachment.get("filename"), fallback)
    dest_path = _unique_path(out_dir, filename)

    urls = _attachment_urls(attachment)
    if not urls:
        raise RuntimeError(f"Attachment {ordinal} has no url or proxy_url")

    errors = []
    for url in urls:
        try:
            _download_url(url, dest_path)
            return dest_path
        except urllib.error.HTTPError as e:
            errors.append(f"{url}: HTTP {e.code} {e.reason}")
        except urllib.error.URLError as e:
            errors.append(f"{url}: {e.reason}")
        except Exception as e:
            errors.append(f"{url}: {e}")

    raise RuntimeError(
        f"Failed to download attachment {ordinal} ({attachment.get('filename', attachment_id)}): "
        + "; ".join(errors)
    )


def _select_attachments(parser, attachments, index_text):
    if index_text is None:
        return list(enumerate(attachments, start=1))

    try:
        index = int(index_text)
    except ValueError:
        parser.error("attachment-index must be a 1-based integer")

    if index < 1 or index > len(attachments):
        parser.error(f"attachment-index must be between 1 and {len(attachments)}")

    return [(index, attachments[index - 1])]


def _fetch_message(channel_id, message_id):
    """Fetch a message using endpoints available to user-token clients.

    Discord's single-message endpoint currently returns "Only bots can use this
    endpoint" for user tokens, but the normal channel history endpoint supports
    an `around` query and returns the target message when it is visible.
    """
    direct_error = None
    try:
        return api.get_message(channel_id, message_id)
    except Exception as e:
        direct_error = e

    try:
        msgs = api.get_messages(channel_id, limit=1, around=message_id) or []
    except Exception:
        msgs = []
    for msg in msgs:
        if msg.get("id") == message_id:
            return msg

    # Some Discord history responses around an exact message can be sparse near
    # edges. Try a slightly wider around window before giving up.
    try:
        msgs = api.get_messages(channel_id, limit=10, around=message_id) or []
    except Exception as e:
        raise RuntimeError(f"Could not fetch message {message_id}: {e}; direct endpoint also failed: {direct_error}")
    for msg in msgs:
        if msg.get("id") == message_id:
            return msg

    raise RuntimeError(f"Message {message_id} was not found in channel {channel_id}; direct endpoint failed: {direct_error}")


def download(argv):
    p = argparse.ArgumentParser(
        prog="discord download",
        description="Download attachment(s) from a Discord message.",
        epilog=(
            "examples:\n"
            "  discord download general 123456789012345678 -g 'My Server'\n"
            "  discord download 111111111111111111 123456789012345678 1\n"
            "  discord download general 123456789012345678 --out /tmp/discord -g 'My Server'\n\n"
            "attachment-index is 1-based. If omitted, all attachments are downloaded.\n"
            f"Default output directory: {DEFAULT_OUT_DIR}"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("channel", help="Channel name or ID")
    p.add_argument("message", help="Discord message ID")
    p.add_argument("attachment_index", nargs="?", metavar="attachment-index", help="1-based attachment index to download")
    p.add_argument("-g", "--guild", "--server", dest="guild", help="Server name or ID (required if using channel name)")
    p.add_argument("-o", "--out", default=str(DEFAULT_OUT_DIR), metavar="DIR", help=f"Output directory (default: {DEFAULT_OUT_DIR})")
    args = p.parse_args(argv)

    guild_id = None
    if args.guild:
        g = resolve_guild(args.guild)
        guild_id = g["id"]
    ch = resolve_channel(args.channel, guild_id)

    out_dir = os.path.abspath(args.out)
    if os.path.exists(out_dir) and not os.path.isdir(out_dir):
        raise RuntimeError(f"Output path is not a directory: {out_dir}")
    os.makedirs(out_dir, exist_ok=True)

    msg = _fetch_message(ch["id"], args.message)
    attachments = msg.get("attachments") or []
    if not attachments:
        raise RuntimeError(f"Message {args.message} has no attachments")

    selected = _select_attachments(p, attachments, args.attachment_index)
    written = []
    for ordinal, attachment in selected:
        path = _download_attachment(attachment, out_dir, ordinal)
        written.append(path)
        size = os.path.getsize(path)
        print(f"Downloaded [{ordinal}/{len(attachments)}] {path} ({size} bytes)")

    if len(written) > 1:
        print(f"Downloaded {len(written)} attachment(s) to {out_dir}")


def dispatch(cmd, argv):
    if cmd != "download":
        raise RuntimeError(f"Unknown download command: {cmd}")
    download(argv)
