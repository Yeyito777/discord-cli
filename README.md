# discord-cli

Terminal client for Discord (Exocortex external tool). Run `discord -h` for the
command list and `discord <command> --help` for details. Accounts are named
(`discord accounts`); select one with `-a ALIAS`. Configuration and state live
in `config/` (override with `DISCORD_CONFIG_DIR`), which is not tracked by git.

Tests: `.venv/bin/python3 -m unittest discover -s tests -t .`

## Channels

```sh
discord -a ALIAS channel create SERVER NAME [--type text|voice|announcement|category] [--topic T] [--position top|N] [--parent CATEGORY]
discord -a ALIAS channel move CHANNEL --position top|N [-g SERVER]
```

Both need Manage Channels and an explicit `-a` when several accounts exist.
`create` POSTs `/guilds/{id}/channels` (never resent after an ambiguous
failure) and refuses a same-name, same-type channel in the same category. With
`--position` it then reorders; `move` does only that. A position is a 0-based
index among the channel's siblings (same category, or the top level), `top` being
0, and N past the end means last. Reordering renumbers the siblings 0..n-1 and
PATCHes `/guilds/{id}/channels` with only the entries that changed. If the
reorder fails after the channel was created, the error gives the new ID and the
`move` command to retry.

## Notifications

Exocortex owns routing: `discord notify subscribe CONV [--channel ID --guild G]`
creates a core subscription to one of two sources per account,
`account:<alias>:notifications` (DMs and @mentions) or
`account:<alias>:channel:<id>:messages` (every new post in one server channel).

The supervised daemon (manifest `daemon`, `python -m src.notify __daemon__`,
see `src/supervisor.py`) owns every Gateway that collects them. It reads the
core subscription registry at start, every 15 s, and at once on SIGHUP (the CLI
sends one after subscribe/unsubscribe/start). It runs:

- the DMs/@mentions Gateway of the supervised account (`discord account notify`),
  always, and of any other account while it has an enabled subscription;
- one `__channel_notify__<id>` Gateway per subscribed channel and account,
  stopped after the last unsubscribe.

Each Gateway is a child process with its own `DISCORD_ACCOUNT` and keeps its
per-account/channel singleton lock, so there is exactly one collector per
source. Starts are spaced 5 s apart, because Discord invalidates sessions that
identify at the same moment. A Gateway that exits is restarted with backoff
(1 s up to 60 s), and a copy started outside the daemon is replaced. Gateways reconnect through network
drops themselves, backing off while connects keep failing. exocortexd restarts
the daemon, and kills its whole process group on stop, so nothing depends on
the process that ran the CLI.

```sh
discord -a ALIAS notify subscribe CONV --channel ID --guild G   # waits for "ready"
discord -a ALIAS notify list --channel ID --guild G             # Listener: ready (PID …), supervised by …
discord -a ALIAS notify unsubscribe CONV --channel ID           # last one stops the listener
```

`notify start --channel` only nudges the daemon and waits for a subscribed
channel to be ready; `notify stop --channel` refuses while subscriptions remain.
`notify start|stop` without `--channel`, for the supervised account, starts or
stops the whole daemon, channel listeners included. Runtime files (PID, lock,
state, log, err) live in `$XDG_RUNTIME_DIR/discord-cli/<config-hash>/<alias>/`,
and the daemon's PID in `supervisor.pid` one level up. Daemon logs go to
`config/service.log`.

Don't rely on a process started from an Exocortex turn to outlive it. Each turn
runs in a transient systemd unit, and its whole cgroup, `setsid` children
included, is killed when the turn ends. So `discord listen` from a turn lasts
only as long as that turn, and a hand-run `gateway.py` is not a durable
listener; subscribe instead.

## GIFs

`discord gifs` finds GIFs the way Discord's own GIF picker does, and keeps a
small local library of GIFs learned from chats.

### Picker search

```sh
discord gifs search this is fine -n 5     # or --json
discord gifs trending -n 10
```

These call the client's authenticated picker endpoints
(`GET /api/v9/gifs/search?q&media_format=mp4&locale=en-US&limit` and
`/gifs/trending-gifs`; Discord requires `limit >= 20` and returns at most 100).
The provider behind them is currently Klipy. Each result prints a title and a
page URL such as `https://klipy.com/gifs/this-fine-4`. That is the same `url`
field the desktop client sends as message content when you pick a GIF, and
Discord unfurls it into an animated `gifv` embed. To post one, send that URL as
the whole message:

```sh
echo 'https://klipy.com/gifs/this-fine-4' | discord -a ALIAS send CHANNEL
```

Use these results instead of guessing Tenor URLs. Tenor page links that already
exist still embed, but a made-up or mistyped ID will not.

### Learned library

Learning GIFs from chat context was zosa's idea.

The library is a plain JSON file at `config/gifs.json`. All accounts share it,
and it stores only URLs, tags and notes. Writes are atomic and use a lock file.

```sh
# Scan recent history for GIFs other people posted, with surrounding messages
discord gifs learn general -g 'My Server' -n 200 -C 3      # or a channel ID; --json
# Save what one means, judging from that context
discord gifs save https://klipy.com/gifs/this-fine-4 \
    --tags 'this is fine,coping,chaos' --title 'Dog in burning room' \
    --note 'reply when everything is breaking but we carry on' \
    --source CHANNEL_ID/MESSAGE_ID
# Reuse later
discord gifs find coping chaos        # ranks by number of matching tags/title/note words
discord gifs list | discord gifs tags | discord gifs check | discord gifs remove URL
```

`learn` is read-only and does not mark the channel as read. By default it hides
GIFs that are already saved; `--all` shows them along with their saved tags.
`learn` picks up `gifv` embeds (Klipy, Tenor, Giphy), `.gif` image embeds and
uploaded `.gif` attachments. If a GIF link has no embed, it is marked
`[no embed: may be dead]`. Saving a URL that is already in the library merges
its tags.

### Discord uploads (`cdn.discordapp.com/attachments/...`)

Fetched directly, attachment URLs stop working once their `ex`/`is`/`hm`
signature expires (about 24h) or is missing. You get "This content is no longer
available." Sent as message content, though, the unsigned URL still embeds as an
animated GIF, because Discord re-signs the embed's URLs every time the message
is read. A message whose content is an unsigned attachment `.gif` link gets an
`image/gif` embed with freshly signed thumbnail URLs.

An upload really dies only when its original message or attachment is deleted.
So the library:

- stores uploads in their canonical, unsigned form,
  `https://cdn.discordapp.com/attachments/CHANNEL/ATTACHMENT/name.gif`
  (`media.discordapp.net` links and query strings are normalized away);
- refuses to `save` an upload unless it is verified alive. To verify, the tool
  re-signs it with `POST /api/v9/attachments/refresh-urls`
  (`{"attachment_urls": [...]}`, 1–50 per call; changes nothing and is not
  written to the audit log) and fetches the re-signed URL. Discord signs any
  well-formed path, even a deleted one, so the fetch is what proves it exists;
- re-checks matched uploads in `find` and skips dead ones (`--no-check` for
  offline use);
- offers `discord gifs check [--prune]` to verify or remove saved uploads.

Prefer provider GIFs from `gifs search` when an equivalent exists; they don't
depend on someone else's message surviving.
