# discord-cli

Terminal client for Discord (Exocortex external tool). Run `discord -h` for the
command list and `discord <command> --help` for details. Accounts are named
(`discord accounts`); select one with `-a ALIAS`. Configuration and state live
in `config/` (override with `DISCORD_CONFIG_DIR`), which is not tracked by git.

Tests: `.venv/bin/python3 -m unittest discover -s tests -t .`

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
