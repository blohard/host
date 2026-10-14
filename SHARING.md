# Share pages

Apps like X, Slack and Discord show a preview card when someone shares a link. They read the card's
title, description and image from the page's metadata, and most of them don't run JavaScript. So
when a gateway serves the built web app with `--web-dir`, it fills in that metadata itself for the
app's shareable pages. The same URLs still open the app for people.

| Page | Card |
|---|---|
| `/p/<chainId>/<postId>` | The post: its date, author and the start of its text. |
| `/u/<chainId>/<address>` and its tabs `/replies`, `/keys`, `/keyring` and `/keyring/holders` | The account's verified name, or its address, and what the tab shows. |
| `/about` and `/keys/<chainId>/about` | Fixed cards for the about pages. |

The gateway writes the card into the app's own `index.html`, between its
`<!-- blohard-meta:start -->` and `<!-- blohard-meta:end -->` markers, escaping every value. The
site's address comes from that file's `og:url`, never from the request's `Host` header, so a
deployment on another domain must update the app's metadata to match.

## Post cards

A post's card uses only records the gateway's index has accepted. Before it quotes any text, the
gateway checks the whole stored record against the content hash the chain committed to. If the body
isn't stored locally, it may fetch it from a peer, with the same checks.

- The title is the post's date in UTC and its author, for example
  `blohard - 01 Oct 2026 - alice.eth`.
- The description is the start of the post, with whitespace collapsed: at most 240 characters,
  followed by an ellipsis when cut short.
- The card also carries the author, the chain id and the post's Unix time. Open Graph gets the wide
  whale picture. X gets its small `summary` card with the square whale avatar. Other apps choose
  their own layout. Cards have no images made for the individual post.

Looking up the author's name is optional and bounded in time. If it fails, the card shows the
author's address, shortened the way the app shows it (`0x1234…abcd`). A name is only a label; the
indexed address is what establishes who wrote the post.

An optional `?commit=0x…` pins the exact record, using the same commitment as a quoted post. If the
commitment doesn't match, the answer is 409, without the post's current text. `?reply` opens the
reply box in the app, and is left out of the card's canonical URL.

Unknown chains and posts answer 404. Deleted and removed posts answer 410, without their original
text.

## When a card can't be built

If a card can't be built in time, because the index is still catching up, a read fails or the host
is busy, the page is served with the app's generic card instead. Only an unreadable `index.html`
answers 503, with `Retry-After`.

Card responses are marked `no-store`. Other apps may still keep their own copies of a preview,
including one fetched before a post was deleted.

## Limits

The gateway builds at most eight cards at a time. Each build has eight seconds, and its name lookup
has two of them, or five for a link-preview crawler (recognized by its `User-Agent`), since a
crawler fetches a card once and keeps it. Each visitor may start four builds, then one every two
seconds, at most two at a time. A card built in the last minute is served from a cache, at no cost.
After reading the content and the name, a build checks the indexed record again before rendering, so
it never shows a record that was replaced in the meantime.
