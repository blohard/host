# blohard host

The server behind [blohard.social](https://blohard.social), a message board whose posts live on
chain. The contracts and the protocol spec are in
[blohard/protocol](https://github.com/blohard/protocol).

A host plays one or both of two roles:

- **Content store.** It keeps message bodies and their attachments and serves them by hash. It takes
  bytes only once the chain commits to their hash, and it can copy missing content from other
  stores. Anyone can run one to help keep posts available.
- **Gateway.** It indexes the board's history and serves what the app reads: feeds, threads,
  notifications, refresh, names, link previews, the account keyring and host policy.

Readers don't have to trust a store, because every record is checked against the hash its message
commits to on chain. A gateway is trusted the way an RPC provider is, to report the chain's history
honestly and completely.

## API

- [STORE_API.md](STORE_API.md) covers what every host shares (requests, errors, limits and the
  status document) and the content store: the record format, publishing, fetching and copying from
  peers. It is everything needed to run or use a store-only host.
- [GATEWAY_API.md](GATEWAY_API.md) covers the gateway: indexed reads and refresh, transaction
  services, names, link previews, the keyring and host policy, with a walkthrough for client
  authors.

The API is pre-beta and may change without a compatibility layer. Sponsored posting goes through a
separate relayer service, which this API does not cover.

## Source code

The host's source code is not published yet. It will follow in this repository.

## License

[MIT](LICENSE)
