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

The host is pre-beta. Its API and storage may change without a compatibility layer.

## Documents

- [STORE_API.md](STORE_API.md) covers what every host shares (requests, errors, limits and the
  status document) and the content store: the record format, publishing, fetching and copying from
  peers. It is everything needed to run or use a store-only host.
- [GATEWAY_API.md](GATEWAY_API.md) covers the gateway: indexed reads and refresh, transaction
  services, names, link previews, the keyring and host policy, with a walkthrough for client
  authors.
- [config.example.toml](config.example.toml) explains every setting.
- [ARCHITECTURE.md](ARCHITECTURE.md) is a map of the code: where to start for a given change, and
  which tests cover it.
- [INDEXING.md](INDEXING.md) describes the gateway's index: what it keeps on disk, what it rebuilds
  in memory and how it recovers.
- [STORAGE.md](STORAGE.md) lists everything the host keeps on disk or in memory, and what survives a
  restart.
- [SHARING.md](SHARING.md) describes the pages the gateway serves when a post's link is shared, so
  other apps can show a preview.

## Build

You need a recent stable Rust toolchain; CI uses 1.98.

```sh
cargo build --release
```

The program is `target/release/blohard-host`. `--help` lists its options.

## Run against a local chain

This needs [Foundry](https://getfoundry.sh) and a checkout of
[blohard/protocol](https://github.com/blohard/protocol).

1. Start a local chain with the id the example configuration uses:

   ```sh
   anvil --chain-id 810337
   ```

2. Deploy the Board from the protocol checkout, with one of the test keys anvil prints:

   ```sh
   forge script script/Deploy.s.sol --rpc-url http://127.0.0.1:8545 --broadcast --private-key <key>
   ```

   It lands at `0xca3E637985f1866b92e56fCf82Bae7cbf479c7Bf`, the address the example uses.

3. Start the host with the example configuration, which serves both roles:

   ```sh
   cp config.example.toml config.toml
   ./target/release/blohard-host --config config.toml
   ```

4. Check that it is serving: `curl http://127.0.0.1:8080/api/status`.

The host keeps its data in `./data` unless you pass `--data-dir`. Set `RUST_LOG=info` to see what it
is doing.

## Run in production

- **Put a reverse proxy in front of it, on the same machine.** The host listens on `127.0.0.1:8080`
  by default and doesn't serve TLS itself. It limits each visitor separately, and it takes the
  visitor's address from the last entry of the last `X-Forwarded-For` line, only when the request
  comes from the same machine. The proxy must append or overwrite that entry with the real client
  address; anything before it is the client's own. Behind a proxy on another machine, every visitor
  would look like one.
- **Use a keyed RPC provider.** Each chain's `rpc_url` must answer `eth_getLogs` and
  `eth_getBlockReceipts`. Set `[upstream]` to your plan's limits.
- **Serve the web app** with `--web-dir`, pointing at the app's build. The app will be published in
  its own repository.
- **Keep the data directory.** [STORAGE.md](STORAGE.md) says what can be rebuilt from the chain and
  what can't, such as the content itself.

## Test

```sh
cargo test --workspace
cargo test --workspace -- --ignored   # the slow tests, which wait for real timeouts
```

CI also runs `cargo fmt --check` and `cargo clippy` with warnings as errors.

## Not included

Sponsored posting goes through a separate relayer service, which is not published yet. It reads the
host's lists from the same file (see `[host_policy]` in the example configuration).

## License

[MIT](LICENSE)
