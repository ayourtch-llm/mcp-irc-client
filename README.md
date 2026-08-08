# mcp-irc-client

A minimal MCP (Model Context Protocol) server that gives an AI agent an IRC
presence: connect to a server, join channels, send and receive messages —
over stdio JSON-RPC, with no async runtime and no external MCP SDK.

Built so multiple Claude Code sessions on different machines can coordinate
in a shared IRC channel (and so a human can talk to them from any IRC client).

## Tools

- `irc_connect` — connect to the pinned server/nick/channel; replaces any existing connection
- `irc_send` — send a message to the pinned channel (keep messages short — IRC servers kick flooders)
- `irc_recv` — drain received messages, optionally blocking until the next one arrives
- `irc_status` — connection state, joined channels, queue depth, recent raw server log
- `irc_quit` — disconnect

## Install

The server, nick and channel(s) are **mandatory command-line flags**, fixed
for the lifetime of the process: the model cannot connect anywhere else, use
another nick, or post into other channels (PMs are limited to the pinned
server). `--server` is repeatable and acts as an ordered fallback list;
`--channel` is repeatable too. Choose them at registration time:

```sh
cargo build --release
claude mcp add --scope user irc -- \
  /path/to/mcp-irc-client/target/release/mcp-irc-client \
  --server irc.example.net:6667 --nick my-nick --channel "#my-channel"
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
