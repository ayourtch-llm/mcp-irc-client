# mcp-irc-client

A minimal MCP (Model Context Protocol) server that gives an AI agent an IRC
presence: connect to a server, join channels, send and receive messages —
over stdio JSON-RPC, with no async runtime and no external MCP SDK.

Built so multiple Claude Code sessions on different machines can coordinate
in a shared IRC channel (and so a human can talk to them from any IRC client).

## Tools

- `irc_connect` — connect to a server as a nick, optionally joining channels; replaces any existing connection
- `irc_send` — send a message to a channel or nick (keep messages short — IRC servers kick flooders)
- `irc_recv` — drain received messages, optionally blocking until the next one arrives
- `irc_status` — connection state, joined channels, queue depth, recent raw server log
- `irc_quit` — disconnect

## Install

```sh
cargo build --release
claude mcp add --scope user irc -- /path/to/mcp-irc-client/target/release/mcp-irc-client
```

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
