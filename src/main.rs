//! mcp-irc-client — an MCP (Model Context Protocol) stdio server that exposes
//! a plain IRC client as tools, so a Claude Code session can join an IRC
//! network and converse (including with other Claude sessions).
//!
//! Tools: irc_connect, irc_send, irc_recv (blocking poll), irc_status, irc_quit.
//! Transport: newline-delimited JSON-RPC 2.0 on stdin/stdout (MCP stdio).
//!
//! The server, nick and channel(s) are pinned at startup via mandatory CLI
//! flags; the tools cannot connect elsewhere, use another nick, or post to
//! other channels. `--server` is repeatable and acts as an ordered fallback
//! list (e.g. direct LAN address first, local port-forward second).

use serde_json::{json, Value};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Values pinned at startup — the model cannot override them via tool args.
#[derive(Clone)]
struct Pinned {
    servers: Vec<(String, u16)>, // ordered fallback list
    nick: String,
    channels: Vec<String>,
}

struct IrcState {
    stream: Option<TcpStream>, // write half (reader thread owns a clone)
    nick: String,
    server: String,
    channels: Vec<String>,
    registered: bool,
    messages: VecDeque<String>, // human-readable events, drained by irc_recv
    log: VecDeque<String>,      // recent raw lines, for irc_status debugging
}

impl IrcState {
    fn new() -> Self {
        IrcState {
            stream: None,
            nick: String::new(),
            server: String::new(),
            channels: Vec::new(),
            registered: false,
            messages: VecDeque::new(),
            log: VecDeque::new(),
        }
    }
}

type Shared = Arc<(Mutex<IrcState>, Condvar)>;

fn send_raw(st: &mut IrcState, line: &str) -> Result<(), String> {
    match st.stream.as_mut() {
        Some(s) => s
            .write_all(format!("{line}\r\n").as_bytes())
            .map_err(|e| format!("send failed: {e}")),
        None => Err("not connected".into()),
    }
}

fn push_event(shared: &Shared, text: String) {
    let (lock, cv) = &**shared;
    let mut st = lock.lock().unwrap();
    st.messages.push_back(text);
    if st.messages.len() > 500 {
        st.messages.pop_front();
    }
    cv.notify_all();
}

/// Parse ":nick!user@host CMD args :trailing" into (prefix_nick, cmd, args, trailing).
fn parse_irc(line: &str) -> (String, String, Vec<String>, String) {
    let mut rest = line;
    let mut prefix = String::new();
    if let Some(r) = rest.strip_prefix(':') {
        if let Some(sp) = r.find(' ') {
            prefix = r[..sp].split('!').next().unwrap_or("").to_string();
            rest = &r[sp + 1..];
        }
    }
    let (head, trailing) = match rest.find(" :") {
        Some(i) => (&rest[..i], rest[i + 2..].to_string()),
        None => (rest, String::new()),
    };
    let mut parts = head.split_whitespace().map(|s| s.to_string());
    let cmd = parts.next().unwrap_or_default().to_uppercase();
    (prefix, cmd, parts.collect(), trailing)
}

fn reader_thread(shared: Shared, stream: TcpStream) {
    let reader = BufReader::new(stream);
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let (from, cmd, args, trailing) = parse_irc(&line);
        {
            let (lock, _) = &*shared;
            let mut st = lock.lock().unwrap();
            st.log.push_back(line.clone());
            if st.log.len() > 100 {
                st.log.pop_front();
            }
            match cmd.as_str() {
                "PING" => {
                    let _ = send_raw(&mut st, &format!("PONG :{trailing}"));
                    continue;
                }
                "001" => st.registered = true,
                _ => {}
            }
        }
        let event = match cmd.as_str() {
            "PRIVMSG" | "NOTICE" => {
                let target = args.first().cloned().unwrap_or_default();
                Some(format!("[{target}] <{from}> {trailing}"))
            }
            "JOIN" => {
                let ch = args.first().cloned().unwrap_or(trailing.clone());
                Some(format!("* {from} joined {ch}"))
            }
            "PART" => {
                let ch = args.first().cloned().unwrap_or_default();
                Some(format!("* {from} left {ch}"))
            }
            "QUIT" => Some(format!("* {from} quit ({trailing})")),
            "NICK" => Some(format!("* {from} is now known as {trailing}")),
            "TOPIC" => {
                let ch = args.first().cloned().unwrap_or_default();
                Some(format!("* {from} set topic of {ch}: {trailing}"))
            }
            "KICK" => {
                let ch = args.first().cloned().unwrap_or_default();
                let who = args.get(1).cloned().unwrap_or_default();
                Some(format!("* {who} was kicked from {ch} by {from} ({trailing})"))
            }
            "353" => {
                // NAMES reply: args = [me, sym, channel]
                let ch = args.get(2).cloned().unwrap_or_default();
                Some(format!("* users in {ch}: {trailing}"))
            }
            _ => None,
        };
        if let Some(e) = event {
            push_event(&shared, e);
        }
    }
    let (lock, cv) = &*shared;
    let mut st = lock.lock().unwrap();
    st.stream = None;
    st.registered = false;
    st.messages.push_back("* disconnected from server".into());
    cv.notify_all();
}

// ---- tool implementations ----------------------------------------------------

fn tool_connect(shared: &Shared, pinned: &Pinned) -> Result<String, String> {
    let nick = pinned.nick.clone();
    let channels = pinned.channels.clone();

    let mut errors = Vec::new();
    let mut connected = None;
    for (server, port) in &pinned.servers {
        match TcpStream::connect((server.as_str(), *port)) {
            Ok(s) => {
                connected = Some((s, server.clone(), *port));
                break;
            }
            Err(e) => errors.push(format!("connect {server}:{port} failed: {e}")),
        }
    }
    let (stream, server, port) = connected.ok_or_else(|| errors.join("; "))?;
    let rstream = stream.try_clone().map_err(|e| e.to_string())?;
    {
        let (lock, _) = &**shared;
        let mut st = lock.lock().unwrap();
        st.stream = Some(stream);
        st.nick = nick.clone();
        st.server = format!("{server}:{port}");
        st.channels = channels.clone();
        st.registered = false;
        st.messages.clear();
        send_raw(&mut st, &format!("NICK {nick}"))?;
        send_raw(&mut st, &format!("USER {nick} 0 * :{nick} (mcp-irc-client)"))?;
    }
    let sh = shared.clone();
    std::thread::spawn(move || reader_thread(sh, rstream));

    // Wait for registration (001), then join.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        {
            let (lock, _) = &**shared;
            let mut st = lock.lock().unwrap();
            if st.registered {
                for ch in &channels.clone() {
                    send_raw(&mut st, &format!("JOIN {ch}"))?;
                }
                return Ok(format!(
                    "connected to {} as {nick}{}",
                    st.server,
                    if channels.is_empty() {
                        String::new()
                    } else {
                        format!(", joined {}", channels.join(" "))
                    }
                ));
            }
            if st.stream.is_none() {
                return Err("server closed the connection during registration".into());
            }
        }
        if Instant::now() > deadline {
            return Err("timed out waiting for IRC registration (001)".into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn tool_send(shared: &Shared, pinned: &Pinned, a: &Value) -> Result<String, String> {
    let target = match a["target"].as_str() {
        Some(t) => t,
        None => pinned.channels[0].as_str(),
    };
    // Pin the reachable targets: only the configured channel(s), or a bare
    // nick (a PM on the pinned server). No posting into other channels.
    if target.starts_with('#') || target.starts_with('&') {
        if !pinned.channels.iter().any(|c| c == target) {
            return Err(format!(
                "target '{target}' is not a pinned channel (allowed: {})",
                pinned.channels.join(" ")
            ));
        }
    }
    let message = a["message"].as_str().ok_or("missing 'message'")?;
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    let mut n = 0;
    for line in message.lines().filter(|l| !l.trim().is_empty()) {
        send_raw(&mut st, &format!("PRIVMSG {target} :{line}"))?;
        n += 1;
    }
    Ok(format!("sent {n} line(s) to {target}"))
}

fn tool_recv(shared: &Shared, a: &Value) -> Result<String, String> {
    let wait_secs = a["wait_secs"].as_f64().unwrap_or(0.0).clamp(0.0, 600.0);
    let (lock, cv) = &**shared;
    let mut st = lock.lock().unwrap();
    if st.messages.is_empty() && wait_secs > 0.0 {
        let deadline = Instant::now() + Duration::from_secs_f64(wait_secs);
        while st.messages.is_empty() {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let (g, _t) = cv.wait_timeout(st, deadline - now).unwrap();
            st = g;
        }
    }
    if st.messages.is_empty() {
        return Ok("(no new messages)".into());
    }
    let out: Vec<String> = st.messages.drain(..).collect();
    Ok(out.join("\n"))
}

fn tool_status(shared: &Shared) -> Result<String, String> {
    let (lock, _) = &**shared;
    let st = lock.lock().unwrap();
    if st.stream.is_none() {
        return Ok("not connected".into());
    }
    Ok(format!(
        "connected to {} as {} (registered={}), channels: {}, {} queued message(s)\nrecent raw log:\n{}",
        st.server,
        st.nick,
        st.registered,
        if st.channels.is_empty() { "(none)".into() } else { st.channels.join(" ") },
        st.messages.len(),
        st.log.iter().rev().take(10).rev().cloned().collect::<Vec<_>>().join("\n"),
    ))
}

fn tool_quit(shared: &Shared) -> Result<String, String> {
    let (lock, _) = &**shared;
    let mut st = lock.lock().unwrap();
    if st.stream.is_none() {
        return Ok("already disconnected".into());
    }
    let _ = send_raw(&mut st, "QUIT :leaving");
    st.stream = None;
    st.registered = false;
    Ok("disconnected".into())
}

// ---- MCP plumbing ------------------------------------------------------------

fn tools_json(pinned: &Pinned) -> Value {
    json!([
        {
            "name": "irc_connect",
            "description": format!(
                "(Re)connect to the pinned IRC server as '{}' and join {} (server/nick/channels are fixed at startup and cannot be changed). Replaces any existing connection.",
                pinned.nick, pinned.channels.join(" ")
            ),
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "irc_send",
            "description": "Send a message to the pinned channel (default) or as a PM to a nick. Multi-line messages are sent as one PRIVMSG per line. KEEP MESSAGES SHORT: IRC caps a line at ~450 bytes and the server kicks clients for flooding — send at most 2-3 short lines at a time, chat-style, not paragraphs. Split longer thoughts across multiple irc_send calls with pauses in between.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": {"type": "string", "description": "Pinned channel or a nick (default: the pinned channel)"},
                    "message": {"type": "string", "description": "Text to send"}
                },
                "required": ["message"]
            }
        },
        {
            "name": "irc_recv",
            "description": "Drain received IRC messages/events. If none are queued, optionally block up to wait_secs for the next one. Returns '(no new messages)' on timeout.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "wait_secs": {"type": "number", "description": "Seconds to block waiting if the queue is empty (0 = return immediately, max 600)"}
                }
            }
        },
        {
            "name": "irc_status",
            "description": "Show connection state, joined channels, queue depth, and the recent raw server log.",
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "irc_quit",
            "description": "Disconnect from the IRC server.",
            "inputSchema": {"type": "object", "properties": {}}
        }
    ])
}

fn parse_args() -> Result<Pinned, String> {
    let mut servers = Vec::new();
    let mut nick = None;
    let mut channels = Vec::new();
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let val = it
            .next()
            .ok_or_else(|| format!("missing value after {flag}"))?;
        match flag.as_str() {
            "--server" => {
                let (host, port) = match val.rsplit_once(':') {
                    Some((h, p)) => (
                        h.to_string(),
                        p.parse::<u16>().map_err(|_| format!("bad port in '{val}'"))?,
                    ),
                    None => (val, 6667),
                };
                servers.push((host, port));
            }
            "--nick" => nick = Some(val),
            "--channel" => {
                if !val.starts_with('#') && !val.starts_with('&') {
                    return Err(format!("channel '{val}' must start with # or &"));
                }
                channels.push(val);
            }
            _ => return Err(format!("unknown flag: {flag}")),
        }
    }
    if servers.is_empty() || nick.is_none() || channels.is_empty() {
        return Err("usage: mcp-irc-client --server host[:port] [--server ...] --nick NICK --channel '#chan' [--channel ...]".into());
    }
    Ok(Pinned {
        servers,
        nick: nick.unwrap(),
        channels,
    })
}

fn main() {
    let pinned = match parse_args() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("mcp-irc-client: {e}");
            std::process::exit(2);
        }
    };
    let shared: Shared = Arc::new((Mutex::new(IrcState::new()), Condvar::new()));
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.trim().is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let id = msg.get("id").cloned();
        let method = msg["method"].as_str().unwrap_or("").to_string();
        let reply = |result: Value| -> Value {
            json!({"jsonrpc": "2.0", "id": id.clone().unwrap_or(Value::Null), "result": result})
        };
        let response: Option<Value> = match method.as_str() {
            "initialize" => {
                let ver = msg["params"]["protocolVersion"]
                    .as_str()
                    .unwrap_or("2024-11-05");
                Some(reply(json!({
                    "protocolVersion": ver,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "mcp-irc-client", "version": env!("CARGO_PKG_VERSION")}
                })))
            }
            "ping" => Some(reply(json!({}))),
            "tools/list" => Some(reply(json!({"tools": tools_json(&pinned)}))),
            "tools/call" => {
                let name = msg["params"]["name"].as_str().unwrap_or("");
                let args = msg["params"]["arguments"].clone();
                let out = match name {
                    "irc_connect" => tool_connect(&shared, &pinned),
                    "irc_send" => tool_send(&shared, &pinned, &args),
                    "irc_recv" => tool_recv(&shared, &args),
                    "irc_status" => tool_status(&shared),
                    "irc_quit" => tool_quit(&shared),
                    _ => Err(format!("unknown tool: {name}")),
                };
                let (text, is_err) = match out {
                    Ok(t) => (t, false),
                    Err(e) => (e, true),
                };
                Some(reply(json!({
                    "content": [{"type": "text", "text": text}],
                    "isError": is_err
                })))
            }
            _ => {
                if id.is_some() && !method.starts_with("notifications/") {
                    Some(json!({"jsonrpc": "2.0", "id": id.clone().unwrap(),
                        "error": {"code": -32601, "message": format!("method not found: {method}")}}))
                } else {
                    None // notification — no response
                }
            }
        };
        if let Some(r) = response {
            let mut out = stdout.lock();
            let _ = writeln!(out, "{r}");
            let _ = out.flush();
        }
    }
}
