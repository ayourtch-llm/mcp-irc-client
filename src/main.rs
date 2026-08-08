//! mcp-irc-client — an MCP (Model Context Protocol) stdio server that exposes
//! a plain IRC client as tools, so a Claude Code session can join an IRC
//! network and converse (including with other Claude sessions).
//!
//! Tools: irc_connect, irc_send, irc_recv (blocking poll), irc_status, irc_quit.
//! Transport: newline-delimited JSON-RPC 2.0 on stdin/stdout (MCP stdio).
//!
//! The server, nick and channel are pinned at process start via mandatory
//! command-line arguments; the tools cannot connect anywhere else or message
//! any other target. This keeps the model's blast radius to one channel on
//! one server chosen by whoever registered the MCP server.

struct Pinned {
    server: String,
    port: u16,
    nick: String,
    channel: String,
}

fn parse_pinned() -> Pinned {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        eprintln!("usage: mcp-irc-client <server[:port]> <nick> <#channel>");
        eprintln!("  all three are mandatory; the tools are pinned to these values");
        std::process::exit(2);
    }
    let (server, port) = match args[0].rsplit_once(':') {
        Some((h, p)) if p.parse::<u16>().is_ok() => (h.to_string(), p.parse().unwrap()),
        _ => (args[0].clone(), 6667),
    };
    Pinned { server, port, nick: args[1].clone(), channel: args[2].clone() }
}

use serde_json::{json, Value};
use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

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
    let server = pinned.server.clone();
    let port = pinned.port;
    let nick = pinned.nick.clone();
    let channels: Vec<String> = vec![pinned.channel.clone()];

    let stream = TcpStream::connect((server.as_str(), port))
        .map_err(|e| format!("connect {server}:{port} failed: {e}"))?;
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
    let target = pinned.channel.as_str();
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
                "Connect to the pinned IRC server {}:{} as {} and join {}. Server, nick and channel are fixed at process start and cannot be changed. Replaces any existing connection.",
                pinned.server, pinned.port, pinned.nick, pinned.channel),
            "inputSchema": {"type": "object", "properties": {}}
        },
        {
            "name": "irc_send",
            "description": format!(
                "Send a message to the pinned channel {} (the only allowed target). Multi-line messages are sent as one PRIVMSG per line. KEEP MESSAGES SHORT: IRC caps a line at ~450 bytes and the server kicks clients for flooding — send at most 2-3 short lines at a time, chat-style, not paragraphs. Split longer thoughts across multiple irc_send calls with pauses in between.",
                pinned.channel),
            "inputSchema": {
                "type": "object",
                "properties": {
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

fn main() {
    let pinned = parse_pinned();
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
