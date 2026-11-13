# RustyRelay

RustyRelay is a small, encrypted relay server and client library written in Rust. It lets players
host and join **lobbies** without opening ports or connecting directly to each other. Every
message goes through the relay server.

- Transport: TCP, encrypted with the [Noise protocol](https://noiseprotocol.org/) (`snow`, AES-GCM)
- Client API: `RelayClient` (Rust), plus a C ABI with generated C# bindings (`bindings/RelayClientNative.g.cs`)
- Server-side rate limiting per packet type

---

## Table of contents

1. [Core concepts](#core-concepts)
2. [Running the server](#running-the-server)
3. [Using the RelayClient (Rust)](#using-the-relayclient-rust)
   - [Connecting](#connecting)
   - [Hosting a lobby](#hosting-a-lobby)
   - [Joining a lobby](#joining-a-lobby)
   - [Sending data](#sending-data)
   - [Receiving events](#receiving-events)
   - [Kicking, leaving and closing](#kicking-leaving-and-closing)
   - [Latency](#latency)
   - [Disconnecting](#disconnecting)
4. [Event reference](#event-reference)
5. [Error reference](#error-reference)
6. [Using the client from C# / other languages (FFI)](#using-the-client-from-c--other-languages-ffi)
7. [Limits and notes](#limits-and-notes)
8. [Usage of AI in this project](#usage-of-ai-in-this-project)

---

## Core concepts

| Term | Meaning |
|------|---------|
| **Host** | The client that created a lobby. It always sits on **seat `0`**. |
| **Member** | A client that joined a lobby with a lobby code. Each member gets a **seat id** (`1`, `2`, …). Seats freed by leaving or kicked members can be reused. |
| **Lobby code** | An 8-character alphanumeric string (e.g. `aB3xK9qZ`) that the host shares so others can join. |
| **Seat id** | Identifies a member inside a lobby. Used for kicking, direct messages and to tell who sent a message. |

### Message routing (star topology)

The relay uses a **host-authoritative** model. Members never talk to each other directly:

```
          ┌──────────── broadcast ────────────┐
          │                                   ▼
   Member (seat 1) ──► Relay ──► Host ──► Relay ──► all Members
   Member (seat 2) ──►        (seat 0)
```

- A **member's** broadcast goes **only to the host**. The host sees the sender's seat id.
- A **host's** broadcast goes to **every member**.
- The host can also send a message to **one specific member** (`send_explicit_lobby`).

If members need to see each other's data, the host forwards it.

---

## Running the server

```rust
use RustyRelay::networking::server::tcp::TCPServerCore;

fn main() {
    // Starts the relay on its own runtime and returns immediately
    TCPServerCore::run("0.0.0.0:25565".parse().unwrap());

    std::thread::park(); // keep the process alive
}
```

The default limits are set in `src/networking/server/tcp/mod.rs`:

```rust
LobbyManagerHandle::new(96 /* channel buffer */, 255 /* max lobbies */, 2 /* max members per lobby, host not counted */)
```

Change these values to fit your game.

---

## Using the RelayClient (Rust)

All `RelayClient` calls are **blocking**. The client runs its own background Tokio runtime
internally, so you don't need an async runtime in your code. Don't call these methods from
inside an async context.

> **Note: an async client is planned.** The current client is blocking because it was written
> for my upcoming **Cuphead multiplayer mod**, where a simple blocking API fits best. An async
> option will be added to the library in the future.

### Connecting

```rust
use RustyRelay::networking::client::RelayClient;

let mut client = RelayClient::connect("127.0.0.1:25565")?;
```

`connect` parses the address, opens a TCP connection and runs the encrypted handshake.
It returns a `RelayHandleError` on failure:

| Error | Cause |
|-------|-------|
| `InvalidAddress` | The string is not a valid `ip:port` socket address. |
| `ServerRefusedConnection` | The TCP connection could not be opened. |
| `HandshakeFailure` | The Noise handshake failed (e.g. version mismatch). |
| `ConnectionClosed` | The server closed the connection. |
| `PipeError` | Internal channel error. |

### Hosting a lobby

```rust
let code: String = client.create_lobby()?;
println!("Lobby created! Share this code: {code}");

assert!(client.is_host());
```

`create_lobby` fails with `AlreadyHostOrMember` if the client is already in a lobby,
or with `MaxLobbiesExceded` if the server has reached its lobby limit.

### Joining a lobby

```rust
client.join_lobby("aB3xK9qZ".to_string())?;
```

Possible errors: `LobbyNotFound`, `LobbyFull`, `AlreadyHostOrMember`.

After a successful join you get a `LobbySeatChanged { seat_id }` event with your own seat id
(see [Receiving events](#receiving-events)).

### Sending data

Payloads are raw bytes (`bytes::Bytes`), so you can use any serialization format.

**Broadcast**: host → all members, or member → host:

```rust
use bytes::Bytes;

client.broadcast_lobby(Bytes::from_static(b"hello lobby"))?;
```

**Explicit message**: host → one member (host only):

```rust
client.send_explicit_lobby(1 /* seat id */, Bytes::from_static(b"just for you"))?;
```

- A member calling `send_explicit_lobby` gets `NotAuthorized`.
- Sending to an empty seat is silently ignored. It still returns `Ok(())`.
- Both calls return `NoMember` if you are not in a lobby, and `PayloadTooLarge` if the payload
  is bigger than `LobbyBroadcast::MAX_DYNAMIC_SIZE` / `LobbyExplicit::MAX_DYNAMIC_SIZE`.
- Both calls don't wait for the server: they return as soon as the packet is queued.

### Receiving events

Everything that comes **from** the server (data, players joining or leaving, being kicked)
arrives as a `RelayClientCallback` through `poll_event`:

```rust
use RustyRelay::networking::client::RelayClientCallback;

while let Some(event) = client.poll_event() {
    match event {
        RelayClientCallback::Broadcast { author_id, buffer } => {
            // Host: author_id = sender's seat id
            // Member: author_id is always 0 (data always comes from the host)
            println!("{} bytes from seat {author_id}", buffer.len());
        }
        RelayClientCallback::LobbyJoined { author_id } => {
            println!("Player on seat {author_id} joined");
        }
        RelayClientCallback::LobbyLeft { author_id } => {
            println!("Player on seat {author_id} left");
        }
        RelayClientCallback::LobbyKicked => {
            println!("We were kicked or the lobby was closed");
        }
        RelayClientCallback::LobbySeatChanged { seat_id } => {
            println!("Our seat is now {seat_id}");
        }
    }
}
// poll_event returned None -> connection closed
```

Important:

- `poll_event` **blocks** until the next event arrives. It returns `None` once the connection
  is closed.
- The local lobby state (e.g. switching back to "not in a lobby" after a kick) is updated
  **while polling**. Keep polling so `is_host()` and the other calls stay accurate.
- Events are buffered in a channel with room for 96 entries. If you stop polling, the client's
  network task eventually stalls, so poll continuously.
- `poll_event` takes `&mut self`, like the send methods. In Rust you can't call it from a
  separate thread while other methods run on the same client, so build your loop around
  it (e.g. poll on one thread and send from the same thread when events arrive).

### Kicking, leaving and closing

**Kick a member** (host only):

```rust
client.kick_client(2 /* seat id */)?;
```

- The kicked client gets a `LobbyKicked` event. Everyone else in the lobby gets `LobbyLeft { author_id: 2 }`.
- Errors: `NotAuthorized` (not the host), `SeatNotFound` (seat is empty).
- Kicking seat `0` (yourself) **closes the lobby**.

**Leave a lobby** (member):

```rust
client.leave_lobby()?;
```

- The host and the other members get `LobbyLeft { author_id: <your seat> }`.
- You get `LobbySeatChanged { seat_id: 0 }`.
- Returns `NoMember` if you are not in a lobby.

**Close a lobby** (host):

```rust
client.close_lobby()?;
```

- Every member is removed and gets a `LobbyKicked` event.
- The lobby code becomes invalid.
- Returns `NotAuthorized` if you are not the host.

> Hosts should use `close_lobby()` rather than `leave_lobby()` to end their lobby.

### Latency

The client sends a ping to the server every 5 seconds:

```rust
let ms: u64 = client.get_latency(); // 0 until the first ping has completed
```

### Disconnecting

```rust
client.close_client(); // closes the connection; poll_event will return None
```

Dropping a `RelayClient` also closes the connection. When a client disconnects, the server
cleans up automatically:

- **Host disconnects**: the lobby is closed and every member gets `LobbyKicked`.
- **Member disconnects**: the host gets `LobbyLeft { author_id: <seat> }`.

---

## Event reference

| Event | Who receives it | Fields |
|-------|-----------------|--------|
| `Broadcast { author_id, buffer }` | Host: data from a member.<br>Member: data from the host (broadcast or explicit). | `author_id`: sender seat (always `0` for members), `buffer`: payload |
| `LobbyJoined { author_id }` | Host and all **other** members when someone joins. | `author_id`: seat of the new player |
| `LobbyLeft { author_id }` | Host and all **other** members when someone leaves or is kicked. Only the host when a member disconnects. | `author_id`: seat of the player who left |
| `LobbyKicked` | The member who was kicked; every member when the lobby is closed or the host disconnects. | — |
| `LobbySeatChanged { seat_id }` | Yourself, after joining (`seat_id` = your seat) and after leaving (`seat_id = 0`). | `seat_id`: your new seat |

---

## Error reference

Lobby operations return `SendableError` (`src/networking/packethandler/packets/error_packet.rs`).
The numeric value is also the status code returned by the FFI layer.

| Code | Variant | Meaning |
|-----:|---------|---------|
| 0 | `ConnectionClose` | Connection to the server is closed. |
| 1 | `Kick` | Reserved. |
| 2 | `AlreadyHostOrMember` | You are already in a lobby. |
| 3 | `LobbyFull` | The lobby has reached its member limit. |
| 4 | `MaxLobbiesExceded` | The server has reached its lobby limit. |
| 5 | `LobbyNotFound` | No lobby with that code exists (anymore). |
| 6 | `NotAuthorized` | Only the host can do this. |
| 7 | `SeatNotFound` | No member sits on that seat. |
| 8 | `NoMember` | You are not in a lobby. |
| 9 | `RessourceRateLimited` | You are sending this packet type too often; the server rate-limited you. |
| 10 | `PayloadTooLarge` | Payload doesn't fit into one encrypted frame. |

---

## Using the client from C# / other languages (FFI)

The crate builds as a `cdylib` (`RustyRelay.dll` / `libRustyRelay.so`) and exports a C ABI from
`src/ffi.rs`. On every build, `build.rs` uses [csbindgen](https://github.com/Cysharp/csbindgen)
to regenerate `bindings/RelayClientNative.g.cs` (namespace `RelayClient.Interop`, compatible with .NET 3.5+ / Unity).

```bash
cargo build --release
```

### Status codes

Every function that can fail returns a `byte`:

| Value | Meaning |
|------:|---------|
| `255` | **Ok** |
| `0`–`10` | `SendableError` (see [Error reference](#error-reference)) |
| `100` | Invalid address |
| `101` | Server refused connection |
| `102` | Handshake failure |
| `103` | Connection closed by peer |
| `104` | Pipe error |
| `110` | Null pointer |
| `111` | Invalid UTF-8 |
| `112` | Buffer too small |
| `120` | A Rust panic was caught |

### Functions

| Function | Description |
|----------|-------------|
| `relay_client_connect(address, out_handle)` | Connect. `address` is a null-terminated UTF-8 string. |
| `relay_client_create_lobby(handle, out_code, capacity)` | Create a lobby. Writes the 8-byte code into your buffer (capacity ≥ 8). |
| `relay_client_join_lobby(handle, code, code_len)` | Join with a code (not null-terminated). |
| `relay_client_leave_lobby(handle)` | Leave the current lobby. |
| `relay_client_close_lobby(handle)` | Close your lobby (host). |
| `relay_client_kick_client(handle, seat_id)` | Kick a member (host). |
| `relay_client_broadcast(handle, data, len)` | Broadcast bytes. |
| `relay_client_send_explicit(handle, seat_id, data, len)` | Send bytes to one member (host). |
| `relay_client_poll_event(handle, out_event)` | **Blocking.** Wait for the next event. |
| `relay_free_buffer(data, len)` | Free the payload of a `Broadcast` event. |
| `relay_client_is_host(handle)` | Returns `1` / `0`. |
| `relay_client_get_latency(handle)` | Latency in ms. |
| `relay_client_close(handle)` | Close the connection (handle stays valid). |
| `relay_client_free(handle)` | Close (if needed) and free the handle. Call exactly once. |

### `RelayEvent`

| `kind` | Event | Valid fields |
|-------:|-------|--------------|
| 0 | Broadcast | `author_id`, `data_ptr`, `data_len` (**must be freed with `relay_free_buffer`**) |
| 1 | LobbyJoined | `author_id` |
| 2 | LobbyLeft | `author_id` |
| 3 | LobbyKicked | — |
| 4 | LobbySeatChanged | `seat_id` |

### C# example

```csharp
using System;
using System.Text;
using System.Threading;
using RelayClient.Interop;

unsafe class RelayExample
{
    RelayClientHandle* handle;

    public void Start()
    {
        byte[] addr = Encoding.UTF8.GetBytes("127.0.0.1:25565\0");
        fixed (byte* a = addr)
        fixed (RelayClientHandle** h = &handle)
        {
            byte status = RelayClientNative.relay_client_connect(a, h);
            if (status != 255) throw new Exception("connect failed: " + status);
        }

        // Host a lobby
        byte[] code = new byte[8];
        fixed (byte* c = code)
        {
            byte status = RelayClientNative.relay_client_create_lobby(handle, c, (UIntPtr)code.Length);
            if (status == 255) Console.WriteLine("Lobby code: " + Encoding.UTF8.GetString(code));
        }

        // poll_event blocks, so run it on a background thread
        new Thread(EventLoop) { IsBackground = true }.Start();
    }

    void EventLoop()
    {
        RelayEvent ev;
        while (RelayClientNative.relay_client_poll_event(handle, &ev) == 255)
        {
            switch (ev.kind)
            {
                case 0: // Broadcast
                    byte[] payload = new byte[(int)ev.data_len];
                    for (int i = 0; i < payload.Length; i++) payload[i] = ev.data_ptr[i];
                    RelayClientNative.relay_free_buffer(ev.data_ptr, ev.data_len);
                    Console.WriteLine("Data from seat " + ev.author_id);
                    break;
                case 1: Console.WriteLine("Seat " + ev.author_id + " joined"); break;
                case 2: Console.WriteLine("Seat " + ev.author_id + " left"); break;
                case 3: Console.WriteLine("Kicked / lobby closed"); break;
                case 4: Console.WriteLine("Our seat is now " + ev.seat_id); break;
            }
        }
        // Loop exits once the connection is closed
    }

    public void Send(byte[] data)
    {
        fixed (byte* d = data)
            RelayClientNative.relay_client_broadcast(handle, d, (UIntPtr)data.Length);
    }

    public void Stop()
    {
        RelayClientNative.relay_client_close(handle);
        // Wait for the event thread to exit before freeing!
        RelayClientNative.relay_client_free(handle);
    }
}
```

> Make sure the event thread has stopped polling before you call `relay_client_free`.
> Using the handle after freeing it is undefined behavior.

---

## Limits and notes

- **Max payload**: a single message must fit into one Noise frame (max 65535 bytes including
  overhead). Use `LobbyBroadcast::MAX_DYNAMIC_SIZE` for the exact limit and split larger data yourself.
- **Rate limiting**: the server limits how often each packet type may be sent. Exceeding the
  limit returns `RessourceRateLimited`. Spamming broadcasts can get you throttled.
- **Spoofing protection**: members include their seat id when broadcasting. If the server sees a
  wrong seat id, it disconnects the client.
- **UDP**: UDP transport is not implemented yet. Everything currently runs over TCP.
- **Blocking only (for now)**: only a blocking client exists so far. An async client is planned
  (see [the note above](#using-the-relayclient-rust)).

---

## Usage of AI in this project

AI was used in this project for repetitive work, such as:

- **FFI bindings**: writing the C ABI layer in `src/ffi.rs` used to generate the C# bindings.
- **Packet macro cleanup**: making the code of my original packet macro
  (`src/networking/packethandler/packet_macro`) easier to read, since the first version was a mess.
- **Documentation**: this README was written by AI.

Everything else in this project was **designed and written by me**. Any AI-generated code is
listed in this section, and all of it was closely reviewed and checked by me.

## License

See [LICENSE](LICENSE).
