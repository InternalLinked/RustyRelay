//! C-ABI shim over `RelayClient`.
//!
//! This file is the single source csbindgen reads to generate the matching
//! C# `DllImport` declarations. Keep every exported item `#[unsafe(no_mangle)] pub
//! extern "C"` and stick to FFI-safe types (raw pointers, primitives, byte
//! buffers) — no `String`, `Bytes`, `Result`, or Rust enums with data cross
//! this boundary directly.
//!
//! ## Status code convention
//! Every operation returns a `u8`:
//! - `255` (`FfiStatus::Ok`)      -> success
//! - `0..=8`                      -> passed straight through from `SendableError`
//!                                    (see that enum's doc comments for meaning)
//! - `100..=199`                  -> FFI/connection-level errors that have no
//!                                    `SendableError` equivalent (see `FfiStatus`)
//!
//! ## Ownership convention
//! - `relay_client_connect` hands you an opaque `*mut RelayClientHandle`. You
//!   own it and must eventually call `relay_client_free`.
//! - `relay_client_poll_event` hands you a `RelayEvent` by value (you
//!   allocate it, we just fill it in). If `kind == Broadcast`, its
//!   `data_ptr`/`data_len` describe a Rust-allocated buffer you must return
//!   via `relay_free_buffer` exactly once — don't free it any other way and
//!   don't use it after freeing. For every other `kind`, no buffer exists
//!   and there's nothing to free.
//! - Every other buffer (lobby codes, join codes, broadcast payloads) is
//!   caller-allocated: you own it before and after the call.
//!
//! ## Panic safety
//! Every exported function is wrapped in `catch_unwind`. If your Rust code
//! panics (e.g. an `unwrap()` deep in the packet handler), the panic is
//! caught here and converted to `FfiStatus::PanicCaught` instead of
//! unwinding across the FFI boundary, which would otherwise be undefined
//! behavior and typically crashes the whole CLR process.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;

use bytes::Bytes;

use crate::networking::client::{RelayClient, RelayClientCallback, RelayHandleError};
use crate::networking::packethandler::packets::error_packet::SendableError;

/// Opaque handle representing a connected `RelayClient`, as seen from the
/// FFI boundary.
///
/// This is deliberately a distinct, zero-sized marker type rather than
/// exposing `*mut RelayClient` directly:
/// - csbindgen echoes Rust type names verbatim into the generated C#, and
///   `RelayClient` collided with the `RelayClient.Interop` C# namespace
///   ("'RelayClient' is a namespace but is used like a type").
/// - It also decouples the public ABI from the internal `RelayClient`
///   struct — renaming/refactoring it on the Rust side won't change the
///   exported symbol signatures.
///
/// Never constructed or dereferenced as itself; only used as a typed
/// pointer that gets cast to/from `*mut RelayClient` at the two edges
/// (`as_handle` / `handle_to_client` / `handle_into_box` below).
#[repr(C)]
pub struct RelayClientHandle {
    _private: [u8; 0],
}

fn as_handle(client: Box<RelayClient>) -> *mut RelayClientHandle {
    Box::into_raw(client).cast()
}

/// Turns a `*mut RelayClientHandle` into a safe `&mut RelayClient`, or
/// `None` if the pointer is null. Caller must guarantee the pointer, if
/// non-null, was produced by `relay_client_connect` and hasn't been freed.
unsafe fn handle_to_client<'a>(handle: *mut RelayClientHandle) -> Option<&'a mut RelayClient> {
    if handle.is_null() {
        None
    } else {
        Some(&mut *handle.cast::<RelayClient>())
    }
}

/// Reconstructs the owning `Box<RelayClient>` from a handle, for freeing.
/// Caller must guarantee this handle is only ever consumed this way once.
unsafe fn handle_into_box(handle: *mut RelayClientHandle) -> Box<RelayClient> {
    Box::from_raw(handle.cast::<RelayClient>())
}

/// FFI-level status codes. Cast to `u8` when returning — functions return
/// plain `u8` (not this enum) to keep the csbindgen-generated C# signatures
/// simple.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FfiStatus {
    /// The call succeeded. Chosen to sit well outside `SendableError`'s
    /// 0..=8 range so the two error domains never collide.
    Ok = 255,

    // --- errors specific to `RelayClient::connect`, mapped from RelayHandleError ---
    InvalidAddress = 100,
    ServerRefusedConnection = 101,
    HandshakeFailure = 102,
    ConnectionClosedByPeer = 103,
    PipeError = 104,

    // --- generic FFI-boundary errors, can happen on any call ---
    NullPointer = 110,
    InvalidUtf8 = 111,
    BufferTooSmall = 112,
    PanicCaught = 120,
}

fn map_relay_handle_error(err: RelayHandleError) -> u8 {
    match err {
        RelayHandleError::InvalidAddress => FfiStatus::InvalidAddress as u8,
        RelayHandleError::ServerRefusedConnection => FfiStatus::ServerRefusedConnection as u8,
        RelayHandleError::HandshakeFailure => FfiStatus::HandshakeFailure as u8,
        RelayHandleError::ConnectionClosed => FfiStatus::ConnectionClosedByPeer as u8,
        RelayHandleError::PipeError => FfiStatus::PipeError as u8,
    }
}

/// Discriminant values for `RelayEvent::kind`.
///
/// This exists purely for readable `match`ing on the Rust side — the value
/// that actually crosses the FFI boundary is the plain `u8` field on
/// `RelayEvent`, not this enum itself. csbindgen won't auto-generate a C#
/// enum for it (it's never used directly in an extern "C" signature), so
/// the C# side mirrors these values by hand. Keep the two in sync if this
/// ever changes.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayEventKind {
    Broadcast = 0,
    LobbyJoined = 1,
    LobbyLeft = 2,
    LobbyKicked = 3,
    LobbySeatChanged = 4,
}

/// C-ABI representation of `RelayClientCallback`.
///
/// C has no tagged unions with variant-specific fields the way Rust does,
/// so this flattens all three variants into one fixed-layout struct: a
/// discriminant byte (`kind`, see `RelayEventKind`) plus every field any
/// variant might need. Fields that don't apply to the current `kind` are
/// always zeroed/null rather than left uninitialized.
///
/// - `author_id` is populated for `Broadcast`/`LobbyJoined`/`LobbyLeft`;
///   it's 0 for `LobbyKicked`/`LobbySeatChanged` and should be ignored.
/// - `seat_id` is ONLY meaningful when `kind == LobbySeatChanged` (4), and
///   is 0 for every other `kind`.
/// - `data_ptr`/`data_len` are ONLY meaningful when `kind == Broadcast`
///   (0). When populated, the buffer is Rust-allocated and must be
///   released via `relay_free_buffer` exactly once — same contract the old
///   `poll_packet` had. For `LobbyJoined`/`LobbyLeft` they're always
///   null/0 and there's nothing to free.
#[repr(C)]
pub struct RelayEvent {
    pub kind: u8,
    pub author_id: u64,
    pub data_ptr: *mut u8,
    pub data_len: usize,
    pub seat_id: u64,
}

impl RelayEvent {
    fn broadcast(author_id: usize, data_ptr: *mut u8, data_len: usize) -> Self {
        Self {
            kind: RelayEventKind::Broadcast as u8,
            author_id: author_id as u64,
            data_ptr,
            data_len,
            seat_id: 0,
        }
    }

    fn lobby_joined(author_id: usize) -> Self {
        Self {
            kind: RelayEventKind::LobbyJoined as u8,
            author_id: author_id as u64,
            data_ptr: ptr::null_mut(),
            data_len: 0,
            seat_id: 0,
        }
    }

    fn lobby_left(author_id: usize) -> Self {
        Self {
            kind: RelayEventKind::LobbyLeft as u8,
            author_id: author_id as u64,
            data_ptr: ptr::null_mut(),
            data_len: 0,
            seat_id: 0,
        }
    }

    /// `LobbyKicked` carries no fields on the Rust side (it's only ever
    /// sent to the client that got kicked, so there's no third party to
    /// identify) -- `author_id` is left at 0 and should be ignored by
    /// callers for this `kind`.
    fn lobby_kicked() -> Self {
        Self {
            kind: RelayEventKind::LobbyKicked as u8,
            author_id: 0,
            data_ptr: ptr::null_mut(),
            data_len: 0,
            seat_id: 0,
        }
    }

    /// Sent to this client when its own seat in the lobby changed.
    /// `author_id` is left at 0 and should be ignored for this `kind`.
    fn lobby_seat_changed(seat_id: usize) -> Self {
        Self {
            kind: RelayEventKind::LobbySeatChanged as u8,
            author_id: 0,
            data_ptr: ptr::null_mut(),
            data_len: 0,
            seat_id: seat_id as u64,
        }
    }

    fn empty() -> Self {
        Self {
            kind: 0,
            author_id: 0,
            data_ptr: ptr::null_mut(),
            data_len: 0,
            seat_id: 0,
        }
    }
}

/// Connects to `address` (a null-terminated UTF-8 string, e.g. "127.0.0.1:7777")
/// and writes an opaque handle to `out_handle` on success.
///
/// Returns `FfiStatus::Ok` (255) on success, otherwise one of the
/// `RelayHandleError`-mapped codes (100-104), `NullPointer` (110),
/// `InvalidUtf8` (111), or `PanicCaught` (120). On any error, `*out_handle`
/// is set to null.
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_connect(
    address: *const c_char,
    out_handle: *mut *mut RelayClientHandle,
) -> u8 {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if address.is_null() || out_handle.is_null() {
            return FfiStatus::NullPointer as u8;
        }

        let address_str = match unsafe { CStr::from_ptr(address) }.to_str() {
            Ok(s) => s,
            Err(_) => return FfiStatus::InvalidUtf8 as u8,
        };

        match RelayClient::connect(address_str) {
            Ok(client) => {
                unsafe {
                    *out_handle = as_handle(Box::new(client));
                }
                FfiStatus::Ok as u8
            }
            Err(err) => map_relay_handle_error(err),
        }
    }));

    match outcome {
        Ok(status) => status,
        Err(_) => {
            if !out_handle.is_null() {
                unsafe {
                    *out_handle = ptr::null_mut();
                }
            }
            FfiStatus::PanicCaught as u8
        }
    }
}

/// Creates a lobby and writes the lobby code into the caller-allocated
/// `out_code` buffer (`out_code_capacity` bytes long). The code is currently
/// always 8 bytes; pass a capacity of at least 8.
///
/// Returns `FfiStatus::Ok` on success, a raw `SendableError` value (0-8) if
/// the server rejected the request, or `BufferTooSmall` / `NullPointer` /
/// `PanicCaught` for FFI-level problems.
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_create_lobby(
    handle: *mut RelayClientHandle,
    out_code: *mut u8,
    out_code_capacity: usize,
) -> u8 {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let client = match unsafe { handle_to_client(handle) } {
            Some(c) => c,
            None => return FfiStatus::NullPointer as u8,
        };
        if out_code.is_null() {
            return FfiStatus::NullPointer as u8;
        }

        match client.create_lobby() {
            Ok(code) => {
                let bytes = code.as_bytes();
                if bytes.len() > out_code_capacity {
                    return FfiStatus::BufferTooSmall as u8;
                }
                unsafe {
                    ptr::copy_nonoverlapping(bytes.as_ptr(), out_code, bytes.len());
                }
                FfiStatus::Ok as u8
            }
            Err(err) => u8::from(err),
        }
    }));

    outcome.unwrap_or(FfiStatus::PanicCaught as u8)
}

/// Joins a lobby using `code` (a UTF-8 byte buffer, NOT null-terminated —
/// pass its exact length in `code_len`).
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_join_lobby(
    handle: *mut RelayClientHandle,
    code: *const u8,
    code_len: usize,
) -> u8 {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let client = match unsafe { handle_to_client(handle) } {
            Some(c) => c,
            None => return FfiStatus::NullPointer as u8,
        };
        if code.is_null() {
            return FfiStatus::NullPointer as u8;
        }

        let slice = unsafe { slice::from_raw_parts(code, code_len) };
        let code_str = match std::str::from_utf8(slice) {
            Ok(s) => s.to_owned(),
            Err(_) => return FfiStatus::InvalidUtf8 as u8,
        };

        match client.join_lobby(code_str) {
            Ok(()) => FfiStatus::Ok as u8,
            Err(err) => u8::from(err),
        }
    }));

    outcome.unwrap_or(FfiStatus::PanicCaught as u8)
}

/// Leaves the current lobby.
///
/// Note: as of the current `RelayClient::leave_lobby` implementation, this
/// only succeeds while `lobby_state == Alone`, which looks inverted from the
/// intended check (you'd normally expect to leave a lobby *while in one*).
/// Worth double-checking on the Rust side — the FFI layer just passes
/// whatever `SendableError` comes back through unchanged.
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_leave_lobby(handle: *mut RelayClientHandle) -> u8 {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let client = match unsafe { handle_to_client(handle) } {
            Some(c) => c,
            None => return FfiStatus::NullPointer as u8,
        };

        match client.leave_lobby() {
            Ok(()) => FfiStatus::Ok as u8,
            Err(err) => u8::from(err),
        }
    }));

    outcome.unwrap_or(FfiStatus::PanicCaught as u8)
}

/// Closes the current lobby (host-only, per your server-side authorization).
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_close_lobby(handle: *mut RelayClientHandle) -> u8 {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let client = match unsafe { handle_to_client(handle) } {
            Some(c) => c,
            None => return FfiStatus::NullPointer as u8,
        };

        match client.close_lobby() {
            Ok(()) => FfiStatus::Ok as u8,
            Err(err) => u8::from(err),
        }
    }));

    outcome.unwrap_or(FfiStatus::PanicCaught as u8)
}

/// Returns whether this client is currently hosting a lobby.
///
/// Unlike every other exported function, this doesn't return an
/// `FfiStatus` — `RelayClient::is_host` can't fail in Rust (it's a plain
/// `&self` query with no `Result`), so there's nothing meaningful to
/// report beyond the answer itself. A null handle is treated as `false`
/// rather than surfaced as `NullPointer` — there's no separate error
/// channel here to put it in, so don't call this with a null/freed handle
/// and expect to detect that from the return value.
///
/// Returns `1` for true, `0` for false. Deliberately `u8`, not Rust
/// `bool` — C#'s default P/Invoke marshaling for a `bool` return value is
/// a 4-byte Win32 `BOOL`, which doesn't match Rust's 1-byte `bool` and
/// would read past it. `u8` avoids that mismatch entirely.
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_is_host(handle: *mut RelayClientHandle) -> u8 {
    let outcome = catch_unwind(AssertUnwindSafe(|| match unsafe { handle_to_client(handle) } {
        Some(client) => client.is_host() as u8,
        None => 0,
    }));

    outcome.unwrap_or(0)
}

/// Returns the latest measured round-trip latency to the relay server, in
/// milliseconds; 0 before the first ping has completed.
///
/// Like `relay_client_is_host`, this returns the value directly rather than
/// an `FfiStatus`: it is a lock-free atomic load that can't fail. A null
/// handle (or a caught panic) yields 0.
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_get_latency(handle: *mut RelayClientHandle) -> u64 {
    let outcome = catch_unwind(AssertUnwindSafe(|| match unsafe { handle_to_client(handle) } {
        Some(client) => client.get_latency(),
        None => 0,
    }));

    outcome.unwrap_or(0)
}

/// Kicks the client occupying `seat_id` from the current lobby. Host-only
/// on the server side — if the caller isn't currently hosting, the
/// underlying `kick_client` call itself returns `NotAuthorized` rather than
/// this layer rejecting it, so that error still comes back as a normal
/// `SendableError` code (6).
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_kick_client(handle: *mut RelayClientHandle, seat_id: u64) -> u8 {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let client = match unsafe { handle_to_client(handle) } {
            Some(c) => c,
            None => return FfiStatus::NullPointer as u8,
        };

        match client.kick_client(seat_id as usize) {
            Ok(()) => FfiStatus::Ok as u8,
            Err(err) => u8::from(err),
        }
    }));

    outcome.unwrap_or(FfiStatus::PanicCaught as u8)
}

/// Broadcasts `data` (`data_len` bytes) to the current lobby.
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_broadcast(
    handle: *mut RelayClientHandle,
    data: *const u8,
    data_len: usize,
) -> u8 {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let client = match unsafe { handle_to_client(handle) } {
            Some(c) => c,
            None => return FfiStatus::NullPointer as u8,
        };
        if data.is_null() && data_len > 0 {
            return FfiStatus::NullPointer as u8;
        }

        let slice = if data_len == 0 {
            &[][..]
        } else {
            unsafe { slice::from_raw_parts(data, data_len) }
        };
        let payload = Bytes::copy_from_slice(slice);

        match client.broadcast_lobby(payload) {
            Ok(()) => FfiStatus::Ok as u8,
            Err(err) => u8::from(err),
        }
    }));

    outcome.unwrap_or(FfiStatus::PanicCaught as u8)
}

/// Blocks until the next event (broadcast packet, lobby join/leave/kick,
/// or seat change) arrives, and writes it into the caller-allocated `*out_event`.
///
/// If the written `kind == Broadcast` (0), `data_ptr`/`data_len` describe a
/// Rust-allocated buffer you MUST release with `relay_free_buffer` exactly
/// once. For every other `kind` (1..=4), no buffer is allocated —
/// there's nothing to free.
///
/// Call this from a dedicated background thread on the C# side (there's no
/// `async`/`await` in .NET 3.5, and this call blocks indefinitely).
///
/// Returns `FfiStatus::Ok` with `*out_event` populated on success, or
/// `FfiStatus::ConnectionClosedByPeer` with `*out_event` zeroed if the
/// internal channel has closed (e.g. after `relay_client_close`).
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_poll_event(
    handle: *mut RelayClientHandle,
    out_event: *mut RelayEvent,
) -> u8 {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let client = match unsafe { handle_to_client(handle) } {
            Some(c) => c,
            None => return FfiStatus::NullPointer as u8,
        };
        if out_event.is_null() {
            return FfiStatus::NullPointer as u8;
        }

        match client.poll_event() {
            Some(RelayClientCallback::Broadcast { author_id, buffer }) => {
                let mut boxed_slice = buffer.to_vec().into_boxed_slice();
                let len = boxed_slice.len();
                let data_ptr = boxed_slice.as_mut_ptr();
                std::mem::forget(boxed_slice);
                unsafe {
                    *out_event = RelayEvent::broadcast(author_id, data_ptr, len);
                }
                FfiStatus::Ok as u8
            }
            Some(RelayClientCallback::LobbyJoined { author_id }) => {
                unsafe {
                    *out_event = RelayEvent::lobby_joined(author_id);
                }
                FfiStatus::Ok as u8
            }
            Some(RelayClientCallback::LobbyLeft { author_id }) => {
                unsafe {
                    *out_event = RelayEvent::lobby_left(author_id);
                }
                FfiStatus::Ok as u8
            }
            Some(RelayClientCallback::LobbyKicked) => {
                unsafe {
                    *out_event = RelayEvent::lobby_kicked();
                }
                FfiStatus::Ok as u8
            }
            Some(RelayClientCallback::LobbySeatChanged { seat_id }) => {
                unsafe {
                    *out_event = RelayEvent::lobby_seat_changed(seat_id);
                }
                FfiStatus::Ok as u8
            }
            None => {
                unsafe {
                    *out_event = RelayEvent::empty();
                }
                FfiStatus::ConnectionClosedByPeer as u8
            }
        }
    }));

    match outcome {
        Ok(status) => status,
        Err(_) => {
            if !out_event.is_null() {
                unsafe {
                    *out_event = RelayEvent::empty();
                }
            }
            FfiStatus::PanicCaught as u8
        }
    }
}

/// Frees a buffer previously returned by `relay_client_poll_event` (only
/// when its `kind` was `Broadcast`). Safe to call with `data == null`
/// (no-op). Do not call twice on the same pointer, and do not call on a
/// buffer you allocated yourself.
#[unsafe(no_mangle)]
pub extern "C" fn relay_free_buffer(data: *mut u8, len: usize) {
    if data.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        drop(Box::from_raw(slice::from_raw_parts_mut(data, len)));
    }));
}

/// Closes the underlying TCP connection without freeing the handle. Safe to
/// call before `relay_client_free`; `relay_client_free` also calls this
/// internally as a safety net, so calling it explicitly first is optional
/// but recommended so you can distinguish "closed" from "freed" on the C#
/// side.
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_close(handle: *mut RelayClientHandle) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if let Some(client) = unsafe { handle_to_client(handle) } {
            client.close_client();
        }
    }));
}

/// Releases the handle returned by `relay_client_connect`. Closes the
/// connection first if it hasn't been closed already. Do not use the
/// handle after calling this, and do not call this twice on the same
/// handle.
#[unsafe(no_mangle)]
pub extern "C" fn relay_client_free(handle: *mut RelayClientHandle) {
    if handle.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        let mut boxed = handle_into_box(handle);
        boxed.close_client();
        // `boxed` drops here, releasing the TCPClientHandle and channel.
    }));
}