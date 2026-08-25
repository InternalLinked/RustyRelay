use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use syn::{
    parse::{Parse, ParseStream, Parser},
    parse_macro_input, Data, DeriveInput, Expr, Fields, Lit, LitBool, LitInt, Token, Type,
};

struct PacketProperties {
    id: u8,
    dynamic: bool,
    tcp: bool,
    ip_based: bool,
    max_cache: usize,
    cache_threshold: usize,
    timeout: usize,
}

impl Parse for PacketProperties {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let id: LitInt = input.parse()?;
        input.parse::<Token![,]>()?;
        let tcp: LitBool = input.parse()?;
        input.parse::<Token![,]>()?;
        let dynamic: LitBool = input.parse()?;
        input.parse::<Token![,]>()?;
        let ip_based: LitBool = input.parse()?;
        input.parse::<Token![,]>()?;
        let max_cache: LitInt = input.parse()?;
        input.parse::<Token![,]>()?;
        let cache_threshold: LitInt = input.parse()?;
        input.parse::<Token![,]>()?;
        let timeout: LitInt = input.parse()?;

        Ok(PacketProperties {
            id: id.base10_parse::<u8>()?,
            tcp: tcp.value(),
            dynamic: dynamic.value(),
            ip_based: ip_based.value(),
            max_cache: max_cache.base10_parse::<usize>()?,
            cache_threshold: cache_threshold.base10_parse::<usize>()?,
            timeout: timeout.base10_parse::<usize>()?,
           
        })
    }
}

/// Everything we need to know about a single field's wire representation,
/// computed once instead of being re-derived independently in three
/// different places (capacity, deserialize, serialize).
struct FieldPlan {
    /// Bytes this field occupies on the wire.
    size: usize,
    /// `let <field>: <ty> = buf.get_...();` (or array equivalent).
    /// Used in `from`, where `buf: &mut Bytes`. Reads directly out of the
    /// caller-supplied buffer via the `Buf` trait, consuming it in place --
    /// no intermediate copy.
    read: TokenStream2,
    /// `buf.put_...(self.<field>);` (or array equivalent).
    /// Used in `encode`, where `buf: &mut BytesMut`. Writes directly into
    /// the caller-supplied buffer via the `BufMut` trait.
    write: TokenStream2,
}

fn plan_for_field(field: &syn::Field) -> FieldPlan {
    let field_name = field.ident.as_ref().unwrap();
    let field_type = &field.ty;

    match field_type {
        Type::Path(type_path) => {
            let type_name = type_path.path.segments.last().unwrap().ident.to_string();

            let (size, getter, putter) = match type_name.as_str() {
                "u8" => (1, quote::format_ident!("get_u8"), quote::format_ident!("put_u8")),
                "u16" => (2, quote::format_ident!("get_u16"), quote::format_ident!("put_u16")),
                "u32" => (4, quote::format_ident!("get_u32"), quote::format_ident!("put_u32")),
                "u64" => (8, quote::format_ident!("get_u64"), quote::format_ident!("put_u64")),
                "i8" => (1, quote::format_ident!("get_i8"), quote::format_ident!("put_i8")),
                "i16" => (2, quote::format_ident!("get_i16"), quote::format_ident!("put_i16")),
                "i32" => (4, quote::format_ident!("get_i32"), quote::format_ident!("put_i32")),
                "i64" => (8, quote::format_ident!("get_i64"), quote::format_ident!("put_i64")),
                "f32" => (4, quote::format_ident!("get_f32"), quote::format_ident!("put_f32")),
                "f64" => (8, quote::format_ident!("get_f64"), quote::format_ident!("put_f64")),
                other => panic!("Type {} is not supported as packet type!", other),
            };

            FieldPlan {
                size,
                read: quote::quote! { let #field_name: #field_type = buf.#getter(); },
                write: quote::quote! { buf.#putter(self.#field_name); },
            }
        }

        Type::Array(type_array) => {
            if *type_array.elem != syn::parse_quote!(u8) {
                panic!("Only [u8; N] arrays are allowed.");
            }

            let Expr::Lit(syn::ExprLit { lit: Lit::Int(lit_int), .. }) = &type_array.len else {
                panic!("Array length must be an integer literal");
            };
            let length: usize = lit_int
                .base10_parse::<usize>()
                .expect("Was not able to parse array length of array");

            FieldPlan {
                size: length,
                // `copy_to_slice`/`put_slice` are plain `bytes::Buf`/`BufMut`
                // methods -- same panic-on-insufficient-bytes behavior as
                // the numeric getters above, no custom bytebatch code
                // needed.
                read: quote::quote! {
                    let mut #field_name: [u8; #length] = [0u8; #length];
                    buf.copy_to_slice(&mut #field_name);
                },
                write: quote::quote! {
                    buf.put_slice(&self.#field_name);
                },
            }
        }

        _ => panic!("Type {:?} is unsupported", field_type),
    }
}

#[proc_macro_attribute]
pub fn packet(attr: TokenStream, item: TokenStream) -> TokenStream {
    let properties = parse_macro_input!(attr as PacketProperties);
    let id = properties.id;
    let is_dynamic = properties.dynamic;
    let is_tcp = properties.tcp;

    let mut ast: DeriveInput = syn::parse(item).unwrap();
    let struct_name = ast.ident.clone();

    // Prepend synthetic fields. `dynamic_size` must come before `nonce` so
    // that `nonce` ends up first on the wire (needed for deserialization).
    // Every packet is awaitable now, so `nonce` is unconditional.
    let prepend_field = |ast: &mut DeriveInput, tokens: TokenStream2| {
        if let Data::Struct(data_struct) = &mut ast.data {
            if let Fields::Named(fields) = &mut data_struct.fields {
                fields
                    .named
                    .insert(0, syn::Field::parse_named.parse2(tokens).unwrap());
            }
        }
    };

    if is_dynamic {
        prepend_field(&mut ast, quote::quote! { dynamic_size: u64 });
    }
    prepend_field(&mut ast, quote::quote! { nonce: u64 });
    // `awaitable` is inserted last, so it lands first on the wire (ahead of
    // `nonce`). It's a plain u8 flag: 0 = not awaitable, 1 = awaitable on
    // the client, 2 = awaitable on the server. Read-only from the outside
    // except via `set_awaitable_client`/`set_awaitable_server`, which are
    // one-directional.
    prepend_field(&mut ast, quote::quote! { awaitable: u8 });

    let fields: syn::punctuated::Punctuated<syn::Field, syn::Token![,]> = match &ast.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => fields.named.clone(),
            _ => panic!("Only named fields are supported"),
        },
        _ => panic!("Only structs are supported"),
    };

    // Public getter for every user-declared field (not the synthetic ones).
    // Array fields are returned by reference instead of by value -- these
    // can be large (e.g. `[u8; 32]`), and returning by value would memcpy
    // the whole array on every call. Scalars (u8/u32/etc.) are `Copy` and
    // cheap to return by value, so those are left as-is.
    let readers = fields.iter().filter_map(|field| {
        let field_name = field.ident.as_ref().unwrap();
        if field_name == "nonce" || field_name == "dynamic_size" || field_name == "awaitable" {
            return None;
        }

        let field_type = &field.ty;
        let getter_name = quote::format_ident!("read_{}", field_name);

        let (ret_type, body) = match field_type {
            Type::Array(_) => (
                quote::quote! { &#field_type },
                quote::quote! { &self.#field_name },
            ),
            _ => (
                quote::quote! { #field_type },
                quote::quote! { self.#field_name },
            ),
        };

        Some(quote::quote! {
            pub fn #getter_name(&self) -> #ret_type {
                #body
            }
        })
    });

    // One pass per field computes size + (de)serialization code together,
    // instead of three separate matches over the same type.
    let plans: Vec<FieldPlan> = fields.iter().map(plan_for_field).collect();

    // +1 for the packet-id byte written at the start of every packet.
    let capacity: usize = 1 + plans.iter().map(|p| p.size).sum::<usize>();
    let field_readers = plans.iter().map(|p| &p.read);
    let field_writers = plans.iter().map(|p| &p.write);

    let field_names: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();

    // `new()`'s parameter list: every field except `dynamic_size` (derived
    // from the payload slice below rather than passed in separately),
    // `nonce` (not settable at construction time -- it starts at 0 and can
    // only be changed afterward via `set_nonce()`), and `awaitable` (starts
    // at 0, can only be set afterward via `set_awaitable_client()` /
    // `set_awaitable_server()`).
    let mut ctor_params: Vec<TokenStream2> = fields
        .iter()
        .filter(|f| {
            let name = f.ident.as_ref().unwrap();
            !(is_dynamic && name == "dynamic_size") && name != "nonce" && name != "awaitable"
        })
        .map(|f| {
            let name = f.ident.as_ref().unwrap();
            let ty = &f.ty;
            quote::quote! { #name: #ty }
        })
        .collect();
    // `Bytes` instead of `&[u8]`: the caller hands over an already-owned
    // buffer (e.g. `Bytes::from(vec)`), so `new()` can just take it as-is
    // with no copy. If the caller only has a borrowed slice, they pay the
    // copy at the call site via `Bytes::copy_from_slice`, but callers who
    // already own the data (common when it just came off a socket or was
    // built in place) get a genuinely zero-copy path.
    if is_dynamic {
        ctor_params.push(quote::quote! { dynamic_payload: Bytes });
    }

    // Dynamic packets carry a second buffer, separate from the wire buffer
    // used by `encode`/`from`, that holds the variable-length payload
    // described by `dynamic_size`. It isn't one of the fixed wire fields, so
    // it's added after `fields` was collected above and is set up/sized at
    // runtime rather than being part of the fixed-size (de)serialization.
    // `Bytes` (not `BytesMut`) because it's only ever read from after being
    // populated once in `new`/`from` -- never mutated piecemeal.
    if is_dynamic {
        prepend_field(&mut ast, quote::quote! { dynamic_buffer: Bytes });
    }

    // `, dynamic_buffer` appended to the `Self { .. }` literals below.
    let dynamic_buffer_field_init = if is_dynamic {
        quote::quote! { , dynamic_buffer }
    } else {
        quote::quote! {}
    };

    // Splits `dynamic_buffer` off the tail of the incoming buffer in
    // `from()`. By this point all fixed-size fields have been consumed via
    // `Buf::get_*`, so whatever remains is exactly `dynamic_size` bytes of
    // payload. `split_to` is zero-copy (shares the underlying allocation).
    let dynamic_buffer_from_init = if is_dynamic {
        quote::quote! {
            let dynamic_buffer: Bytes = buf.split_to(dynamic_size as usize);
        }
    } else {
        quote::quote! {}
    };

    // `dynamic_size` is computed from the payload's length rather than
    // taken as a separate constructor argument, so the two can never
    // disagree.
    let dynamic_size_from_payload = if is_dynamic {
        quote::quote! {
            let dynamic_size: u64 = dynamic_payload.len() as u64;
        }
    } else {
        quote::quote! {}
    };

    // No copy: `dynamic_payload` is already an owned `Bytes`, so the
    // struct just takes it over directly.
    let dynamic_buffer_new_init = if is_dynamic {
        quote::quote! {
            let dynamic_buffer: Bytes = dynamic_payload;
        }
    } else {
        quote::quote! {}
    };

    // Public accessor for the dynamic buffer.
    let dynamic_buffer_getter = if is_dynamic {
        quote::quote! {
            pub fn get_dynamic(&self) -> &Bytes {
                &self.dynamic_buffer
            }
        }
    } else {
        quote::quote! {}
    };

    // The second element of `encode()`'s return tuple: the dynamic payload,
    // handed back as its own `Bytes` instead of being copied into `buf`.
    // `Bytes::clone()` is a refcounted pointer bump, not a copy, so this is
    // free -- the actual bytes are shared with `self.dynamic_buffer` and,
    // beyond that, with whatever `new()`/`from()` populated it from.
    let dynamic_payload_out = if is_dynamic {
        quote::quote! { Some(self.dynamic_buffer.clone()) }
    } else {
        quote::quote! { None }
    };

    let dynamic_impl = if is_dynamic {
        quote::quote! { impl DynamicPacket for #struct_name {} }
    } else {
        quote::quote! {}
    };

    let dynamic_functions = if is_dynamic {
        quote::quote! {
            fn get_dynamic_size(&self) -> u64 {
                self.dynamic_size
            }
        }
    } else {
        quote::quote! {
            fn get_dynamic_size(&self) -> u64 {
                0
            }
        }
    };

    // A packet only ever travels over one transport. Rather than making
    // implementors stub out the trigger methods for the transport that
    // doesn't apply (which would otherwise fall through to the trait's
    // `todo!()` defaults and panic if ever accidentally called), the macro
    // no-ops that pair for them. The pair for the transport this packet
    // *does* use is left alone, falling through to the trait's default
    // (`todo!()`) so the implementor is still forced to fill it in.
    let transport_functions = if is_tcp {
        quote::quote! {
            async fn on_trigger_server_udp(&self) {

            }

            async fn on_trigger_client_udp(&self) {}
        }
    } else {
        quote::quote! {
            async fn on_trigger_server_tcp(&self, client_handle: &mut TCPClientHandle, encryption_state: &mut EncryptionState, manager_handler: &mut LobbyManagerHandle<TCPClientHandle>) -> ServerCallback {
                ServerCallback::Pass
            }

            async fn on_trigger_client_tcp(&self, client_handle: &mut TCPClientHandle) -> ClientCallback {
                ClientCallback::Pass
            }
        }
    };

    quote::quote! {
        #ast

        impl #struct_name {
            pub const ID: u8 = #id;
            pub const CAPACITY: usize = #capacity;
            pub const DYNAMIC: bool = #is_dynamic;
            pub const TCP: bool = #is_tcp;
        }

        impl Packet for #struct_name {
            fn get_id(&self) -> u8 {
                Self::ID
            }

            // NOTE: this assumes the `Packet` trait's method has been
            // updated to
            // `fn encode(&mut self, buf: &mut BytesMut) -> io::Result<(Bytes, Option<Bytes>)>;`
            // The first element is the packet-id + fixed fields; the second
            // is the dynamic payload (if this packet is dynamic), returned
            // as its own `Bytes` rather than copied into `buf`. Send both
            // pieces with a vectored write (`write_vectored` /
            // `AsyncWriteVectored`) to avoid concatenating them into one
            // buffer before they hit the socket. Update the trait
            // definition if it doesn't already look like this.
            fn encode(&mut self, buf: &mut BytesMut) -> io::Result<(Bytes, Option<Bytes>)> {
                buf.put_u8(Self::ID);

                #(#field_writers)*

                Ok((buf.split().freeze(), #dynamic_payload_out))
            }

            fn get_size(&self) -> usize {
                Self::CAPACITY + (self.get_dynamic_size() as usize)
            }

            #dynamic_functions

            // `nonce` is a synthetic field present on every packet, so
            // `get_nonce`/`set_nonce` are no longer conditional, and
            // `get_nonce` no longer needs to express "packet has no nonce"
            // via `None`.
            fn get_nonce(&self) -> u64 {
                self.nonce
            }

            fn set_nonce(&mut self, nonce: u64) {
                self.nonce = nonce;
            }

            // `awaitable` is a single synthetic u8: 0 = not awaitable,
            // 1 = awaitable on the client side, 2 = awaitable on the
            // server side. The two `set_*` methods are one-directional
            // (no unsetter) and mutually exclusive -- calling one after
            // the other overwrites the previous value rather than
            // combining them, since a packet only ever awaits on one side.
            fn set_awaitable_client(&mut self) {
                self.awaitable = 1;
            }

            fn set_awaitable_server(&mut self) {
                self.awaitable = 2;
            }

            fn is_awaitable_client(&self) -> bool {
                self.awaitable == 1
            }

            fn is_awaitable_server(&self) -> bool {
                self.awaitable == 2
            }

            #transport_functions
        }

        #dynamic_impl
        impl Awaitable for #struct_name {}

        impl #struct_name {
            /// Parses fields out of `buf`, consuming it in place. Assumes
            /// `buf` starts at the packet-id byte (which is skipped) and
            /// contains exactly this packet's bytes -- for dynamic packets,
            /// that's `CAPACITY - 1 + dynamic_size` bytes after the id.
            /// No intermediate allocation: fixed fields are read directly
            /// off `buf` via `Buf::get_*`/`copy_to_slice`, and the dynamic
            /// payload (if any) is split off with zero-copy `split_to`.
            pub fn from(buf: &mut Bytes) -> io::Result<Self> {
                buf.advance(1); // skip the packet-id byte

                #(#field_readers)*

                #dynamic_buffer_from_init

                Ok(Self {
                    #(#field_names),*
                    #dynamic_buffer_field_init
                })
            }

            /// Just fills in the fields -- no serialization happens here.
            /// Call `encode()` with a reusable buffer when you actually
            /// need the wire bytes. `nonce` is not a parameter: it starts
            /// at 0 and must be set afterward via `set_nonce()`. Likewise
            /// `awaitable` starts at 0 (neither client- nor
            /// server-awaitable) and can only be set afterward via
            /// `set_awaitable_client()` / `set_awaitable_server()`.
            pub fn new(#(#ctor_params),*) -> Self {
                #dynamic_size_from_payload
                let nonce: u64 = 0;
                let awaitable: u8 = 0;

                #dynamic_buffer_new_init

                Self {
                    #(#field_names),*
                    #dynamic_buffer_field_init
                }
            }

            #(#readers)*

            #dynamic_buffer_getter
        }
    }
    .into()
}