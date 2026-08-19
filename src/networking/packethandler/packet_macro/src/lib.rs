use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use syn::{
    parse::{Parse, ParseStream, Parser},
    parse_macro_input, Data, DeriveInput, Expr, Fields, Lit, LitBool, LitInt, Token, Type,
};

struct PacketProperties {
    id: u8,
    awaitable: bool,
    dynamic: bool,
    ip_based: bool,
    max_cache: usize,
    cache_threshold: usize,
    timeout: usize,
}

impl Parse for PacketProperties {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let id: LitInt = input.parse()?;
        input.parse::<Token![,]>()?;
        let awaitable: LitBool = input.parse()?;
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
            awaitable: awaitable.value(),
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
    let is_awaitable = properties.awaitable;
    let is_dynamic = properties.dynamic;

    let mut ast: DeriveInput = syn::parse(item).unwrap();
    let struct_name = ast.ident.clone();

    // Prepend synthetic fields. `dynamic_size` must come before `nonce` so
    // that `nonce` ends up first on the wire (needed for deserialization).
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
    if is_awaitable {
        prepend_field(&mut ast, quote::quote! { nonce: u64 });
    }

    let fields: syn::punctuated::Punctuated<syn::Field, syn::Token![,]> = match &ast.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields) => fields.named.clone(),
            _ => panic!("Only named fields are supported"),
        },
        _ => panic!("Only structs are supported"),
    };

    // Public getter for every user-declared field (not the synthetic ones).
    let readers = fields.iter().filter_map(|field| {
        let field_name = field.ident.as_ref().unwrap();
        if field_name == "nonce" || field_name == "dynamic_size" {
            return None;
        }

        let field_type = &field.ty;
        let getter_name = quote::format_ident!("read_{}", field_name);

        Some(quote::quote! {
            pub fn #getter_name(&self) -> #field_type {
                self.#field_name
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
    // from the payload slice below rather than passed in separately) and
    // `nonce` (not settable at construction time -- it starts at 0 and can
    // only be changed afterward via `set_nonce()`).
    let mut ctor_params: Vec<TokenStream2> = fields
        .iter()
        .filter(|f| {
            let name = f.ident.as_ref().unwrap();
            !(is_dynamic && name == "dynamic_size") && !(is_awaitable && name == "nonce")
        })
        .map(|f| {
            let name = f.ident.as_ref().unwrap();
            let ty = &f.ty;
            quote::quote! { #name: #ty }
        })
        .collect();
    if is_dynamic {
        ctor_params.push(quote::quote! { dynamic_payload: &[u8] });
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

    // Owns a copy of the caller-provided payload. This copy is unavoidable
    // in `new()` since the struct needs to own the data for as long as it
    // lives (unlike the wire buffer, which is only borrowed for the
    // duration of a single `encode`/`from` call).
    let dynamic_buffer_new_init = if is_dynamic {
        quote::quote! {
            let dynamic_buffer: Bytes = Bytes::copy_from_slice(dynamic_payload);
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

    // Appends the dynamic payload after the fixed-size portion when encoding.
    let dynamic_buffer_encode = if is_dynamic {
        quote::quote! {
            buf.extend_from_slice(&self.dynamic_buffer);
        }
    } else {
        quote::quote! {}
    };

    let dynamic_impl = if is_dynamic {
        quote::quote! { impl DynamicPacket for #struct_name {} }
    } else {
        quote::quote! {}
    };

    let awaitable_impl = if is_awaitable {
        quote::quote! { impl Awaitable for #struct_name {} }
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

    // `nonce` isn't taken as a constructor argument -- a freshly-`new()`ed
    // awaitable packet always starts with `nonce = 0`. The only way to
    // change it afterward is `set_nonce()`, defined below, so callers can't
    // accidentally construct a packet with a guessed/stale nonce.
    let nonce_default_init = if is_awaitable {
        quote::quote! { let nonce: u64 = 0; }
    } else {
        quote::quote! {}
    };

    // `set_nonce` just updates the field -- `encode()` always writes fields
    // fresh into a caller-supplied buffer, so there's no cached serialized
    // form to patch in place.
    let awaitable_functions = if is_awaitable {
        quote::quote! {
            fn get_nonce(&self) -> Option<u64> {
                Some(self.nonce)
            }

            fn set_nonce(&mut self, nonce: u64) {
                self.nonce = nonce;
            }
        }
    } else {
        quote::quote! {
            fn get_nonce(&self) -> Option<u64> {
                None
            }

            fn set_nonce(&mut self, _nonce: u64) {}
        }
    };

    quote::quote! {
        #ast

        impl #struct_name {
            pub const ID: u8 = #id;
            pub const CAPACITY: usize = #capacity;
            pub const AWAITABLE: bool = #is_awaitable;
            pub const DYNAMIC: bool = #is_dynamic;
        }

        impl Packet for #struct_name {
            fn get_id(&self) -> u8 {
                Self::ID
            }

            // NOTE: this assumes the `Packet` trait's method has been
            // updated to `fn encode(&mut self, buf: &mut BytesMut) -> io::Result<Bytes>;`
            // in place of the old no-argument `get_bytes`. Update the trait
            // definition if it doesn't already look like this.
            fn encode(&mut self, buf: &mut BytesMut) -> io::Result<Bytes> {
                buf.put_u8(Self::ID);

                #(#field_writers)*

                #dynamic_buffer_encode

                Ok(buf.split().freeze())
            }

            fn is_awaitable(&self) -> bool {
                Self::AWAITABLE
            }

            fn get_size(&self) -> usize {
                Self::CAPACITY + (self.get_dynamic_size() as usize)
            }

            #dynamic_functions
            #awaitable_functions
        }

        #dynamic_impl
        #awaitable_impl

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
            /// need the wire bytes. `nonce` (if this packet is awaitable)
            /// is not a parameter: it starts at 0 and must be set
            /// afterward via `set_nonce()`.
            pub fn new(#(#ctor_params),*) -> Self {
                #dynamic_size_from_payload
                #nonce_default_init

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