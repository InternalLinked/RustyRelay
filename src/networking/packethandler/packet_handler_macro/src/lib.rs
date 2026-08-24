use proc_macro::TokenStream;
use quote::{quote, format_ident};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    Expr, Token, Type,
};

struct DispatchInput {
    bytes: Expr,
    packets: Punctuated<Type, Token![,]>,
}
 
impl Parse for DispatchInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let bytes: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let packets = Punctuated::parse_terminated(input)?;
        Ok(DispatchInput { bytes, packets })
    }
}
 
#[proc_macro]
pub fn dispatch_packet_impl(input: TokenStream) -> TokenStream {
    let DispatchInput { bytes, packets } = parse_macro_input!(input as DispatchInput);
 
    let arms = packets.iter().map(|packet| {


        let variant_ident = match packet {
            Type::Path(type_path) => {
                let ident = &type_path.path.segments.last().unwrap().ident;
                format_ident!("{}Packet", ident)
            }
            _ => panic!("expected a simple type path"),
        };

        quote! {
            <#packet>::ID => {
                if #bytes.len() < <#packet>::CAPACITY {
                    // Not enough bytes yet for even the fixed part of this
                    // packet. Leave everything where it is (it's already at
                    // the front of the buffer) and wait for the next read.
                    return AnyPacket::None;
                }
 
                let packet_total_size: usize = if <#packet>::DYNAMIC {
                    let dynamic_size: usize = {
                        let cursor: usize = 10;
                        usize::from_be_bytes(#bytes[cursor..cursor + 8].try_into().unwrap())
                    };
 
                    let total_size = <#packet>::CAPACITY + dynamic_size;
 
                    if #bytes.len() < total_size {
                        return AnyPacket::None;
                    }
 
                    total_size
                } else {
                    <#packet>::CAPACITY
                };
 
                // Zero-copy: shares the same underlying allocation as the
                // read buffer. `packet_buf` owns just the front
                // `packet_total_size` bytes; whatever's left stays in the
                // handle's buffer for the next packet.
                let mut packet_buf: ::bytes::Bytes = #bytes.split_to(packet_total_size).freeze();
                let packet = <#packet>::from(&mut packet_buf).unwrap();
                return AnyPacket::#variant_ident(packet);
            }
        }
    });
 
    quote! {
        {
            if #bytes.is_empty() {
                return AnyPacket::None;
            }

            let id: u8 = #bytes[0];

            match id {
                #(#arms)*
                _ => {
                    println!("[Server] Unknown packet {}", id);
                    println!("{:?}", #bytes);
                    return AnyPacket::Kill;
                }
            }
        }
    }
    .into()
}
