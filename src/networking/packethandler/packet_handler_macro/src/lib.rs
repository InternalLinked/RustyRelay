use proc_macro::TokenStream;
use quote::quote;
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
        quote! {
            <#packet>::ID => {
                if #bytes.len() < <#packet>::CAPACITY {
                    // Not enough bytes yet for even the fixed part of this
                    // packet. Leave everything where it is (it's already at
                    // the front of the buffer) and wait for the next read.
                    return PacketResponse::None;
                }
 
                let packet_total_size: usize = if <#packet>::DYNAMIC {
                    let dynamic_size: usize = {
                        let cursor: usize = if <#packet>::AWAITABLE { 9 } else { 1 };
                        usize::from_be_bytes(#bytes[cursor..cursor + 8].try_into().unwrap())
                    };
 
                    let total_size = <#packet>::CAPACITY + dynamic_size;
 
                    if #bytes.len() < total_size {
                        return PacketResponse::None;
                    }
 
                    total_size
                } else {
                    <#packet>::CAPACITY
                };
 
                // Zero-copy: shares the same underlying allocation as the
                // read buffer. `packet_buf` owns just the front
                // `packet_total_size` bytes; whatever's left stays in the
                // handle's buffer for the next packet.
                let mut packet_buf: ::bytes::Bytes = #bytes.split_to(packet_total_size);
                let packet = <#packet>::from(&mut packet_buf).unwrap();
                return PacketResponse::#packet(packet);
            }
        }
    });
 
    quote! {
        {
            if #bytes.is_empty() {
                return PacketResponse::Kill;
            }

            let id: u8 = #bytes[0];

            match id {
                #(#arms)*
                _ => {
                    println!("[Server] Unknown packet {}", id);
                    println!("{:?}", #bytes);
                    return PacketResponse::Kill;
                }
            }
        }
    }
    .into()
}


struct DispatchInputImpl {
    handle: Expr,
    packets: Punctuated<Type, Token![,]>,
}
 
impl Parse for DispatchInputImpl {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let handle: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let packets = Punctuated::parse_terminated(input)?;
        Ok(DispatchInputImpl { handle, packets })
    }
}

struct DispatchInputServer {
    bytes: Expr,
    client_handle: Expr,
    packets: Punctuated<Type, Token![,]>,
}

impl Parse for DispatchInputServer {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let bytes: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let client_handle: Expr = input.parse()?;
        input.parse::<Token![,]>()?;
        let packets = Punctuated::parse_terminated(input)?;
        Ok(DispatchInputServer { bytes, client_handle, packets })
    }
}
 
#[proc_macro]
pub fn dispatch_packet_server_impl(input: TokenStream) -> TokenStream {
    let DispatchInputServer { bytes, client_handle, packets } = parse_macro_input!(input as DispatchInputServer);
 
    let arms = packets.iter().map(|packet| {
        quote! {
            <#packet>::ID => {
                if #bytes.len() < <#packet>::CAPACITY {
                    // Not enough bytes yet for even the fixed part of this
                    // packet. Leave everything where it is (it's already at
                    // the front of the buffer) and wait for the next read.
                    break;
                }
 
                let packet_total_size: usize = if <#packet>::DYNAMIC {
                    let dynamic_size: usize = {
                        let cursor: usize = if <#packet>::AWAITABLE { 9 } else { 1 };
                        usize::from_be_bytes(#bytes[cursor..cursor + 8].try_into().unwrap())
                    };
 
                    let total_size = <#packet>::CAPACITY + dynamic_size;
 
                    if #bytes.len() < total_size {
                        break;
                    }
 
                    total_size
                } else {
                    <#packet>::CAPACITY
                };
 
                // `split_to` shares the same underlying allocation as the
                // read buffer (zero-copy) -- `.freeze()` just converts the
                // resulting `BytesMut` into the read-only `Bytes` that
                // `from` now expects, still zero-copy. Whatever's left in
                // the read buffer stays there for the next packet.
                let mut packet_buf: ::bytes::Bytes = #bytes.split_to(packet_total_size).freeze();
                let packet = <#packet>::from(&mut packet_buf).unwrap();
                packet.on_trigger_server(#client_handle);
            }
        }
    });
 
    quote! {
        {
            
            loop {
                if #bytes.is_empty() {
                    break;
                }
 
                let id: u8 = #bytes[0];
 
                match id {
                    #(#arms)*
                    _ => {
                        println!("[Server] Unknown packet {}", id);
                        println!("{:?}", #bytes);
                        return ServerCallback::Kill;
                    }
                }
            }
            ServerCallback::Pass
        }
    }
    .into()
}
 
 
#[proc_macro]
pub fn dispatch_packet_client_impl(input: TokenStream) -> TokenStream {
    let DispatchInputImpl { handle, packets } = parse_macro_input!(input as DispatchInputImpl);
 
    let arms = packets.iter().map(|packet| {
 
        quote! {
            <#packet>::ID => {
                if __handle.get_buffer().len() < <#packet>::CAPACITY {
                        // Not enough bytes yet for even the fixed part of this
                        // packet. Leave everything where it is (it's already at
                        // the front of the buffer) and wait for the next read.
                        break;
                    }
 
                let packet_total_size: usize = if <#packet>::DYNAMIC {
                    let dynamic_size: usize = {
                        let buffer = __handle.get_buffer();
                        let cursor: usize = if <#packet>::AWAITABLE { 9 } else { 1 };
                        usize::from_be_bytes(buffer[cursor..cursor + 8].try_into().unwrap())
                    };
 
                    let total_size = <#packet>::CAPACITY + dynamic_size;
 
                    if __handle.get_buffer().len() < total_size {
                        break;
                    }
 
                    total_size
                } else {
                    <#packet>::CAPACITY
                };

                let mut packet_buf: ::bytes::Bytes =
                    __handle.get_buffer_mut().split_to(packet_total_size).freeze();
                let packet = <#packet>::from(&mut packet_buf).unwrap();

                let server_instance: &mut ServerInstance = __handle.get_server_instance();
                packet.on_trigger_client(server_instance);
            }
        }
    });
 
    quote! {
        {
            let __handle = #handle;
            loop {
                if __handle.get_buffer().is_empty() {
                    break;
                }
 
                let id: u8 = __handle.get_buffer()[0];
 
                match id {
                    #(#arms)*
                    _ => {
                        println!("[Client] Unknown packet {}", id);
                        println!("{:?}", __handle.get_buffer());
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
    }
    .into()
}