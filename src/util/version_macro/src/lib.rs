use proc_macro::TokenStream;
use quote::quote;

#[proc_macro]
pub fn packed_version(_input: TokenStream) -> TokenStream {
    let version = std::env::var("CARGO_PKG_VERSION")
        .expect("CARGO_PKG_VERSION not set — must be invoked during a cargo build");

    let mut parts = version.split('.');

    let major: u32 = parts.next().unwrap_or("0").parse().unwrap_or(0);
    let minor: u32 = parts.next().unwrap_or("0").parse().unwrap_or(0);

    // patch may have a pre-release suffix e.g. "3-alpha" — strip anything after the digits
    let patch: u32 = parts
        .next()
        .unwrap_or("0")
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);

    let packed: u32 = (major << 20) | (minor << 10) | patch;

    TokenStream::from(quote! { #packed })
}
