fn main() {
    println!("cargo:rerun-if-changed=src/ffi.rs");
 
    csbindgen::Builder::default()
        .input_extern_file("src/ffi.rs")
        // Must match the `[lib] name = "..."` in Cargo.toml (the .dll/.so
        // filename, without extension/prefix) — this is what goes into
        // [DllImport("...")] on the C# side.
        .csharp_dll_name("RustyRelay")
        .csharp_namespace("RelayClient.Interop")
        .csharp_class_name("RelayClientNative")
        // `nint`/`nuint` are C# 9 / .NET 5+ keywords and don't exist on
        // .NET 3.5's compiler at all. This switches the generated bindings
        // to IntPtr/UIntPtr instead, which have existed since .NET 1.0.
        .csharp_use_nint_types(false)
        // .csharp_use_function_pointer(false) // leave default off; function
        // pointers aren't available pre-C# 9 / .NET 5, and we don't have any
        // callbacks in ffi.rs yet anyway.
        .generate_csharp_file("bindings/RelayClientNative.g.cs")
        .unwrap();
}
