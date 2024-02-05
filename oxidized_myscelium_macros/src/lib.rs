extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

// use oxidized_myscelium_core::host_entry_point::registry_socket_host_callbacks;
// use host_entry_point::set_socket_client_transposer_callbacks;

// TODO >>> client_callback proc macro

// macro_rules! initialize_dependencies {
//     () => {
//         use oxidized_myscelium_core::host_entry_point::registry_socket_host_callbacks;
//     };
// }

// #[proc_macro]
// pub fn host_callback(input: TokenStream) -> TokenStream {
//     let input = parse_macro_input!(input as ItemFn);
//     let fn_name = &input.sig.ident;

//     let output = quote! {
//         #input

//         // This code will be generated at compile time and will be executed when the binary runs
//         {
//             let commands_patterns = std::collections::HashMap::new();
//             let mut callbacks_patterns = std::collections::HashMap::new();
//             callbacks_patterns.insert(stringify!(#fn_name), Box::new(#fn_name) as Box<dyn Fn() + Send + Sync + 'static>);
//             registry_socket_host_callbacks(commands_patterns, callbacks_patterns);
//         }
//     };

//     output.into()
// }

#[proc_macro]
pub fn client_callback(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemFn);
    let fn_name = &input.sig.ident;

    let output = quote! {
        #input

        // This code will be generated at compile time and will be executed when the binary runs
        {
            let fn_closure = Box::new(#fn_name) as CallbackClosure;
            OxidizedMyscelium::client::GLOBAL_CALLBACKS.insert(stringify!(#fn_name), fn_closure);
        }
    };

    output.into()
}

#[proc_macro]
pub fn host_callback(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemFn);
    let fn_name = &input.sig.ident;

    let output = quote! {
        #input

        // This code will be generated at compile time and will be executed when the binary runs
        {
            let fn_closure = Box::new(#fn_name) as CallbackClosure;
            OxidizedMyscelium::host::GLOBAL_CALLBACKS.insert(stringify!(#fn_name), fn_closure);
        }
    };

    output.into()
}

// #[proc_macro]
// pub fn host_callback_with_global(input: TokenStream) -> TokenStream {
//     // Accept an additional argument for GLOBAL_CALLBACKS
//     let global_callbacks: syn::Ident = syn::parse2(quote! { GLOBAL_CALLBACKS }).unwrap();

//     let input = parse_macro_input!(input as ItemFn);
//     let fn_name = &input.sig.ident;

//     let output = quote! {
//         #input

//         // This code will be generated at compile time and will be executed when the binary runs
//         {
//             let fn_closure = Box::new(#fn_name) as CallbackClosure;
//             #global_callbacks.insert(stringify!(#fn_name), fn_closure);
//         }
//     };

//     output.into()
// }
