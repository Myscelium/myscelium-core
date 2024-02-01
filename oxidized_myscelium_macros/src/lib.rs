extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

use oxidized_myscelium_core::host_entry_point::registry_socket_host_callbacks;

// use host_entry_point::set_socket_client_transposer_callbacks;

#[proc_macro]
pub fn set_socket_callback(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as ItemFn);
    let fn_name = &input.sig.ident;

    let output = quote! {
        #input

        // This code will be generated at compile time and will be executed when the binary runs
        {
            let commands_patterns = std::collections::HashMap::new();
            let mut callbacks_patterns = std::collections::HashMap::new();
            callbacks_patterns.insert(stringify!(#fn_name), Box::new(#fn_name) as Box<dyn Fn() + Send + Sync + 'static>);
            registry_socket_host_callbacks(commands_patterns, callbacks_patterns);
        }
    };

    output.into()
}
