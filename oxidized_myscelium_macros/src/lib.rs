extern crate proc_macro;
use proc_macro::TokenStream;
use quote::ToTokens;
use quote::{format_ident, quote};
use syn::{parse_macro_input, AttributeArgs, Error, FnArg, ItemFn, Lit, Meta, NestedMeta, Pat, PatType};

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
// #[proc_macro_attribute]
// pub fn callback(attr: TokenStream, input: TokenStream) -> TokenStream {
//     let args = parse_macro_input!(attr as AttributeArgs);
//     let input_fn = parse_macro_input!(input as ItemFn);
//     let fn_name = &input_fn.sig.ident;
//     let fn_name_str = fn_name.to_string();

//     let mut node_name: Option<String> = None;
//     for arg in args {
//         match arg {
//             NestedMeta::Meta(Meta::NameValue(nv)) if nv.path.is_ident("node") => {
//                 if let Lit::Str(ref s) = nv.lit {
//                     node_name = Some(s.value());
//                 } else {
//                     return Error::new_spanned(nv.lit, "Expected a string literal for the node name").to_compile_error().into();
//                 }
//             },
//             _ => {},
//         }
//     }

//     let node_name = match node_name {
//         Some(name) => name,
//         None => {
//             let error = Error::new_spanned(&input_fn.sig.ident, "Node name must be specified using #[callback(node=\"node_name\")]").to_compile_error();
//             return error.into();
//         },
//     };

//     let args_insertion = input_fn.sig.inputs.iter().map(|arg| {
//         if let syn::FnArg::Typed(syn::PatType { pat, ty, .. }) = arg {
//             let arg_name = match **pat {
//                 syn::Pat::Ident(ref ident) => ident.ident.to_string(),
//                 _ => panic!("Unsupported pattern"),
//             };
//             let arg_type = quote!(#ty).to_string();
//             quote! { args_types_value.insert(#arg_name.to_string(), #arg_type.to_string()); }
//         } else {
//             quote! {}
//         }
//     });

//     let registration_code = quote! {
//         {
//             let mut args_types_value = indexmap::IndexMap::new();
//             #(#args_insertion)*

//             let closure = Box::new(move |args: Vec<Box<dyn std::any::Any + 'static>>| -> Box<dyn std::any::Any> {
//                 Box::new(#fn_name(/* Extract and pass arguments here */))
//             });

//             use crate::socket_host::transposer::set_socket_host_transposer_callback;
//             set_socket_host_transposer_callback(#fn_name_str.clone(), closure);

//             let handler = crate::NodeHandler::new(
//                 #fn_name_str.clone().to_string(),
//                 args_types_value,
//                 crate::CommandType::ExternalFunction,
//                 crate::HandlerStatus::NotTested,
//                 std::collections::HashMap::new(),
//                 "".to_string()
//             );
//             let global_command_patterns = crate::HOST_COMMAND_PATTERNS.lock();
//             let node_version = crate::NodeVersion::cast_version(1, 3, 0, crate::VersionIndentifier::ReleaseCandidate);
//             let host_node = crate::Node::new(#node_name.clone().to_string(), #node_name.to_string(), "".to_string(), node_version, vec![handler], crate::NodeStatus::Online);
//             global_command_patterns.add_or_update_if_exists(host_node);
//         }
//     };

//     let output = quote! {
//         #input_fn

//         fn #fn_name_register() {
//             #registration_code
//         }

//         lazy_static::lazy_static! {
//             static ref _REGISTER: () = {
//                 #fn_name_register();
//             };
//         }
//     };

//     output.into()
// }

// -> Working:
// #[proc_macro_attribute]
// pub fn callback(_attr: TokenStream, input: TokenStream) -> TokenStream {
//     let input_fn = parse_macro_input!(input as ItemFn);
//     let fn_name = &input_fn.sig.ident;
//     let fn_args = &input_fn.sig.inputs;

//     // Handle function arguments and generate appropriate code snippets
//     let unpack_args = fn_args.iter().enumerate().map(|(i, arg)| {
//         let pat = match arg {
//             syn::FnArg::Typed(pat) => pat,
//             _ => panic!("Unsupported function argument type"),
//         };
//         let ident = match &*pat.pat {
//             syn::Pat::Ident(ident) => &ident.ident,
//             _ => panic!("Unsupported pattern in function arguments"),
//         };
//         let ty = &pat.ty;
//         quote! {
//             let #ident: &#ty = args[#i].downcast_ref::<#ty>().expect("Argument type mismatch");
//         }
//     });

//     // Create the function call with unpacked arguments
//     let call_args = fn_args.iter().map(|arg| {
//         let pat = match arg {
//             syn::FnArg::Typed(pat) => pat,
//             _ => panic!("Unsupported function argument type"),
//         };
//         let ident = match &*pat.pat {
//             syn::Pat::Ident(ident) => &ident.ident,
//             _ => panic!("Unsupported pattern in function arguments"),
//         };
//         quote! { #ident }
//     });

//     // Compose the entire function
//     let output = quote! {
//         pub fn #fn_name() -> Box<dyn Fn(Vec<Box<dyn std::any::Any>>) -> Box<dyn std::any::Any> + Send + Sync> {
//             Box::new(move |args: Vec<Box<dyn std::any::Any>>| -> Box<dyn std::any::Any> {
//                 #(#unpack_args)*
//                 let result = {
//                     #input_fn
//                     #fn_name(#(#call_args),*)
//                 };
//                 Box::new(result)
//             })
//         }
//     };

//     output.into()
// }

use indexmap::IndexMap;
use std::any::Any;

// -> Extract metadata:
#[proc_macro_attribute]
pub fn callback(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let fn_name = &input.sig.ident;

    let args = input
        .sig
        .inputs
        .iter()
        .enumerate()
        .map(|(i, arg)| match arg {
            syn::FnArg::Typed(pat) => {
                let arg_name = match &*pat.pat {
                    syn::Pat::Ident(ident) => ident.ident.to_string(),
                    _ => format!("arg{}", i),
                };
                let arg_type = quote! {#pat.ty}.to_string().replace(".ty", "");
                let arg_type = arg_type.replace(' ', ""); // Removing spaces for better readability
                quote! {
                    (#arg_name.to_string(), #arg_type.to_string())
                }
            },
            _ => panic!("Unsupported argument type"),
        })
        .collect::<Vec<_>>();

    let arg_names = input
        .sig
        .inputs
        .iter()
        .enumerate()
        .map(|(i, arg)| match arg {
            syn::FnArg::Typed(pat) => {
                let arg_name = match &*pat.pat {
                    syn::Pat::Ident(ident) => ident.ident.clone(),
                    _ => format_ident!("arg{}", i),
                };
                let arg_type = &pat.ty;
                quote! {
                    let #arg_name: #arg_type = *args_iter.next().unwrap().downcast_ref::<#arg_type>().unwrap();
                }
            },
            _ => panic!("Unsupported argument type"),
        })
        .collect::<Vec<_>>();

    let fn_body = &input.block;

    let gen = quote! {
        pub fn #fn_name() -> FunctionMetadata {
            let func: CallbackClosure = Box::new(|args: Vec<Box<dyn Any + 'static>>| -> Box<dyn Any> {
                let mut args_iter = args.into_iter();

                #(#arg_names)*

                let result = { #fn_body };

                Box::new(result)
            });

            FunctionMetadata {
                name: stringify!(#fn_name),
                args: vec![
                    #(#args),*
                ].into_iter().collect(),
                func,
            }
        }
    };

    gen.into()
}
