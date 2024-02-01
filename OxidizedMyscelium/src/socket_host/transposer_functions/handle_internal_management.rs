// use serde_json::Value;
// use std::collections::HashMap;

// use crate::common::functions::converters::convert_to_value_map;

// use crate::common::functions::verifiers::{fast_json_comparator, ComparatorError};
// use crate::common::structs::results_structs::ResultType;

// use crate::common::enhanced_buffer::utilities::{Command, CommandInstructions, CommandMode, CommandOrigin, CommandStatus, CommandTarget, CommandType};
// use crate::socket_host::client_manager::manager::{Client, ClientError};
// use crate::socket_host::transposer_functions::handle_direct_function::ProcessResult;

// use crate::handle_client_error;

// use crate::common::structs::results_structs::ExpectationError;

// macro_rules! create_error_response_and_return {
//     ($error:expr) => {{
//         let new_command_instructions = CommandInstructions::new(
//             CommandMode::Response,
//             CommandType::DirectFunction,
//             CommandTarget::Origin,
//             CommandStatus::Failure,
//             CommandOrigin::Host,
//             "error_handler".to_string(),
//             HashMap::new(),
//             $error.to_string(),
//         );

//         new_command_instructions
//     }};
// }

// use crate::socket_host::host_logger::log_handler::Logger;
// use crate::HOST_LOG_LEVEL;

// macro_rules! acquire_logger {
//     ($section_name:expr) => {{
//         let host_log_level;
//         {
//             let log_level = HOST_LOG_LEVEL.lock();
//             host_log_level = log_level.clone()
//         }
//         Logger::new(host_log_level, $section_name)
//     }};
// }

// // > --------------------------------------------------------------------------------------------------------------------------------------------
// // > Internal Management Handler

// fn cast_new_client(new_client: Value) -> Result<Client, CommandInstructions> {
//     let mut expected: HashMap<String, Value> = HashMap::new();

//     expected.insert("client_name".to_string(), Value::String("".to_string()));
//     expected.insert("client_key".to_string(), Value::String("".to_string()));
//     expected.insert("client_type".to_string(), Value::String("".to_string()));
//     expected.insert("permission_group".to_string(), Value::String("".to_string()));
//     expected.insert("is_super_user".to_string(), Value::Bool(false));
//     expected.insert("max_sub_channels".to_string(), Value::Number(serde_json::Number::from(0)));
//     expected.insert("owned_sub_channels_keys".to_string(), Value::Array(vec![]));

//     let parsed_new_client = fast_json_comparator(&new_client, &Value::Object(serde_json::Map::from_iter(expected)));

//     let verified_client_value: Value = match parsed_new_client {
//         Err(e) => match e {
//             ComparatorError::TypeMismatch(tp) => {
//                 // logger.warn(format!("ERROR, Client kwargs have mismatch type {} kwarg!", tp));
//                 return Err(create_error_response_and_return!(format!("Error! Client kwargs have mismatch type {} kwarg!", tp)));
//             },
//             ComparatorError::LengthMismatch => {
//                 // logger.warn("ERROR, Client kwargs have mismatch relative length kwargs!".to_string());
//                 return Err(create_error_response_and_return!("Error! Client kwargs have mismatch relative length kwargs!"));
//             },
//             ComparatorError::MissingKey(k) => {
//                 // logger.warn(format!("ERROR, Client kwargs have a missing kwarg: {}!", k));
//                 return Err(create_error_response_and_return!(format!("Error! Client kwargs have a missing kwarg: {}!", k)));
//             },
//             ComparatorError::TargetIsEmpty => {
//                 // logger.warn("ERROR, Client target pattern is empty!".to_string());
//                 return Err(create_error_response_and_return!("Error! Client target pattern is empty!"));
//             },
//             ComparatorError::ParseError(e) => {
//                 // logger.warn(format!("ERROR, Can't parse {:?}!", e).to_string());
//                 return Err(create_error_response_and_return!("Error! Client target pattern is empty!"));
//             },
//         },

//         Ok(new_client) => new_client,
//     };

//     // let owned_sub_channels_keys: Vec<String> = verified_client_value.get("owned_sub_channels_keys").unwrap().as_array().unwrap().iter().map(|v| v.as_str().unwrap()).collect();

//     let owned_sub_channels_keys_result: Result<Vec<String>, _> = verified_client_value
//         .get("owned_sub_channels_keys")
//         .ok_or("Key not found")
//         .and_then(|v| v.as_array().ok_or("Not an array"))
//         .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect());

//     let owned_sub_channels_keys: Vec<String> = match owned_sub_channels_keys_result {
//         Ok(keys) => keys,
//         Err(e) => {
//             return Err(create_error_response_and_return!(format!("Error! Can't extract new client owned_sub_channels_keys error: {}!", e)));
//         },
//     };

//     let client_key = verified_client_value.get("client_key").unwrap().as_str().map(|s| s.to_string()).unwrap();

//     let client_handlers: Vec<HashMap<String, Value>> = Vec::new();

//     let new_client = handle_client_error!(Client::new(
//         verified_client_value.get("client_name").unwrap().as_str().map(|s| s.to_string()).unwrap(),
//         client_key.clone(),
//         verified_client_value.get("client_type").unwrap().as_str().map(|s| s.to_string()).unwrap(),
//         verified_client_value.get("permission_group").unwrap().as_str().map(|s| s.to_string()).unwrap(),
//         verified_client_value.get("is_super_user").unwrap().as_bool().unwrap(),
//         verified_client_value.get("max_sub_channels").unwrap().as_u64().unwrap().try_into().unwrap(), // TODO >>> Create a better handler to cases greather than u32
//         owned_sub_channels_keys,
//         client_handlers,
//     ));

//     // logger.debug(format!("New client: {:?}", new_client));

//     Ok(new_client)
// }

// pub fn handle_internal_management(m: &CommandInstructions, client_id: &mut String) -> CommandInstructions {
//     let logger = acquire_logger!("[Process][Internal Management]");

//     let activation_function: String = m.actf.clone();
//     let kwargs: HashMap<String, Value> = m.kwargs.clone();

//     match activation_function.as_str() {
//         "add_client" => {
//             // > edit client
//             // {'response_mode':'InternalManagement', 'activation_function':'add_client', 'kwargs':response, 'response_activation_function':'function_name'}
//             // 'kwargs':{'new_client':clientpattern}

//             if !kwargs.contains_key("client_key") {
//                 logger.warn("Error! Callback response kwargs don't have client_key kwarg!".to_string());
//                 return create_error_response_and_return!("Error! Callback response kwargs don't have client_key kwarg!");
//             }

//             if !kwargs.contains_key("new_client") {
//                 logger.warn("Error! Callback response kwargs don't have new_client kwarg!".to_string());
//                 return create_error_response_and_return!("Error! Callback response kwargs don't have new_client kwarg!");
//             }

//             let client_key = kwargs.get("client_key").unwrap().as_str().unwrap();

//             // from("client_name":"str", "client_key":"str", "client_type":"str", "permission_group":"str", "is_super_user":"bool", "max_sub_channels":"int", "owned_sub_channels_keys":"list")

//             let new_client = match cast_new_client(kwargs.get("new_client").unwrap().clone()) {
//                 Ok(c) => c,
//                 Err(e) => return e,
//             };

//             new_client.save_into_db(); //> It Already create the new client

//             logger.debug("New client saved into the database!".to_string());

//             // TODO >>> Make a verification if the client already exists or not before add it!
//             let mut resp_kwargs: HashMap<String, Value> = HashMap::new();
//             resp_kwargs.insert("client_key".to_string(), Value::String(client_key.to_string()));

//             let new_command_instructions: CommandInstructions = CommandInstructions::new(
//                 CommandMode::Response,
//                 CommandType::ExternalFunction,
//                 CommandTarget::Origin,
//                 CommandStatus::Success,
//                 CommandOrigin::Host,
//                 "add_client_handler".to_string(),
//                 resp_kwargs,
//                 format!("Successfully add a client: {}!", new_client.client_key).to_string(),
//             );

//             logger.info(format!("Successfully add a client: {}!", new_client.client_key));

//             return new_command_instructions;
//         },

//         "update_client" => {
//             // > update client
//             // {'response_mode':'InternalManagement', 'activation_function':'update_client', 'kwargs':response, 'response_activation_function':'function_name'}
//             // 'kwargs':{'actual_client_key':String, 'updated_client':client} // Client have to have the same client key
//             // 'client': {"client_name":str, "client_key":str, "client_type":str, "permission_group":str, "is_super_user":bool, "max_sub_channels":int, "owned_sub_channels_keys":list}

//             logger.debug("Receive a update client inner command!".to_string());

//             if !kwargs.contains_key("actual_client_key") {
//                 logger.warn("Error! Callback response kwargs don't have actual_client_key kwarg!".to_string());
//                 return create_error_response_and_return!("Error! Callback response kwargs don't have actual_client_key kwarg!");
//             }

//             if !kwargs.contains_key("updated_client") {
//                 logger.warn("ERROR, Error! Callback response kwargs don't have update_client kwarg!".to_string());
//                 return create_error_response_and_return!("Error! Callback response kwargs don't have update_client kwarg!");
//             }

//             let actual_client_key = kwargs.get("actual_client_key").unwrap().as_str().unwrap();

//             // from("client_name":"str", "client_key":"str", "client_type":"str", "permission_group":"str", "is_super_user":"bool", "max_sub_channels":"int", "owned_sub_channels_keys":"list")

//             let new_client = match cast_new_client(kwargs.get("updated_client").unwrap().clone()) {
//                 Ok(c) => c,
//                 Err(e) => return e,
//             };

//             let old_client = handle_client_error!(Client::get_by_key(&actual_client_key.to_string()));

//             let result = old_client.update_to(&new_client); //> It already saves into the database

//             // TODO >>> Maybe implement a fast result-ype to client if needed

//             match result {
//                 Ok(_) => {
//                     let mut resp_kwargs: HashMap<String, Value> = HashMap::new();

//                     resp_kwargs.insert("actual_client_key".to_string(), Value::String(actual_client_key.to_string())); // TODO >>> See if this actual client key is correct

//                     let new_command_instructions: CommandInstructions = CommandInstructions::new(
//                         CommandMode::Response,
//                         CommandType::ExternalFunction,
//                         CommandTarget::Origin,
//                         CommandStatus::Success,
//                         CommandOrigin::Host,
//                         "update_client_handler".to_string(),
//                         resp_kwargs,
//                         format!("Successfully executed the function: {} and remove client: {}!", activation_function, old_client.client_key).to_string(),
//                     );

//                     logger.info(format!("Successfully executed the function: {} and remove client: {}!", activation_function, old_client.client_key));

//                     return new_command_instructions;
//                 },

//                 Err(e) => match e {
//                     ClientError::ClientDoesNotExist(e) => {
//                         logger.warn(format!("Error! Can't Update client because client {} Don't exist!", e));
//                         return create_error_response_and_return!(format!("Error! Can't Update client because client {} Don't exist!", e));
//                     },
//                     _ => {
//                         logger.warn("Error! Can Update client because a unexpected error!".to_string());
//                         return create_error_response_and_return!("Error! Can Update client because a unexpected error!");
//                     },
//                 },
//             }

//             // TODO >>> Implement a mechanism to send back the confirmation or a error message originated from the operation
//             // else {
//             //     logger.warn("Error! Callback response kwargs isn't a Map!".to_string());
//             //     return create_error_response_and_return!("Error! Callback response kwargs isn't a Map!", converted_m, to_send);
//             // }
//         },

//         "remove_client" => {
//             // > remove client
//             // {'response_mode':'InternalManagement', 'activation_function':'remove_client', 'kwargs':response, 'response_activation_function':'function_name'}
//             // 'kwargs':{'client_key':String}

//             if !kwargs.contains_key("client_key") {
//                 return create_error_response_and_return!("Error! Callback response kwargs don't have client_key kwarg!");
//             }

//             let client_key: String = kwargs.get("client_key").unwrap().as_str().map(|s| s.to_string()).unwrap();

//             let client = handle_client_error!(Client::get_by_key(&client_key));

//             let result = client.delete();

//             match result {
//                 Err(e) => match e {
//                     ClientError::ClientDoesNotExist(e) => {
//                         logger.warn(format!("Error! Can't Remove client because client {} Don't exist!", e));
//                         return create_error_response_and_return!(format!("Error! Can't Remove client because client {} Don't exist!", e));
//                     },
//                     _ => {
//                         logger.warn("Error! Can Remove client because a unexpected error!".to_string());
//                         return create_error_response_and_return!("Error! Can Remove client because a unexpected error!");
//                     },
//                 },
//                 Ok(_) => {
//                     // let mut resp_kwargs: HashMap<String, Value> = HashMap::new();
//                     // resp_kwargs.insert("actual_client_key".to_string(), Value::String(client_key.to_string())); // TODO >>> See if this actual client key is correct

//                     let new_command_instructions: CommandInstructions = CommandInstructions::new(
//                         CommandMode::Response,
//                         CommandType::ExternalFunction,
//                         CommandTarget::Origin,
//                         CommandStatus::Success,
//                         CommandOrigin::Host,
//                         "remove_client_handler".to_string(),
//                         HashMap::new(),
//                         format!("Successfully executed the function: {} and remove client: {}!", activation_function, client_key).to_string(),
//                     );

//                     logger.info(format!("Successfully executed the function: {} and remove client: {}!", activation_function, client_key));

//                     return new_command_instructions;
//                 },
//             }
//             // else {
//             //     logger.warn("Error! Callback response kwargs isn't a Map!".to_string());
//             //     return create_error_response_and_return!("Error! Callback response kwargs isn't a Map!", converted_m, to_send);
//             // }

//             // TODO >>> Implement a mechanism to send back the confirmation or a error message originated from the operation
//         },

//         _ => {
//             logger.warn(format!("Response Activation Function: {} doesn't exists!!", activation_function));
//             return create_error_response_and_return!(format!("Response Activation Function: {} doesn't exists!!", activation_function).to_string());
//         },
//     }

//     // TODO >>> Add the cases to handle the following internal management things:

//     //* Need to implement the 'response_activation_function' in the wrapper
// }
