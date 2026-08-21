// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{self, format};

/// `ResultType` Enum
///
/// This enum represents a versatile data structure designed to encapsulate multiple types
/// including basic data types, collections, and even error messages.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum ResultType {
    /// Represents a key-value structure, where values can be of `ResultType` itself.
    Map(HashMap<String, ResultType>),
    /// Represents an ordered list of `ResultType` values.
    List(Vec<ResultType>),
    /// Represents a simple string.
    Str(String),
    /// Represents an integer value.
    Int(i64),
    /// Represents a floating-point number.
    Float(f64),
    /// Represents a boolean value.
    Bool(bool),
    /// Represents an absence of a value.
    Empty,
    /// Represents an error message.
    /// Assumption: The error variant holds a String detailing the error.
    Error(String),
}

/// Display Trait Implementation for `ResultType`
///
/// This allows for a user-friendly representation of the `ResultType` enum variants.
impl fmt::Display for ResultType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ResultType::Empty => write!(f, "Empty"),
            ResultType::Str(s) => write!(f, "\"{}\"", s),
            ResultType::Int(i) => write!(f, "{}", i),
            ResultType::Float(fl) => write!(f, "{}", fl),
            ResultType::Bool(b) => write!(f, "{}", b),
            ResultType::List(list) => {
                write!(f, "[")?;
                for (index, item) in list.iter().enumerate() {
                    if index != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            ResultType::Map(map) => {
                write!(f, "{{")?;
                let mut first = true;
                for (key, value) in map {
                    if !first {
                        write!(f, ", ")?;
                    }
                    write!(f, "\"{}\": {}", key, value)?;
                    first = false;
                }
                write!(f, "}}")
            }
            ResultType::Error(err) => write!(f, "Error: {}", err),
        }
    }
}

/// Utility Methods for `ResultType`
///
/// These methods allow for extracting data from the `ResultType` enum by providing
/// type-specific getter methods.
impl ResultType {
    /// Attempts to extract the `Map` variant.
    pub fn to_map(&self) -> Option<HashMap<String, ResultType>> {
        if let ResultType::Map(ref map) = &self {
            Some(map.clone())
        } else {
            None
        }
    }

    /// Attempts to extract the `List` variant.
    pub fn to_list(&self) -> Option<Vec<ResultType>> {
        if let ResultType::List(ref list) = &self {
            Some(list.clone())
        } else {
            None
        }
    }

    /// Attempts to extract the `Str` variant.
    pub fn to_str(&self) -> Option<String> {
        if let ResultType::Str(ref s) = &self {
            Some(s.clone())
        } else {
            None
        }
    }

    /// Attempts to extract the `Int` variant.
    pub fn to_int(&self) -> Option<i64> {
        if let ResultType::Int(i) = &self {
            Some(*i)
        } else {
            None
        }
    }

    /// Attempts to extract the `Float` variant.
    pub fn to_float(&self) -> Option<f64> {
        if let ResultType::Float(f) = &self {
            Some(*f)
        } else {
            None
        }
    }

    /// Attempts to extract the `Bool` variant.
    pub fn to_bool(&self) -> Option<bool> {
        if let ResultType::Bool(b) = &self {
            Some(*b)
        } else if let ResultType::Int(b) = &self {
            match b {
                1i64 => Some(true),
                0i64 => Some(false),
                _ => None,
            }
        } else {
            None
        }
        // TODO >>> Add String to Bool
        // TODO >>> Add unsigned int to Bool
    }

    /// Attempts to extract the `Error` variant.
    pub fn to_error(&self) -> Option<String> {
        if let ResultType::Error(ref err) = &self {
            Some(err.clone())
        } else {
            None
        }
    }
}

/// `ExpectationError` Enum
///
/// This enum represents the types of errors that can occur when verifying
/// the structure and types of `ResultType` instances.
pub enum ExpectationError {
    /// Occurs when a required keyword argument is missing.
    Missingkwarg(String),
    /// Occurs when there's a type mismatch between the target and the current type.
    MismatchType(String),
    /// Occurs when the target for comparison is empty.
    TargetIsEmpty,
    /// Occurs when the relative lengths between a list and its target are different.
    MismatchRelativeLength,
}

/// To an given case:
///
/// ```ignore
/// ResultType::List(Vec(ResultType::List(Vec(ResultType::Float, ResultType::Int)), ResultType::Int))
/// ```
///
/// We may want to try to iterate recursivelly into the List checking its types and if some of the types dont match we can return a bool
/// The structure here will be like a tree, we will ramificate the tests and we can also add some multithreading to help with large data
/// Checking.
impl ResultType {
    /// Helper function to get the type of the current `ResultType` variant as a string.
    ///
    /// This makes it easier to handle type mismatches by providing a human-readable
    /// type name.
    pub fn type_of(&self) -> &'static str {
        match self {
            ResultType::Map(_) => "Map",
            ResultType::List(_) => "List",
            ResultType::Str(_) => "Str",
            ResultType::Int(_) => "Int",
            ResultType::Float(_) => "Float",
            ResultType::Bool(_) => "Bool",
            ResultType::Empty => "Empty",
            ResultType::Error(_) => "Error",
        }
    }

    /// Quickly verifies the structure and types of the current `ResultType` against a target.
    ///
    /// And then convert based in the target equivalent tipes, like int(1) == Bool(True)
    /// or like int(0) == Bool(False), this allow users to don't need to pass exactly the required type.
    /// This works similar to python Eval() but based in a target patterns to don'v convert things that
    /// we can't conver like floats into bool or ints into list, etc..
    ///
    /// # Arguments
    ///
    /// * `target` - The `ResultType` instance that represents the expected structure and types.
    ///
    /// # Returns
    ///
    /// * new parsed `ResultType` if the current instance matches the target in both structure and types.
    /// * a `Err(ExpectationError)` if there's any mismatch.
    pub fn fast_parse(&self, target: &ResultType) -> Result<ResultType, ExpectationError> {
        match (self, target) {
            // Check if the Maps have matching keys and types.
            (ResultType::Map(map), ResultType::Map(target_map)) => {
                if target_map.is_empty() {
                    return Ok(ResultType::Map(map.clone())); // Skip because if the target is empt the list does not require any args
                }

                let mut new_map: HashMap<String, ResultType> = HashMap::new();

                for (tk, tv) in target_map {
                    // Case where the map doesn't contain the expected key
                    if !&map.contains_key(tk) {
                        return Err(ExpectationError::Missingkwarg(
                            format!("{}:{}", tk.clone(), tv.clone()).to_string(),
                        ));
                    }

                    // Check if inner ResultsTypes are correct and then insert the update one into the new map
                    new_map.insert(tk.clone(), map.get(tk).unwrap().fast_parse(tv)?);
                }

                return Ok(ResultType::Map(new_map));
            }

            // Check if Lists have matching elements and types.
            (ResultType::List(list), ResultType::List(target_list)) => {
                if target_list.is_empty() {
                    return Ok(ResultType::List(list.to_vec())); // Skip because if the target is empt the list does not require any args
                }

                if list.len() != target_list.len() {
                    return Err(ExpectationError::MismatchRelativeLength);
                }

                let mut new_list: Vec<ResultType> = vec![];

                // Otherwise, we'll use the first entry in target_map as the example structure for all values in list.
                for (i, element) in list.iter().enumerate() {
                    new_list.push(element.fast_parse(&target_list[i])?);
                }

                return Ok(ResultType::List(new_list));
            }

            // Special case: self is Int and target is Bool
            (ResultType::Int(i), ResultType::Bool(_)) => {
                match *i {
                    1 => Ok(ResultType::Bool(true)),  // Consider 1 as true
                    0 => Ok(ResultType::Bool(false)), // Consider 0 as false
                    _ => Err(ExpectationError::MismatchType(format!(
                        "get: Int({}), expecting: Bool",
                        i
                    ))),
                }
            }

            // Special case: self is Str("1" or "0") and target is Bool
            (ResultType::Str(s), ResultType::Bool(_)) => {
                match s.as_str() {
                    "1" => Ok(ResultType::Bool(true)),  // Consider "1" as true
                    "0" => Ok(ResultType::Bool(false)), // Consider "0" as false
                    _ => Err(ExpectationError::MismatchType(format!(
                        "get: Str({}), expecting: Bool",
                        s
                    ))),
                }
            }

            // Special case: self is Str("i64") and target is Int
            (ResultType::Str(s), ResultType::Int(_)) => match s.parse::<i64>() {
                Ok(i) => Ok(ResultType::Int(i)),
                Err(_) => Err(ExpectationError::MismatchType(format!(
                    "get: Str({}), expecting: Int",
                    s
                ))),
            },

            // Special case: self is Str("f64") and target is Float
            (ResultType::Str(s), ResultType::Float(_)) => match s.parse::<f64>() {
                Ok(f) => Ok(ResultType::Float(f)),
                Err(_) => Err(ExpectationError::MismatchType(format!(
                    "get: Str({}), expecting: Float",
                    s
                ))),
            },

            // For other types, just check if the types match.
            _ => {
                if std::mem::discriminant(self) == std::mem::discriminant(target) {
                    Ok(self.clone())
                } else {
                    Err(ExpectationError::MismatchType(format!(
                        "get: {}, expecting: {}",
                        self.type_of().to_string(),
                        target.type_of()
                    )))
                }
            }
        }
    }

    /// Quickly verifies the structure and types of the current `ResultType` against a target.
    ///
    /// This function performs a recursive check of nested structures (like maps and lists)
    /// to ensure that the current instance matches the expected structure of the target.
    /// If the structures match but the types within them differ, an error is returned.
    ///
    /// # Arguments
    ///
    /// * `target` - The `ResultType` instance that represents the expected structure and types.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the current instance matches the target in both structure and types.
    /// * `Err(ExpectationError)` if there's any mismatch.
    pub fn fast_verify_kwargs_and_types(
        &self,
        target: &ResultType,
    ) -> Result<(), ExpectationError> {
        match (self, target) {
            // Check if the Maps have matching keys and types.
            (ResultType::Map(map), ResultType::Map(target_map)) => {
                if target_map.is_empty() {
                    return Ok(()); // Skip because if the target is empt the list does not require any args
                }

                for (tk, tv) in target_map {
                    // Case where the map doesn't contain the expected key
                    if !&map.contains_key(tk) {
                        return Err(ExpectationError::Missingkwarg(
                            format!("{}:{}", tk.clone(), tv.clone()).to_string(),
                        ));
                    }

                    // Check if inner ResultsTypes are correct
                    map.get(tk).unwrap().fast_verify_kwargs_and_types(tv)?;
                }

                return Ok(());
            }

            // Check if Lists have matching elements and types.
            (ResultType::List(list), ResultType::List(target_list)) => {
                // TODO >>> Possible implement a Str List ResultType to handle cases where all in the list are indead Strings
                if target_list.is_empty() {
                    return Ok(()); // Skip because if the target is empt the list does not require any args
                }

                if list.len() != target_list.len() {
                    return Err(ExpectationError::MismatchRelativeLength);
                }

                // Otherwise, we'll use the first entry in target_map as the example structure for all values in list.
                for (i, element) in list.iter().enumerate() {
                    element.fast_verify_kwargs_and_types(&target_list[i])?;
                }

                return Ok(());
            }

            // For other types, just check if the types match.
            _ => {
                if std::mem::discriminant(self) == std::mem::discriminant(target) {
                    return Ok(());
                } else {
                    return Err(ExpectationError::MismatchType(format!(
                        "get: {}, expecting: {}",
                        self.type_of().to_string(),
                        target.type_of()
                    )));
                }
            }
        }
    }
}
