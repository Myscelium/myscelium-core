#[macro_export]
macro_rules! process_commands {
    ($py:expr, $commands:expr, $callback_pattern:expr) => {
        for command in $commands.iter() {
            let command_dict: &PyDict = command.downcast().unwrap();
            let function: &PyAny = command_dict.get_item("function").unwrap();

            let args_item: &PyAny = command_dict.get_item("args").unwrap();

            // Check if args_item is a dict or a string with the value "None"
            let args_dict: Option<&PyDict>;

            if let Ok(args_as_dict) = args_item.downcast::<PyDict>() {
                args_dict = Some(args_as_dict);
            } else if let Ok(args_as_str) = args_item.extract::<String>() {
                if args_as_str == "None" {
                    args_dict = None;
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("args must be a dict or the string 'None'"));
                }
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>("args must be a dict or the string 'None'"));
            }

            // Extract the Python function name
            let function_name: &str = function.getattr("__name__")?.extract()?;

            // Extract the argument types
            let args_types_value;
            if let Some(args_dict) = args_dict {
                args_types_value = extract_arg_types(args_dict)?;
            } else {
                args_types_value = Value::Array(Vec::new()); // or whatever default value you want to use
            }

            let function = function.downcast::<PyFunction>()?.clone();

            let function: Py<PyFunction> = function.into_py($py); // convert &PyAny to Py<PyFunction>
            $callback_pattern.insert(function_name.to_string(), (function, args_types_value));
        }
    };
}
