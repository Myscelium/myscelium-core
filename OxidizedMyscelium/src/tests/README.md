Use:

```shell
cargo test
```

To test the program! *No setup to test the core*

# More advanced instructions:

By default, Rust will run tests in documentation comments (doc tests) when you execute `cargo test`. If you want to exclude doc tests from being run, you can use the `--lib` or `--bin` flags to run only library or binary tests, excluding doc tests.

Here are some ways to run your tests without running doc tests:

### 1. Run Only Library Tests

If you want to run only the tests in your library (i.e., tests in your `src` and `tests` directories), you can use the `--lib` flag:

```sh
cargo test --lib
```

### 2. Run Only Binary Tests

If you have binaries and you want to run only their tests, you can use the `--bin` flag followed by the binary name:

```sh
cargo test --bin your_binary_name
```

### 3. Run Specific Tests

You can run specific tests or test modules by specifying their names:

```sh
cargo test your_test_name
```

### 4. Exclude Doc Tests with Environment Variable

As of now, there isn't a built-in Cargo flag to exclude doc tests directly. However, you can conditionally compile your doc tests using environment variables. For example:

In your documentation comments, you can add a condition to include the doc tests only when a specific environment variable is set:

```rust
/// This is a documented function.
/// 
/// ```
/// # #[cfg(doc_tests)]
/// assert_eq!(2 + 2, 4);
/// ```
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}
```

Then, set the environment variable when you want to run doc tests:

```sh
RUSTFLAGS="--cfg doc_tests" cargo test
```

By default, running `cargo test` without setting the environment variable will skip these doc tests.

### Summary

To run tests without including doc tests, the most straightforward approach is to use the `--lib` flag to run only the library tests:

```sh
cargo test --lib
```

This command ensures that only the tests in your `src` and `tests` directories are executed, excluding any doc tests.