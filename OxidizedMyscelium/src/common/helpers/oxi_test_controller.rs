// SPDX-License-Identifier: MPL-2.0
// Copyright © 2021-2026 Cristian Camargo Filho

use std::collections::HashSet;

pub struct TestController {
    tests: HashSet<String>,
    passed: HashSet<String>,
    failed: HashSet<String>,
}

impl TestController {
    pub fn new(tests: HashSet<String>) -> Self {
        Self {
            tests,
            passed: HashSet::new(),
            failed: HashSet::new(),
        }
    }

    pub fn complet(&mut self, test: &'static str, passed: bool) {
        if self.tests.contains(test) {
            if passed {
                self.passed.insert(test.to_string());
            } else {
                self.failed.insert(test.to_string());
            }

            self.tests.remove(test);
        }
        // TODO >>> Make last test call the end teardown function.
    }

    pub fn summary() {
        // TODO >>> Return sumarry of the tests done, how much passed, success rate, passible time
        // spend, and other metricts.
    }
}
