#!/bin/bash
git clone -b "$GIT_BRANCH" https://github.com/stackabletech/hive-operator.git
(cd hive-operator/ && ./scripts/run_tests.sh --parallel 1)
exit_code=$?
./operator-logs.sh hive > /target/hive-operator.log
exit $exit_code
