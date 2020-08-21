#
# Copyright 2020 Gabor Hermann
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import time

from nozzle.dag import Dag, Operator
from nozzle.runtime import run_dag, OperatorFailedError

sleep_time = 0.4


class RuntimeTests(unittest.TestCase):

    def test_empty_dag(self):
        dag = Dag("d")
        run_dag(dag, 2)

    def test_execution_order(self):
        with Dag("d") as dag:
            op1 = Operator(run_id, op_args=[1])
            op2 = Operator(run_id, op_args=[2])
            op3 = Operator(run_id, op_args=[3])
            op4 = Operator(run_id, op_args=[4])

            op1 >> [op2, op3] >> op4

        start_time = time.time()
        run_dag(dag, 4)
        running_time = time.time() - start_time
        self.assertGreater(running_time, sleep_time * 3)
        # Because of parallelism we expect to finish faster
        # than total sleep time of all 4 operators.
        self.assertLess(running_time, sleep_time * 4)

    def test_failing_dag(self):
        with Dag("d") as dag:
            op1 = Operator(error_run_id, op_args=[1])
            op2 = Operator(error_run_id, op_args=[2])
            op3 = Operator(error_run_id, op_args=[3])
            op4 = Operator(error_run_id, op_args=[4])
            op5 = Operator(error_run_id, op_args=[5])

            op1 >> [op2, op3, op4] >> op5

        with self.assertRaises(OperatorFailedError):
            run_dag(dag, 4)


if __name__ == '__main__':
    unittest.main()


def run_id(id):
    time.sleep(sleep_time)
    # Communication between processes is difficult when they are already set up.
    # We can only attach queues when they are. So we revert to just printing.
    # TODO if we add custom error handling we could communicate with errors.
    print(id)


def error_run_id(id):
    if id == 2:
        raise ValueError("SUCCESS (expected error)")

