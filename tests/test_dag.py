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

from nozzle.dag import Dag, Operator, CyclicDagError


class DagTests(unittest.TestCase):

    def test_create_regular_dag_with_dependencies(self):
        dag = Dag("d")
        op1 = Operator(lambda: None, dag)
        op2 = Operator(lambda: None, dag)
        op3 = Operator(lambda: None, dag)
        op4 = Operator(lambda: None, dag)
        op1 >> [op2, op3] >> op4
        self.assertEqual(set(), op1._upstream_indices)
        self.assertEqual({op1._idx}, op2._upstream_indices)
        self.assertEqual({op1._idx}, op3._upstream_indices)
        self.assertEqual({op2._idx, op3._idx}, op4._upstream_indices)

    def test_create_circular_dag(self):
        with self.assertRaises(CyclicDagError):
            dag = Dag("d")
            op1 = Operator(lambda: None, dag)
            op2 = Operator(lambda: None, dag)
            op3 = Operator(lambda: None, dag)
            op1 >> op2 >> op3
            op3 >> op1


if __name__ == '__main__':
    unittest.main()
