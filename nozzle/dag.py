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

class CyclicDagError(Exception):
    pass


class Dag:
    """
    Directed acyclic graph of Operators to execute.
    """
    def __init__(self, id):
        self.id = id
        self.ops = []

    def add_op(self, op):
        self.ops.append(op)

    def downstream_ops(self):
        downstream_ops = [[] for _ in range(len(self.ops))]
        for downstream_idx, downstream_op in enumerate(self.ops):
            for upstream_idx in downstream_op.upstream_indices:
                downstream_ops[upstream_idx].append(downstream_idx)
        return downstream_ops

    def ops_without_deps(self):
        return set(
            idx for idx, op in enumerate(self.ops)
            if not op.upstream_indices
        )

    def num_upstream_ops(self):
        return [len(op.upstream_indices) for op in self.ops]

    def topological_sort(self):
        """
        Kahn's algorithm based on Wikipedia: https://en.wikipedia.org/wiki/Topological_sorting
        """
        downstream_ops = self.downstream_ops()
        num_upstream_ops = self.num_upstream_ops()

        # Empty list that will contain the sorted elements
        topological_order = []
        # Set of all nodes with no incoming edge
        ops_without_deps = self.ops_without_deps()
        while ops_without_deps:
            op = ops_without_deps.pop()
            topological_order.append(op)
            for d in downstream_ops[op]:
                num_upstream_ops[d] -= 1 # remove edge from graph
                if num_upstream_ops[d] <= 0:
                    ops_without_deps.add(d)

        if any(n != 0 for n in num_upstream_ops):
            raise CyclicDagError("There is a cycle in the DAG and shouldn't be "
                                 "(A stands for Acyclic).")
        else:
            return topological_order


class Op:
    def __init__(self, function, dag, args=None, kwargs=None):
        self.function = function
        self.dag = dag

        self.upstream_indices = set()
        self.idx = len(dag.ops)
        self.args = args if args else []
        self.kwargs = kwargs if kwargs else dict()
        dag.add_op(self)

    def set_upstream(self, upstream):
        self.upstream_indices.add(upstream.idx)

    def set_downstream(self, downstream):
        downstream.set_upstream(self)



# dag = Dag("d")
# op1 = Op(lambda: None, dag)
# op4 = Op(lambda: None, dag)
# op2 = Op(lambda: None, dag)
# op3 = Op(lambda: None, dag)
#
#
# op1.set_downstream(op2)
# op2.set_downstream(op3)
# op3.set_downstream(op1)
#
# dag.topological_sort()
