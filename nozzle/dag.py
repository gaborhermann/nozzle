
class CyclicDagError(Exception):
    pass


class Dag:
    def __init__(self, id):
        self.id = id
        self.ops = []

    def add_op(self, op):
        self.ops.append(op)

    def topological_sort(self):
        """
        Kahn's algorithm based on Wikipedia: https://en.wikipedia.org/wiki/Topological_sorting
        """
        ops = self.ops.copy()

        downstream_ops = _build_downstream_op_lists(ops)
        num_upstream_ops = [len(op.upstream_indices) for op in ops]

        # Empty list that will contain the sorted elements
        topological_order = []
        # Set of all nodes with no incoming edge
        ops_without_deps = set(
            idx for idx, op in enumerate(ops)
            if not op.upstream_indices
        )
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
    def __init__(self, function, dag):
        self.function = function
        self.dag = dag

        self.upstream_indices = set()
        self.idx = len(dag.ops)
        dag.add_op(self)

    def upstream_ops(self):
        return [dag.ops[idx] for idx in self.upstream_indices]

    def set_upstream(self, upstream):
        self.upstream_indices.add(upstream.idx)

    def set_downstream(self, downstream):
        downstream.set_upstream(self)


def _build_downstream_op_lists(ops):
    downstream_ops = [[] for _ in range(len(ops))]
    for downstream_idx, downstream_op in enumerate(ops):
        for upstream_idx in downstream_op.upstream_indices:
            downstream_ops[upstream_idx].append(downstream_idx)
    return downstream_ops


dag = Dag("d")
op1 = Op(lambda: None, dag)
op4 = Op(lambda: None, dag)
op2 = Op(lambda: None, dag)
op3 = Op(lambda: None, dag)


op1.set_downstream(op2)
op2.set_downstream(op3)
#op3.set_downstream(op1)

dag.topological_sort()
