
class Dag:
    def __init__(self, id):
        self.id = id
        self.ops = []

    def depends_on(self, upstream, downstream):
        self.dependencies[downstream] += upstream

    def add_op(self, op):
        self.ops.append(op)

    def is_there_cycle(self):
        s = []
        discovered = [False] * len(self.ops)

        while True:
            try:
                next_index = discovered.index(False)
            except ValueError:
                # everything is discovered, there's no cycle
                return False

            op = self.ops[next_index]
            s.append(op)
            # print(f"DFS {op.idx}")

            while s:
                # print([x.idx for x in s])
                v = s.pop()
                if not discovered[v.idx]:
                    discovered[v.idx] = True
                    for w in v.dependencies:
                        if w.idx == op.idx:
                            # discovered a cycle
                            return True
                        s.append(w)


class Op:
    def __init__(self, function, dag):
        self.function = function
        self.dag = dag

        self.dependencies = []
        self.idx = len(dag.ops)
        dag.add_op(self)

    def set_upstream(self, upstream):
        self.dependencies.append(upstream)

    def set_downstream(self, downstream):
        downstream.set_upstream(self)


dag = Dag("d")
op4 = Op(lambda: None, dag)
op1 = Op(lambda: None, dag)
op2 = Op(lambda: None, dag)
op3 = Op(lambda: None, dag)


op1.set_downstream(op2)
op2.set_downstream(op3)
op3.set_downstream(op1)

dag.is_there_cycle()

