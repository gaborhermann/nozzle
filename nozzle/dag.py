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
from collections.abc import Iterable
from typing import Union, Sequence
import pickle
from io import BytesIO


class CyclicDagError(Exception):
    """
    Raised when there's a cycle in the Dag.
    """
    pass


class Dag:
    """
    Directed acyclic graph of Operators to execute.
    """
    def __init__(self, id):
        self.id = id
        self._ops = []

    def _add_op(self, op):
        self._ops.append(op)

    def _downstream_ops(self):
        downstream_ops = [[] for _ in range(len(self._ops))]
        for downstream_idx, downstream_op in enumerate(self._ops):
            for upstream_idx in downstream_op._upstream_indices:
                downstream_ops[upstream_idx].append(downstream_idx)
        return downstream_ops

    def _ops_without_upstream(self):
        return set(
            idx for idx, op in enumerate(self._ops)
            if not op._upstream_indices
        )

    def _num_upstream_ops(self):
        return [len(op._upstream_indices) for op in self._ops]

    def topological_sort(self):
        """
        Kahn's algorithm based on Wikipedia: https://en.wikipedia.org/wiki/Topological_sorting
        """
        downstream_ops = self._downstream_ops()
        num_upstream_ops = self._num_upstream_ops()

        # Empty list that will contain the sorted elements
        topological_order = []
        # Set of all nodes with no incoming edge
        ops_without_deps = self._ops_without_upstream()
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

    # Managing context
    def __enter__(self):
        _dag_context_stack.append(self)
        return self

    def __exit__(self, type, value, traceback):
        _dag_context_stack.pop()


# Heavily inspired by Airflow PythonOperator and BaseOperator (i.e. code taken from):
# https://github.com/apache/airflow/blob/fdd68ec653fb9ec4d4c99fac51a6250dea4d7b2c/airflow/operators/python.py
# https://github.com/apache/airflow/blob/fdd68ec653fb9ec4d4c99fac51a6250dea4d7b2c/airflow/models/baseoperator.py#L497
class Operator:
    """
        Executes a Python callable

    :param python_callable: A reference to an object that is callable
    :param Dag dag: Dag of operator
    :param str op_id: Identifier of operator
    :param list op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :param dict op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    """
    def __init__(self, python_callable, dag=None, op_id=None, op_args=None, op_kwargs=None):
        _verify_picklable(python_callable)
        _verify_picklable(op_args)
        _verify_picklable(op_kwargs)
        self.python_callable = python_callable
        self.dag = dag or _current_dag_context()
        self._upstream_indices = set()
        self._idx = len(self.dag._ops)
        self.args = op_args or []
        self.kwargs = op_kwargs or dict()
        self.op_id = op_id or f'#{self._idx}'
        self.dag._add_op(self)

    def set_upstream(self, operator_or_operator_list: Union['Operator', Sequence['Operator']]) -> None:
        """
        Set an operator or an operator list to be directly downstream from the current
        operator.
        """
        self._upstream_indices.update([
            upstream._idx
            for upstream in _make_singleton_if_not_list(operator_or_operator_list)
        ])
        # inefficient, but working way to check for cycles at every added edge
        self.dag.topological_sort()

    def set_downstream(self, operator_or_operator_list: Union['Operator', Sequence['Operator']]) -> None:
        for downstream in _make_singleton_if_not_list(operator_or_operator_list):
            downstream.set_upstream(self)

    # Composing Operators -----------------------------------------------

    def __rshift__(self, other):
        """
        Implements Self >> Other == self.set_downstream(other)
        """
        self.set_downstream(other)
        return other

    def __lshift__(self, other):
        """
        Implements Self << Other == self.set_upstream(other)
        """
        self.set_upstream(other)
        return other

    def __rrshift__(self, other):
        """
        Called for Operator >> [Operator] because list don't have
        __rshift__ operators.
        """
        self.__lshift__(other)
        return self

    def __rlshift__(self, other):
        """
        Called for Operator << [Operator] because list don't have
        __lshift__ operators.
        """
        self.__rshift__(other)
        return self


def _make_singleton_if_not_list(obj_or_list):
    return (obj_or_list
            if isinstance(obj_or_list, Iterable)
            else [obj_or_list])


_dag_context_stack = []


def _current_dag_context():
    if not _dag_context_stack:
        raise RuntimeError("Cannot create operator without Dag context or Dag specified.")
    current_dag = _dag_context_stack[-1]
    return current_dag


def _verify_picklable(obj):
    with BytesIO() as bytes_io:
        pickle.dump(obj, bytes_io)
