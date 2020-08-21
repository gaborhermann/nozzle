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

from multiprocessing import Process, Queue, current_process
import multiprocessing
import logging

import time

from dag import Dag, Operator


def _enforce_fork_start_method():
    # We need to make sure we use "fork" because "spawn" is the default in Python 3.8.
    try:
        multiprocessing.set_start_method("fork")
    except RuntimeError as e:
        if e != "context has already been set":
            raise e
        start_method = multiprocessing.get_start_method()
        if start_method != "fork":
            raise RuntimeError(f"Could not set multiprocessing start method to 'fork', "
                               f"it's already set to '{start_method}'.") from e


_enforce_fork_start_method()


class _OperatorFailedMessage:
    """
    Internal message object sent back to main process.
    """
    def __init__(self, op, e, tb):
        self.op = op
        self.e = e
        self.tb = tb


class OperatorFailedError(Exception):
    """
    Happens when an Operator fails while running a Dag.
    """
    pass


def _worker(task_queue, done_queue):
    """
    Worker process definition
    :param task_queue Queue:
    :param done_queue Queue:
    :return:
    """
    for op in iter(task_queue.get, "STOP"):
        try:
            logging.info(f"{current_process().name} running Operator {op.idx}")
            op.python_callable(*op.args, **op.kwargs)
            logging.info(f"{current_process().name} finished Operator {op.idx}")
            done_queue.put(op)
        except Exception as e:
            import sys
            import traceback
            # we cannot pickle traceback object, so we send it back to main process in string
            tb_str = "".join(traceback.format_exception(*sys.exc_info()))
            done_queue.put(_OperatorFailedMessage(op, e, tb_str))


def run_dag(dag, num_of_processes):
    """
    Executes dag by forking `num_of_processes` processes respecting dependencies.

    :param Dag dag: Dag to execute
    :param int num_of_processes: number of :py:class:`Process`es to run
    """
    task_queue = Queue()
    task_output_queue = Queue()

    def schedule_op(idx):
        logging.info(f"Scheduled to run operator {dag.ops[idx]}")
        task_queue.put(dag.ops[idx])

    for idx in dag._ops_without_upstream():
        schedule_op(idx)

    downstream_ops = dag._downstream_ops()
    num_upstream_ops = dag._num_upstream_ops()
    num_ops_done = 0

    try:
        # start processes
        for i in range(num_of_processes):
            Process(target=_worker, args=(task_queue, task_output_queue)).start()

        while num_ops_done < len(dag.ops):
            task_output = task_output_queue.get()

            # handle exceptions in task execution
            if isinstance(task_output, _OperatorFailedMessage):
                op_failed = task_output
                logging.error(f"Stopping execution because operator {op_failed.op.idx} failed")
                raise OperatorFailedError(
                    f"Operator with index {op_failed.op.idx} failed. "
                    f"Cause traceback:\n\n{op_failed.tb}"
                ) from op_failed.e

            # operator is done
            done_op = task_output
            logging.info(f'Operator {done_op.idx} has finished successfully.')
            num_ops_done += 1

            for d in downstream_ops[done_op.idx]:
                num_upstream_ops[d] -= 1  # remove edge from graph
                if num_upstream_ops[d] <= 0:
                    # submit downstream op, because all dependencies are done
                    schedule_op(d)
    finally:
        # stop processes
        for i in range(num_of_processes):
            task_queue.put('STOP')


def run_id(id):
    print(f"Started {id}")
    time.sleep(0.2)
    if id == 2:
        print("GONNA FAIL!")
        raise ValueError("ERROR!!!!!")
    print(f"Done {id}")
    return 0


dag = Dag("d")

op1 = Operator(run_id, dag, op_args=[1])
op2 = Operator(run_id, dag, op_args=[2])
op3 = Operator(run_id, dag, op_args=[3])
op4 = Operator(run_id, dag, op_args=[4])
op5 = Operator(run_id, dag, op_args=[5])

op1 >> [op2, op3, op4] >> op5

run_dag(dag, 4)
