from multiprocessing import Process, Queue, current_process
import multiprocessing
import logging

import time

from dag import Dag, Op


def enforce_fork_start_method():
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


enforce_fork_start_method()


class OperatorFailedMessage:
    def __init__(self, op, e, tb):
        self.op = op
        self.e = e
        self.tb = tb


class OperatorFailedError(Exception):
    pass


def worker(task_queue, done_queue):
    for op in iter(task_queue.get, "STOP"):
        try:
            print(f"{current_process().name} running op {op.idx}")
            op.function(*op.args, **op.kwargs)
            print(f"{current_process().name} finished op {op.idx}")
            done_queue.put(op)
        except Exception as e:
            import sys
            import traceback
            tb_str = "".join(traceback.format_exception(*sys.exc_info()))
            done_queue.put(OperatorFailedMessage(op, e, tb_str))


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

    for idx in dag.ops_without_deps():
        schedule_op(idx)

    downstream_ops = dag.downstream_ops()
    num_upstream_ops = dag.num_upstream_ops()
    num_ops_done = 0

    try:
        # start processes
        for i in range(num_of_processes):
            Process(target=worker, args=(task_queue, task_output_queue)).start()

        while num_ops_done < len(dag.ops):
            task_output = task_output_queue.get()

            # handle exceptions in task execution
            if isinstance(task_output, OperatorFailedMessage):
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

op1 = Op(run_id, dag, args=[1])
op2 = Op(run_id, dag, args=[2])
op3 = Op(run_id, dag, args=[3])
op4 = Op(run_id, dag, args=[4])
op5 = Op(run_id, dag, args=[5])

op1.set_downstream(op2)
op1.set_downstream(op3)
op1.set_downstream(op4)

op2.set_downstream(op5)
op3.set_downstream(op5)
op4.set_downstream(op5)

run_dag(dag, 4)
