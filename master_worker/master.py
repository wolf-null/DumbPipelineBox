import multiprocessing
from typing import List, Type, Union

from .worker import Worker
from .task_monitor import AbstractTaskMonitor


class Master:
    HALT_TIMEOUT = 1

    def __init__(self, worker_implementation:Worker, callback=None, monitor : AbstractTaskMonitor=None, verbose=False):
        assert issubclass(worker_implementation, Worker),\
            ValueError('worker_implementation parameter must be a Worker inheritor')
        assert callable(callback) or callback is None,\
            ValueError('callback must be a callable(progress:int, max:int) or None')
        assert monitor is None or isinstance(monitor, AbstractTaskMonitor),\
            ValueError('monitor must be an inheritor of AbstractTaskMonitor')
        assert isinstance(verbose, bool),\
            ValueError('verbose parameter must be True, False or None (for default)')

        self._verbose = verbose

        self._worker_task_queues = list()  # type: List[multiprocessing.Queue]
        self._worker_result_queues = list()  # type: List[multiprocessing.Queue]
        self._worker_task_events = list()  # type: List[multiprocessing.Event]
        self._worker_result_events = list()  # type: List[multiprocessing.Event]
        self._worker_processes = list()  # type: List[Worker]
        self._worker_class = worker_implementation  # type: Type[Worker]

        self._callback = callback

        self._master_task_list = list()
        self._task_ids = list()

        self._task_counter = 1
        self._tasks_passsed = 0

        self._task_monitor = monitor  # type: AbstractTaskMonitor

    def add_worker(self, **kwargs):
        task_queue = multiprocessing.Queue()
        result_queue = multiprocessing.Queue()
        task_event = multiprocessing.Event()
        result_event = multiprocessing.Event()

        task_event.clear()
        result_event.clear()

        worker = self._worker_class.WorkerProcess(
            task_queue=task_queue,
            result_queue=result_queue,
            task_event=task_event,
            result_event=result_event,
            **kwargs
        )

        self._worker_task_queues.append(task_queue)
        self._worker_result_queues.append(result_queue)
        self._worker_task_events.append(task_event)
        self._worker_result_events.append(result_event)
        self._worker_processes.append(worker)

        if self._task_monitor is not None:
            self._task_monitor.on_worker_added(worker_idx=len(self._worker_processes)-1, **kwargs)

    def add_task(self, task:object):
        task_id = self._task_counter
        task_wrapped = task_id, task
        self._task_ids.append(task_id)
        self._task_counter += 1
        self._master_task_list.append(task_wrapped)
        if self._verbose:
            print(f'[Master]: + task {task_id}')

        if self._task_monitor is not None:
            self._task_monitor.on_task_added(task_id=task_id, task=task)

        return task_id

    def deliver_task_to_worker(self, worker_idx, task:object):
        worker_task_event = self._worker_task_events[worker_idx]
        worker_task_queue = self._worker_task_queues[worker_idx]

        if self._task_monitor is not None:
            task_id = task[0]
            self._task_monitor.on_task_attached_to_worker(worker_idx=worker_idx, task_id=task_id)

        worker_task_queue.put(task)
        if self._verbose:
            print(f'[Master]: Delivered {repr(task)} --> {worker_idx}')
        worker_task_event.set()

    def deliver_task_to_all_workers(self, task:object):
        # TODO REFACTOR
        for worker_idx in range(len(self._worker_processes)):
            self.deliver_task_to_worker(worker_idx=worker_idx, task=task)

    def deliver_task_to_idle_worker(self, task:object) -> bool:
        # Find free worker
        for worker_idx in range(len(self._worker_processes)):
            worker_task_event = self._worker_task_events[worker_idx]
            if not worker_task_event.is_set():
                self.deliver_task_to_worker(worker_idx=worker_idx, task=task)
                return True
        return False

    def collect_result_from_workers(self) -> list:
        results = list()
        for worker_idx in range(len(self._worker_processes)):
            worker_result_event = self._worker_result_events[worker_idx]
            worker_result_queue = self._worker_result_queues[worker_idx]
            if worker_result_event.is_set():
                while not worker_result_queue.empty():
                    result = worker_result_queue.get()
                    if self._verbose:
                        print('[Master]: [Result]: ', result)
                    if self._task_monitor is not None:
                        task_id = result[0]
                        self._task_monitor.on_result_retrieved_from_worker(worker_idx=worker_idx, task_id=task_id)
                    results.append(result)
        return results

    def wait_for_any_worker_result_event(self, per_worker_timeout=0.1):
        while True:
            # The problem here is that if a process has no task attached the method will wait for in anyway
            for worker_result_event in self._worker_result_events:
                if worker_result_event.wait(per_worker_timeout):
                    return

    def join_all_workers(self):
        """Returns when all Worker processes are finished"""
        for worker_idx, worker in enumerate(self._worker_processes): # type: multiprocessing.Process
            if self._verbose:
                print(f'[Master]: Joining the {worker_idx} worker to finish')
            if self.HALT_TIMEOUT is not None:
                worker.join(timeout=self.HALT_TIMEOUT)
            else:
                worker.join()

    def terminate_all_workers(self):
        """Terminates all the treads"""

        for worker_idx, worker in enumerate(self._worker_processes): # type: multiprocessing.Process
            if worker.is_alive():
                if self._verbose:
                    print(f'[Master]: Terminating the {worker_idx} worker')
                worker.terminate()

    def run(self, *args):
        for arg in args:
            self.add_task(arg)

        self._tasks_passsed = len(self._task_ids)

        while True:
            # Deliver tasks while possible
            while True:
                if len(self._master_task_list) == 0:
                    break

                task = self._master_task_list.pop()
                delivered = self.deliver_task_to_idle_worker(task)  # Deliverance cause immediate execution
                if not delivered:
                    # Return non delivered task back to the queue
                    self._master_task_list.append(task)
                    break

            self.wait_for_any_worker_result_event()

            results = self.collect_result_from_workers()
            for result in results:
                task_id = result[0]
                self._task_ids.remove(task_id)

                if self._verbose:
                    print(f'[Master]: - task {task_id}')

                if callable(self._callback):
                    self._callback(
                        self._tasks_passsed - len(self._task_ids),
                        self._tasks_passsed
                    )
                yield result

            if len(self._task_ids) == 0:
                # This assumes no task insertion from worker side
                return

    def halt(self, join:bool = True):
        try:
            self.deliver_task_to_all_workers((-1, Worker.Halt()))
            if join:
                self.join_all_workers()
            self.terminate_all_workers()
        except Exception as errmsg:
            print('[Master]: Error during process halt')
            raise errmsg
        else:
            if self._verbose:
                print('[Master]: Halted')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.halt()
        if issubclass(exc_type, KeyboardInterrupt):
            return True

