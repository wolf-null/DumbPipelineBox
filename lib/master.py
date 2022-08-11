import multiprocessing
from typing import List, Type

from .worker import Worker


class Master:
    DEBUG = Worker.DEBUG
    JOIN_TIMEOUT = -1

    def __init__(self, worker_implementation:Worker, callback=None):
        self._worker_task_queues = list()  # type: List[multiprocessing.Queue]
        self._worker_result_queues = list()  # type: List[multiprocessing.Queue]
        self._worker_task_events = list()  # type: List[multiprocessing.Event]
        self._worker_result_events = list()  # type: List[multiprocessing.Event]
        self._workers = list()  # type: List[Worker]
        self._worker_class = worker_implementation  # type: Type[Worker]

        self._callback = callback

        self._master_task_list = list()
        self._task_ids = list()

        self._task_counter = 1
        self._tasks_passsed = 0

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
        self._workers.append(worker)

    def add_task(self, task:object):
        task_id = self._task_counter
        task_wrapped = task_id, task
        self._task_ids.append(task_id)
        self._task_counter += 1
        self._master_task_list.append(task_wrapped)
        if self.DEBUG:
            print(f'[Master]: + task {task_id}')
        return task_id

    def deliver_task_to_all_workers(self, task:object):
        # TODO REFACTOR
        for worker_idx in range(len(self._workers)):
            worker_task_event = self._worker_task_events[worker_idx]
            worker_task_queue = self._worker_task_queues[worker_idx]
            worker_task_queue.put(task)
            if self.DEBUG:
                print(f'[Master]: Delivered {repr(task)} --> {worker_idx}')
            worker_task_event.set()

    def deliver_task_to_idle_worker(self, task:object) -> bool:
        # Find free worker
        for worker_idx in range(len(self._workers)):
            worker_task_event = self._worker_task_events[worker_idx]
            if not worker_task_event.is_set():
                worker_task_queue = self._worker_task_queues[worker_idx]
                worker_task_queue.put(task)
                if self.DEBUG:
                    print(f'[Master]: Delivered {repr(task)} --> {worker_idx}')
                worker_task_event.set()
                return True
        return False

    def collect_result_from_workers(self) -> list:
        results = list()
        for worker_idx in range(len(self._workers)):
            worker_result_event = self._worker_result_events[worker_idx]
            worker_result_queue = self._worker_result_queues[worker_idx]
            if worker_result_event.is_set():
                while not worker_result_queue.empty():
                    result = worker_result_queue.get()
                    if self.DEBUG:
                        print('[Master]: [Result]: ', result)
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

        for worker in self._workers: # type: multiprocessing.Process
            if self.JOIN_TIMEOUT >= 0:
                worker.join(timeout=self.JOIN_TIMEOUT)
            else:
                worker.join()

    def terminate_all_workers(self):
        """Terminates all the treads"""

        for worker in self._workers: # type: multiprocessing.Process
            if worker.is_alive():
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

                if self.DEBUG:
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
        self.deliver_task_to_all_workers((-1, Worker.Halt()))
        if join:
            self.join_all_workers()
        self.terminate_all_workers()


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.halt()
