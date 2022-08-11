import multiprocessing
from abc import abstractmethod
from typing import Union
from enum import Enum


class Worker:
    """
    LOGIC:
        - A WorkerProcess() is started from the outside of the module.
        - It initializes Worker class with the arguments passed
        - Then it starts the run() process
            - Run process looks to the Worker.task_queue and if it's not empty pops the Task (object) which is tuple (tasi_id:int, argument:object)
            - It unsets the Worker.task_event (which means that the tasks are started to run)
            - and passes it to run_task(argument) which is the abstract of a fabricated method
            - When run_task() is finished it is passed to the Worker.result_queue as  (task_id, result:object)
            - It sets the Worker.task_finished event and checks the Worker.task_queue
            - If it's empty, waits for Worker.task_event
    """

    class Halt:
        def __repr__(self):
            return '<WorkerHalt>'

    class Finished:
        def __repr__(self):
            return '<WorkerFinished>'

    DEBUG = False
    EVENT_TIMEOUT = 0.1

    class WorkerFinishedException(Exception):
        pass

    def __init__(self,
                 task_queue:multiprocessing.Queue,
                 result_queue:multiprocessing.Queue,
                 task_event:multiprocessing.Event,
                 result_event: multiprocessing.Event,
                 task_event_timeout : Union[float, None]= EVENT_TIMEOUT,
                 *args, **kwargs):

        # Task queue receives tuples (task_id, task) where task is the argument for Worker.run_task() implementation
        self._task_queue = task_queue

        # Result queue sends results of Worker.run_task() back to Master
        self._result_queue = result_queue

        # 0 - Worker is ready to receive new tasks; 1 - Worker has queued tasks to do
        self._task_event = task_event

        # 0 - All results are read, no new results yet posted. 1 - Result queue has at least one new unread result
        self._result_event = result_event

        # Each iteration of run() cycle has timeout. In other case there are event-lock probability in some cases
        # None for infinite
        self._task_event_timeout = task_event_timeout

    @abstractmethod
    def run_task(self, argument:object) -> object:
        pass

    def push_result_to_queue(self, task_id:Union[int, None], result:object):
        # Pushes result to Worker.result_queue (to Master class) and notifies it
        self._result_queue.put((task_id, result))
        self._result_event.set()

    def run(self):
        if self.DEBUG:
            print('[Worker]: Run!')
        while True:
            if self._task_event_timeout is not None:
                self._task_event.wait(timeout=self._task_event_timeout)
            else:
                self._task_event.wait()

            # Get all queued tasks
            pending_tasks = list()
            while not self._task_queue.empty():
                new_task = self._task_queue.get(block=False)  # Only one queue-reader so blocking is meaningless
                if self.DEBUG:
                    print(f'[Worker]: Task events rcvd {repr(new_task)}')
                pending_tasks.append(new_task)

            # Check for Halt events
            for task_id, task in pending_tasks:
                if isinstance(task, Worker.Halt):
                    if self.DEBUG:
                        print('[Worker]: Halt!')
                    # Exit run() cycle and finish the process
                    return

            # Execute tasks
            for task_id, task in pending_tasks:
                try:
                    result = self.run_task(task)
                except Exception as exception:
                    result = task_id, exception

                self.push_result_to_queue(task_id, result)

            # Tell the worker is awaiting for tasks
            self._task_event.clear()

    @classmethod
    def _WorkerProcess(cls, task_queue:multiprocessing.Queue,
                 result_queue:multiprocessing.Queue,
                 task_event:multiprocessing.Event,
                 result_event: multiprocessing.Event,
                 task_event_timeout: Union[float, None],
                 **kwargs):
        worker_instance = cls(
            task_queue=task_queue,
            result_queue=result_queue,
            task_event=task_event,
            result_event=result_event,
            task_event_timeout=task_event_timeout,
            **kwargs
        )

        worker_instance.run()

    @classmethod
    def WorkerProcess(cls, task_queue:multiprocessing.Queue,
                 result_queue:multiprocessing.Queue,
                 task_event:multiprocessing.Event,
                 result_event: multiprocessing.Event,
                 task_event_timeout: Union[float, None] = EVENT_TIMEOUT,
                 **kwargs):
        if Worker.DEBUG:
            print(f'[Worker]: WorkerProcess of {cls.__name__} with {repr(kwargs)}')

        _kwargs =  {
                    #'cls': cls,
                    'task_queue': task_queue,
                    'result_queue': result_queue,
                    'task_event': task_event,
                    'result_event': result_event,
                    'task_event_timeout': task_event_timeout,
                   }
        _kwargs.update(kwargs)

        worker_process = multiprocessing.Process(
            target=cls._WorkerProcess,
            kwargs=_kwargs
        )
        worker_process.start()
        return worker_process


