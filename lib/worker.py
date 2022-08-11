import multiprocessing
from abc import abstractmethod


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
    DEBUG = False

    class Halt:
        def __repr__(self):
            return '<WorkerHalt>'

    def __init__(self,
                 task_queue:multiprocessing.Queue,
                 result_queue:multiprocessing.Queue,
                 task_event:multiprocessing.Event,
                 result_event: multiprocessing.Event,
                 *args, **kwargs):
        self._task_queue = task_queue
        self._result_queue = result_queue

        # 0 - Worker is ready to receive new tasks; 1 - Worker has queued tasks to do
        self._task_event = task_event

        # 0 - All results are read, no new results yet posted. 1 - Result queue has at least one new unread result
        self._result_event = result_event

    @abstractmethod
    def run_task(self, argument:object) -> object:
        pass

    def run(self):
        if self.DEBUG:
            print('[Worker]: Run!')
        while True:
            self._task_event.wait()

            # Get all queued tasks
            pending_tasks = list()
            while not self._task_queue.empty():
                new_task = self._task_queue.get()
                if self.DEBUG:
                    print(f'[Worker]: Task events rcvd {repr(new_task)}')
                pending_tasks.append(new_task)

            # Check for Halt events
            for task_id, task in pending_tasks:
                if isinstance(task, Worker.Halt):
                    if self.DEBUG:
                        print('[Worker]: Halt!')
                    return

            # Execute tasks
            for task_id, task in pending_tasks:
                try:
                    result = self.run_task(task)
                    result_tuple = task_id, result
                except Exception as exception:
                    result_tuple = task_id, exception
                finally:
                    self._result_queue.put(result_tuple)
                    self._result_event.set()

            # Tell the worker is awaiting for tasks
            self._task_event.clear()

    @classmethod
    def _WorkerProcess(cls, task_queue:multiprocessing.Queue,
                 result_queue:multiprocessing.Queue,
                 task_event:multiprocessing.Event,
                 result_event: multiprocessing.Event,
                 **kwargs):
        worker_instance = cls(
            task_queue=task_queue,
            result_queue=result_queue,
            task_event=task_event,
            result_event=result_event, **kwargs
        )

        worker_instance.run()

    @classmethod
    def WorkerProcess(cls, task_queue:multiprocessing.Queue,
                 result_queue:multiprocessing.Queue,
                 task_event:multiprocessing.Event,
                 result_event: multiprocessing.Event,
                 **kwargs):
        if Worker.DEBUG:
            print(f'[Worker]: WorkerProcess of {cls.__name__} with {repr(kwargs)}')
        _kwargs =  {
                    #'cls': cls,
                    'task_queue': task_queue,
                    'result_queue': result_queue,
                    'task_event': task_event,
                    'result_event': result_event
                    }
        _kwargs.update(kwargs)
        worker_process = multiprocessing.Process(
            target=cls._WorkerProcess,
            kwargs=_kwargs
        )
        worker_process.start()
        return worker_process


