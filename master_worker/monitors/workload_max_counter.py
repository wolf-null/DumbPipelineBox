from master_worker.task_monitor import AbstractTaskMonitor


class WorkloadMaxCounter(AbstractTaskMonitor):
    """Displays estimated current Worker load (number of tasks queued) AND it's maximum value in the braces"""

    def __init__(self, display_at_top=True):
        """
        :param display_at_top: moves the terminal cursor on the left-top corner and prints there
        """
        self._counter = dict()
        self._counter_max = dict()
        self._display_at_top = display_at_top
        self._total_tasks_added = 0
        self._total_tasks_received = 0

    def on_worker_added(self, worker_idx: int, **kwargs):
        self._counter[worker_idx] = 0
        self._counter_max[worker_idx] = 0

    def on_task_added(self, task_id:int, task:object):
        self._total_tasks_added += 1

    def on_task_attached_to_worker(self, worker_idx: int, task_id: int):
        self._counter[worker_idx] += 1
        if self._counter_max[worker_idx] < self._counter[worker_idx]:
            self._counter_max[worker_idx] = self._counter[worker_idx]

    def on_result_retrieved_from_worker(self, worker_idx: int, task_id: int):
        self._counter[worker_idx] -= 1
        self._total_tasks_received += 1

    def __repr__(self):
        if self._display_at_top:
            self.move_to_start()
        line = f'{self._total_tasks_received} of {self._total_tasks_added}\n'
        line += '\n'.join([f'{key}:\t{self._counter[key]} ({self._counter_max[key]})' for key in self._counter.keys()])
        return line

    def move_to_start(self):
        """
        TOOK FROM https://stackoverflow.com/questions/54630766/how-can-move-terminal-cursor-in-python
        """
        print("\033[%d;%dH" % (0, 0))
