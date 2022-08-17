from abc import abstractmethod


class AbstractTaskMonitor:
    """
    TaskMonitor implementations are passed to the Master on __init__
    It can be used to collect and monitor statistics on Master/Worker statuses
    """

    @abstractmethod
    def on_worker_added(self, worker_idx:int, **kwargs):
        # Invoked by Master when worker is registred.
        # **kwargs are for the initialization parameters used to init the worker when added
        pass

    @abstractmethod
    def on_task_added(self, task_id:int, task:object):
        # Invoked by Master when tasks are added. Currently, not expected during Master.run()
        pass

    @abstractmethod
    def on_task_attached_to_worker(self, worker_idx:int, task_id:int):
        # Invoked by Master right before task is pushed to a certain worker with <worker_idx>
        pass

    @abstractmethod
    def on_result_retrieved_from_worker(self, worker_idx:int, task_id:int):
        # Invoked by Master right after it reads the result from the Worker's result_queue
        pass


