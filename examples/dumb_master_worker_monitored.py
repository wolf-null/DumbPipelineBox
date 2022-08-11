from lib.worker import Worker
from lib.master import Master
from lib.monitors.workload_max_counter import WorkloadMaxCounter
from random import randint


class DumbWorker(Worker):
    def __init__(self, name : str='Unnamed', **kwargs):
        # Always call the super()
        super(DumbWorker, self).__init__(**kwargs)

        # An example of pre-initializing the worker module
        self._seed = name

    def run_task(self, argument:object) -> object:
        # Dumb worker sleeps for 2 sec and reports the job is done.
        some_sum = sum([randint(0,1) for _ in range(100000)])
        return f'{self._seed} DONE WITH {argument} and sum {some_sum}'


if __name__ == '__main__':
    monitor = WorkloadMaxCounter()

    with Master(DumbWorker, monitor=monitor) as dumb_master:
        # Add workers. name is just the parameter we defined. It is not present in the base Worker class
        dumb_master.add_worker(name='WORKER-Alice')
        dumb_master.add_worker(name='WORKER-Bob')
        dumb_master.add_worker(name='WORKER-Charlie')
        dumb_master.add_worker(name='WORKER-Dave')

        for task_id, result in dumb_master.run(*[f'task{str(t).zfill(5)}' for t in range(10000)]):
            #print(f'[DumbTest]: {repr(result)}')
            print(monitor)
    print('[DumbTest]: All done')


