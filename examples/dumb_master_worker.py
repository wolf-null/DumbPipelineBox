from master_worker.worker import Worker
from master_worker.master import Master
import time


class DumbWorker(Worker):
    def __init__(self, name : str='Unnamed', **kwargs):
        # Always call the super()
        super(DumbWorker, self).__init__(**kwargs)

        # An example of pre-initializing the worker module
        self._seed = name

    def run_task(self, argument:object) -> object:
        # Dumb worker sleeps for 2 sec and reports the job is done.
        time.sleep(2)
        return f'{self._seed} DONE WITH {argument}'


if __name__ == '__main__':
    with Master(DumbWorker, verbose=True) as dumb_master:
        # Add workers. name is just the parameter we defined. It is not present in the base Worker class
        dumb_master.add_worker(name='WORKER-Alice', verbose=True)
        dumb_master.add_worker(name='WORKER-Bob')

        for task_id, result in dumb_master.run('dumb-task-1', 'dumb-task-2', 'dumb-task-3', 'dumb-task-4', 'dumb-task-5'):
            print(f'[DumbTest]: {repr(result)}')
    print('[DumbTest]: All done')


