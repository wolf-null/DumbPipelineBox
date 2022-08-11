from lib.worker import Worker
from lib.master import Master
import time


class DumbWorker(Worker):
    def __init__(self, **kwargs):
        # Always call the super()
        super(DumbWorker, self).__init__(**kwargs)

        # An example of pre-initializing the worker module
        self._seed = kwargs['name'] if 'name' in kwargs else '??'

    def run_task(self, argument:object) -> object:
        # Dumb worker sleeps for 2 sec and reports the job is done.
        time.sleep(2)
        return f'DONE WITH {argument}'


if __name__ == '__main__':
    with Master(DumbWorker) as master:
        master.add_worker(name='WORKER-Alice')
        master.add_worker(name='WORKER-Bob')

        for task_id, result in master.run('task-1', 'task-2', 'task-3', 'task-4'):
            print(f'[DumbTest]: {repr(result)}')
    print('[DumbTest]: All done')


