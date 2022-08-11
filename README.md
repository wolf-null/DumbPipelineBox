# DumbPipelineBox #
*Yet another master-worker lib for Python*

## Motivation ##
**Why not asyncio lib?**\
Asyncio uses only one core due to GIL. Not meant to be multiprocess. I was in need of multiprocess master-worker.

**Why not concurrent.Futures?**\
I was in need of worker pre-configuration of libs it uses. The libs have time-intensive initialization so I didn't want them to init each time the task is pushed.

**Why?**\
Yes.

**Will the lib grow to something more versatile?**\
Oh, we've got plans...

## Usage ##
1. Import the Worker class from lib.worker and 
   1. overload the ```Worker.__init__()``` method if you need to define worker's pre-init
   2. overload the ```Worker.run_task(argument)``` to define the worker's logic. Use return (not yield) to return the result of the task.

```python
class MyWorkerSubclass(Worker):
    def __init__(self, my_parameter_a, my_parameter_b, ...,  **kwargs):
        # Always call the super()
        super(DumbWorker, self).__init__(**kwargs)

        # Some specific init the class you need to do before any work starts
        # ...

    def run_task(self, argument:object) -> object:
        # Some working activity
        # ...
        return work_result # or None :)

```

2. Import the Master class from lib.master. Initialize it with the user-defined Worker class
   1. Add workers via ```Master.add_worker(**kwargs)```, the kwargs will be passed to your user-defined Worker's ```__init__(**kwargs)```
   2. Add tasks via ```Master.add_task(argument)```
   3. Run the tasks with master.run() which will yield ```task_id, results``` pair (task_id is generated automatically) 

```python
with Master(MyWorkerSubclass) as master:
    # Add workers
    master.add_worker(my_parameter_a='...', my_parameter_b='...')
    master.add_worker(my_parameter_a='...', my_parameter_b='...')

    for task_id, result in master.run('my_argument_1', 'my_argument_2', 'my_argument_3', ...):
        # master.run yields the result on it is done
        # if error will return the exception as the result
```
For examples see examples :)
