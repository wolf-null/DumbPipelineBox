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
Import the Worker class from lib.worker and 
1. overload the Worker.\_\_init\_\_() method if you need to define worker's pre-init
2. overload the Worker.run\_task(argument) to define the worker's logic. Use return (not yield) to return the result of the task.

For examples see examples :)
