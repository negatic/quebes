from abc import ABC, abstractmethod
from queue import Queue
import concurrent.futures
from datetime import datetime
import time

executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix='quebes')

class Worker(ABC):
    def start(self, *args, **kwargs):
        """ Starts a worker in a seprate thread using ThreadPoolExecutor """
        executor.submit(self.execute)

    @abstractmethod
    def execute(self):
        pass

class QueueWorker(Worker):
    def __init__(self, queue:str, max_retries:int) -> None:
        self.queue = queue
        self.max_retries = max_retries

    def execute(self):
        """ Listens to queue and executes items from it """
        # Keep Thread Open
        while True:
            # Get Task From Queue & Execute It
            if not self.queue.empty():
                task = self.queue.get()
                
                try:
                    task['func'](*task['args'], **task['kwargs'])

                except Exception as e:
                    # Re-Insert Task Into Queue
                    if task['retries'] < self.max_retries:
                        print(f'Retrying Task {task["retries"] + 1}/{self.max_retries}')
                        self.queue.put({'func': task['func'], 
                                        'args': task['args'], 
                                        'kwargs': task['kwargs'], 
                                        'retries': task['retries'] + 1})

class ScheduledWorker(Worker):
    def __init__(self, func, period:int, name:str):
        self.func = func
        self.period = period
        self.name = name
        self.schedule_history = dict()

    def execute(self):    
        """ Run a function periodically """
        while True:
            try:
                # Run Task
                self.func()
                # Update History State
                self.schedule_history[self.name] = datetime.now()
                # Wait Before Running It Again
                time.sleep(self.period)

            except Exception as e:
                print(f'Error running scheduled task: {e}')

class Quebes():
    """ 
    Quebes main instance to define settings and starts workers
    :param workers: Number of workers to start
    """
    def __init__(self):
        self.queues = dict()
        self.workers = dict()

    def task(self, queue_name:str, workers:int=3, max_retries:int=0):
        """
        Decorator to run a function as a background task
        If :: param queue_name :: is not set a random name will be generated
        """
        def decorator(func):
            def wrapper(*args, **kwargs):
                # Create Queue If It Doesn't Exist
                if queue_name not in self.queues:
                    self.queues[queue_name] = Queue()
                
                # Create Workers For Queue If They Don't Exist
                if queue_name not in self.workers:
                    self.workers[queue_name] = [QueueWorker(queue=self.queues[queue_name], max_retries=max_retries) for _ in range(workers)]
                    for worker in self.workers[queue_name]:
                        worker.start()

                queue  = self.queues[queue_name]
                queue.put({'func': func, 'args': args, 'kwargs': kwargs, 'retries': 0})
                
                return True
            return wrapper
        return decorator

    def periodic_task(self, period:int, name:str):
        """ Decorator to execute a function every n seconds """
        def decorator(function):
            def wrapper(*args, **kwargs):
                ScheduledWorker(func=function, period=period, name=name).start()
            return wrapper
        return decorator