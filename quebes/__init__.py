from queue import Queue
import concurrent.futures
import time
from datetime import datetime

executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix='quebes')

class Worker():
    def __init__(self, type:str):
        self.type = type
        self.queue = None
        self.max_retries = None
        self.schedule_history = dict()
    
    def start(self, *args, **kwargs):
        """ Starts a worker in a seprate thread using ThreadPoolExecutor """
        if self.type == 'queued_thread':
            executor.submit(self.queued_thread, *args, **kwargs)
        
        elif self.type == 'scheduled_thread':
            executor.submit(self.scheduled_thread, *args, **kwargs)
        
        else:
            print('Bad worker type')

    def queued_thread(self, queue:Queue, max_retries:int):
        """ Listens to queue and executes items from it """
        # Create The Thread
        with executor:
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
    
    def scheduled_thread(self, func, period:int, name:str):
        """ Run a function periodically """
        with executor:
            while True:
                try:
                    # Run Task
                    func()
                    # Update History State
                    self.schedule_history[name] = datetime.now()
                    # Wait Before Running It Again
                    time.sleep(period)

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
                    self.workers[queue_name] = [Worker(type='queued_thread') for _ in range(workers)]
                    for worker in self.workers[queue_name]:
                        worker.start(queue=self.queues[queue_name], max_retries=max_retries)

                queue  = self.queues[queue_name]
                queue.put({'func': func, 'args': args, 'kwargs': kwargs, 'retries': 0})
                
                return True
            return wrapper
        return decorator

    def periodic_task(self, period:int, name:str):
        """ Decorator to execute a function every n seconds """
        def decorator(function):
            def wrapper(*args, **kwargs):
                Worker(type='scheduled_thread').start(func=function, period=period, name=name)
            return wrapper
        return decorator