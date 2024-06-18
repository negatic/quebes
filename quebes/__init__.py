from queue import Queue
import concurrent.futures

executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix='quebes')

class Worker():
    def __init__(self, queue:Queue, max_retries):
        self.queue = queue
        self.max_retries = max_retries

    def start(self) -> None:
        """ Starts a worker in a seprate thread using ThreadPoolExecutor """

        # Creates a New Thread to Run Worker In
        executor.submit(self.listen_and_execute)
    
    def listen_and_execute(self):
        """ Listens to queue and executes items from it """
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
                    self.workers[queue_name] = [Worker(queue=self.queues[queue_name], max_retries=max_retries) for _ in range(workers)]
                    for worker in self.workers[queue_name]:
                        worker.start()

                queue  = self.queues[queue_name]
                queue.put({'func': func, 'args': args, 'kwargs': kwargs, 'retries': 0})
                
                return True
            return wrapper
        return decorator