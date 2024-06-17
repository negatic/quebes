from queue import Queue
import concurrent.futures

executor = concurrent.futures.ThreadPoolExecutor()

class Worker():
    def __init__(self, queue:Queue):
        self.queue = queue

    def start(self) -> None:
        """ Starts a worker in a seprate thread using ThreadPoolExecutor """

        # Creates a New Thread to Run Worker In
        executor.submit(self.listen_to_queue)
    
    def listen_to_queue(self):
        """ Listens to queue and executes items from it """
        while True:
            if not self.queue.empty():
                task = self.queue.get()
                task['func'](*task['args'], **task['kwargs'])

class Quebes():
    """ 
    Quebes main instance to define settings and starts workers
    :param workers: Number of workers to start
    """
    def __init__(self):
        self.queues = dict()
        self.workers = dict()

    def task(self, queue_name:str, workers:int=3):
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
                    self.workers[queue_name] = [Worker(self.queues[queue_name]) for _ in range(workers)]
                    for worker in self.workers[queue_name]:
                        worker.start()

                queue  = self.queues[queue_name]
                queue.put({'func': func, 'args': args, 'kwargs': kwargs})
                
                return True
            return wrapper
        return decorator