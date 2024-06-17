from queue import Queue
import concurrent.futures

queue = Queue()

def task(queue_name:str):
    """
    Decorator to run a function as a background task
    If :: param queue_name :: is not set a random name will be generated
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            queue.put({'func': func, 'args': args, 'kwargs': kwargs})
            
            return True
        return wrapper
    return decorator

class Worker():
    def __init__(self, queue:Queue):
        self.queue = queue

    def start(self) -> None:
        """ Starts a worker in a seprate thread using ThreadPoolExecutor """
        while True:
            if not self.queue.empty():
                task = self.queue.get()
                task['func'](*task['args'], **task['kwargs'])

class Quebes():
    """ 
    Quebes main instance to define settings and starts workers
    :param workers: Number of workers to start
    """
    def __init__(self, max_workers=1):
        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.max_workers = max_workers
    
    def run(self) -> bool:
        """ Starts Quebes Instance """
        try:
            print('\n Quebes is now running... \n')

            # Spin Up Workers
            for _ in range(self.max_workers):
                self.executor.submit(Worker(queue=queue).start)
            
            return True

        except Exception as e:
            print(f'Error starting quebes: {e}')
            
            return False