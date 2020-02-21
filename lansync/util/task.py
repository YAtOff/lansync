import abc
from concurrent.futures import (
    Executor, Future, ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
)
import logging
from threading import RLock
from typing import Dict, List


class Task(abc.ABC):
    def __init__(self, context):
        self.context = context

    @abc.abstractmethod
    def execute(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def on_done(self, result):
        pass

    @abc.abstractmethod
    def on_error(self, error):
        pass

    @abc.abstractmethod
    def cleanup(self):
        pass

    def complete(self, future: Future):
        try:
            self.on_done(future.result())
        except Exception as error:
            logging.exception("Error in task")
            self.on_error(error)
        finally:
            self.cleanup()


class TaskList:
    executor: Executor
    tasks: Dict[int, Task]
    futures: List[Future]

    def __init__(self, executor: Executor = None):
        self.executor = executor or ThreadPoolExecutor(max_workers=32)
        self.tasks = {}
        self.futures = []
        self.lock = RLock()

    def submit(self, task: Task, *args, **kwargs):
        with self.lock:
            future = self.executor.submit(task.execute, *args, **kwargs)
            self.futures.append(future)
            self.tasks[id(future)] = task

    def wait_any(self) -> List[Task]:
        with self.lock:
            completed_tasks = []
            completed, _ = wait(self.futures, return_when=FIRST_COMPLETED)
            for future in completed:
                self.futures.remove(future)
                task = self.tasks.pop(id(future))
                task.complete(future)
                completed_tasks.append(task)
            return completed_tasks

    def wait_all(self) -> List[Task]:
        with self.lock:
            for future in as_completed(self.futures):
                self.tasks[id(future)].complete(future)
            self.tasks = {}
            self.futures = []
            return list(self.tasks.values())

    @property
    def empty(self) -> bool:
        return len(self.tasks) == 0
