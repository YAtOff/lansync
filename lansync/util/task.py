from uuid import uuid4


class AsyncTask:
    def __init__(self, func, executor):
        self.func = func
        self.executor = executor

    def run_in_executor(self, *args, **kwargs):
        task_id = uuid4()

        def func(*args, **kwargs):
            return task_id, self.func(*args, **kwargs)

        return task_id, self.executor.submit(func, *args, **kwargs)

    def run_on_current_thread(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self.run_on_current_thread(*args, **kwargs)


def async_task(executor):
    def wrapper(func):
        return AsyncTask(func, executor)

    return wrapper
