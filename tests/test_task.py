from concurrent.futures import Future, Executor

import pytest

from lansync.util.task import Task, TaskList


class StubTask(Task):
    def execute(self, *args, **kwargs):
        if "result" in self.context:
            return self.context["result"]
        elif "error" in self.context:
            raise self.context["error"]
        else:
            return None

    def on_done(self, result):
        self.result = result

    def on_error(self, error):
        self.error = error

    def cleanup(self):
        self.cleanedup = True


class SyncExecutor(Executor):
    def submit(self, fn, *args, **kwargs):
        f = Future()
        try:
            f.set_result(fn(*args, **kwargs))
        except Exception as err:
            f.set_exception(err)

        return f


def test_task_handle_result():
    result = 1
    f = Future()
    f.set_result(result)
    task = StubTask(None)
    task.complete(f)
    assert task.result == result


def test_task_handle_result():
    error = Exception()
    f = Future()
    f.set_exception(error)
    task = StubTask(None)
    task.complete(f)
    assert task.error == error


def test_task_cleanup():
    f = Future()
    f.set_result(None)
    task = StubTask(None)
    task.complete(f)
    assert task.cleanedup


def test_task_list_is_initialy_empty():
    assert TaskList(SyncExecutor()).empty


def test_on_submit_task_list_is_not_empty():
    task_list = TaskList(SyncExecutor())
    task_list.submit(StubTask(None))

    assert not task_list.empty


def test_task_list_executes_tasks_and_waits_for_result():
    task_list = TaskList(SyncExecutor())
    result = 1
    task = StubTask({"result": result})
    task_list.submit(task)
    task_list.wait_any()

    assert task.result == result


def test_task_list_can_wait_for_all_tasks():
    task_list = TaskList(SyncExecutor())
    tasks = [StubTask({"result": i}) for i in range(10)]
    for task in tasks:
        task_list.submit(task)
    task_list.wait_all()

    assert task_list.empty
    assert all(t.result == i for i, t in enumerate(tasks))
