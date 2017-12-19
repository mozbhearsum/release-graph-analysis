from collections import defaultdict
import json
import sys

import numpy

from taskhuddler import TaskGraph


task_types = (
    "balrog",
    "beetmover-checksums",
    "beetmover-repackage",
    "checksums-signing",
    # Putting this first makes it easy to not match the substring. Horrible hack.
    "nightly-l10n-signing",
    "nightly-l10n",
    "partials-signing",
    "partials",
    "repackage-l10n",
    "repackage-signing",
    "update-verify",
    ""
)


if sys.argv[1] == "gather":
    graph = TaskGraph(sys.argv[2])
    task_info = defaultdict(dict)

    for task in graph.tasks():
        for type_ in task_types:
            if type_ in task.json["task"]["metadata"]["name"]:
                task_info[task.taskid]["type"] = type_
                break

        task_info[task.taskid]["worker"] = task.json["task"]["workerType"]
        if task.completed:
            task_info[task.taskid]["completed"] = True
            task_info[task.taskid]["wait_time"] = (task.started - task.scheduled).total_seconds()
            task_info[task.taskid]["elapsed"] = (task.resolved - task.started).total_seconds()
        else:
            task_info[task.taskid]["completed"] = False
            task_info[task.taskid]["wait_time"] = None
            task_info[task.taskid]["elapsed"] = None
    print(json.dumps(task_info))

elif sys.argv[1] == "report":
    task_info = json.loads(open(sys.argv[2]).read())

    wait_time_by_type = defaultdict()
    wait_time_by_worker = defaultdict()
    for type_ in task_types:
        wait_time_by_type[type_] = [i["wait_time"] for i in task_info.values() if i["completed"] and i["type"] == type_]
    for worker_type in set([i["worker"] for i in task_info.values()]):
        wait_time_by_worker[worker_type] = [i["wait_time"] for i in task_info.values() if i["completed"] and i["worker"] == worker_type]

    print("Wait times by task type:")
    for type_, wait_times in wait_time_by_type.items():
        if wait_times:
            print("{} (mean): {}".format(type_, numpy.mean(wait_times)))
            print("{} (25th percentile): {}".format(type_, numpy.percentile(wait_times, 25)))
            print("{} (75th percentile): {}".format(type_, numpy.percentile(wait_times, 75)))
            print("{} (min): {}".format(type_, min(wait_times)))
            print("{} (max): {}".format(type_, max(wait_times)))
    print()
    print("Wait times by worker type:")
    for worker_type, wait_times in wait_time_by_worker.items():
        if wait_times:
            print("{} (mean): {}".format(worker_type, numpy.mean(wait_times)))
            print("{} (25th percentile): {}".format(worker_type, numpy.percentile(wait_times, 25)))
            print("{} (75th percentile): {}".format(worker_type, numpy.percentile(wait_times, 75)))
            print("{} (min): {}".format(worker_type, min(wait_times)))
            print("{} (max): {}".format(worker_type, max(wait_times)))
