from collections import defaultdict
import datetime
import json
import sys

import dateutil.parser

import matplotlib.pyplot as plt

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
        task_info[task.taskid]["scheduled"] = task.scheduled.isoformat()
        task_info[task.taskid]["started"] = task.started
        if task_info[task.taskid]["started"]:
            task_info[task.taskid]["started"] = task_info[task.taskid]["started"].isoformat()
        task_info[task.taskid]["resolved"] = task.resolved
        if task_info[task.taskid]["resolved"]:
            task_info[task.taskid]["resolved"] = task_info[task.taskid]["resolved"].isoformat()
        if task.completed:
            task_info[task.taskid]["completed"] = True
            task_info[task.taskid]["wait_time"] = (task.started - task.scheduled).total_seconds()
            task_info[task.taskid]["elapsed"] = (task.resolved - task.started).total_seconds()
        else:
            task_info[task.taskid]["completed"] = False
            task_info[task.taskid]["wait_time"] = None
            task_info[task.taskid]["elapsed"] = None
    print(json.dumps(task_info))

elif sys.argv[1] == "wait_times":
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
            print("{}:".format(type_))
            print("    (mean): {}".format(numpy.mean(wait_times)))
            print("    (25th percentile): {}".format(numpy.percentile(wait_times, 25)))
            print("    (75th percentile): {}".format(numpy.percentile(wait_times, 75)))
            print("    (min): {}".format(min(wait_times)))
            print("    (max): {}".format(max(wait_times)))
    print()
    print("Wait times by worker type:")
    for worker_type, wait_times in wait_time_by_worker.items():
        if wait_times:
            print("{}:".format(worker_type))
            print("    (mean): {}".format(numpy.mean(wait_times)))
            print("    (25th percentile): {}".format(numpy.percentile(wait_times, 25)))
            print("    (75th percentile): {}".format(numpy.percentile(wait_times, 75)))
            print("    (min): {}".format(min(wait_times)))
            print("    (max): {}".format(max(wait_times)))

elif sys.argv[1] == "graphs":
    task_info = json.loads(open(sys.argv[2]).read())

    pending_at_time_by_worker = {}
    running_at_time_by_worker = {}

    for t in task_info:
        task_info[t]["scheduled"] = dateutil.parser.parse(task_info[t]["scheduled"])
        if task_info[t]["started"]:
            task_info[t]["started"] = dateutil.parser.parse(task_info[t]["started"])
        if task_info[t]["resolved"]:
            task_info[t]["resolved"] = dateutil.parser.parse(task_info[t]["resolved"])

    earliest_scheduled = min([i["scheduled"] for i in task_info.values() if i["completed"]])
    latest_completed = max([i["resolved"] for i in task_info.values() if i["completed"]])
    interval = datetime.timedelta(seconds=((latest_completed - earliest_scheduled).total_seconds()) / 50)

    current_period = earliest_scheduled
    while current_period < latest_completed:
        print("Analyzing at time period: {}".format(current_period))
        pending_at_time_by_worker[current_period] = defaultdict(int)
        running_at_time_by_worker[current_period] = defaultdict(int)
        for i in task_info.values():
            worker = i["worker"]
            if i["scheduled"] < current_period and (not i["started"] or i["started"] > current_period):
                pending_at_time_by_worker[current_period][worker] += 1
            elif i["started"] and i["started"] < current_period and (not i["resolved"] or i["resolved"] > current_period):
                running_at_time_by_worker[current_period][worker] += 1

        current_period += interval

    import pprint
    pprint.pprint(pending_at_time_by_worker)
    pprint.pprint(running_at_time_by_worker)
