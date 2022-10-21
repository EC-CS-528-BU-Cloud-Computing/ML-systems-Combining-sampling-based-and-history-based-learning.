#!/usr/bin/env python

import re
import copy

# http://stackoverflow.com/questions/20400818/python-trying-to-deserialize-multiple-json-objects-in-a-file-with-each-object-s
from argparse import ArgumentTypeError

from itertools import islice, chain, izip_longest, count
from more_itertools import peekable
from heapq import heappush, heappop, heapify

# read file in chunks of n lines
from enum import Enum


def lines_per_n(f, n, max_jobs_to_read = float("inf")):
    for line in f:
        if max_jobs_to_read ==0:
            return
        max_jobs_to_read -= 1
        yield ''.join(chain([line], islice(f, n - 1)))
#def lines_per_n(f, n):
#    for line in f:
#        yield ''.join(chain([line], islice(f, n - 1)))

# round up a value by an increment
def divide_and_ceil(a, b):
    if b == 0:
        raise Exception("Invalid divisor")
    return (a + (b - 1)) / b

def round_up(a, b):
    return divide_and_ceil(a, b) * b

# Allow sorting of names in the way that humans expect.
def convert(text): int(text) if text.isdigit() else text

def alphanum_key(key): [convert(c) for c in re.split('([0-9]+)', key)]

# Stable implementation of a single-threaded priority queue
class PQueue(object):
    def __init__(self):
        self.pq = []                         # list of entries arranged in a heap
        self.counter = peekable(count())     # unique sequence count

    def __deepcopy__(self, memo):
        new_pqueue = PQueue()
        memo[id(self)] = new_pqueue
        new_pqueue.pq = []
        for entry in self.pq:
            # noinspection PyArgumentList
            new_pqueue.pq.append([entry[0], entry[1], copy.deepcopy(entry[2], memo)])

        return new_pqueue

    def push(self, value, priority=0):
        idx = next(self.counter)
        entry = [priority, idx, value]
        #print "From PQueue.push. Pushing entry[priority, idx, value]: ", entry
        heappush(self.pq, entry)

    def pop(self):
        if self.pq:
            priority, _, value = heappop(self.pq)
            #print "From PQueue.pop. Returning priority: ", priority, " value: ", value, "_", _
            return priority, value
        else:
            raise KeyError('pop from an empty priority queue')

    def unsafe_remove(self, value):
        # This method breaks the priority queue ordering.
        for item in self.pq:
            if item[2] is value:
                self.pq.remove(item)
                break

    def safe_remove(self, value):
        self.unsafe_remove(value)
        heapify(self.pq)

    @property
    def unordered_values(self):
        # Get an unordered list of values in the priority queue.
        return [x[2] for x in self.pq]

    def empty(self):
        return not self.pq


# http://stackoverflow.com/questions/23028192/how-to-argparse-a-range-specified-with-1-3-and-return-a-list

def parsenumlist(string):
    m = re.match(r'(\d+(?:\.\d+)?)(?:-(\d+(?:\.\d+)?))?$', string)
    # ^ (or use .split('-'). anyway you like.)
    if not m:
        raise ArgumentTypeError("'" + string + "' is not a float range. Expected forms like '0-5.1' or '2'.")
    start = m.group(1)
    end = m.group(2) or start
    if float(start) > float(end):
        raise ArgumentTypeError("Range start must be smaller or equal to range end.")
    return float(start), float(end)


# http://stackoverflow.com/questions/2612720/how-to-do-bitwise-exclusive-or-of-two-strings-in-python
def sxor(s1, s2):
    # convert strings to a list of character pair tuples
    # go through each tuple, converting them to ASCII code (ord)
    # perform exclusive or on the ASCII code
    # then convert the result back to ASCII (chr)
    # merge the resulting array of characters as a string
    return ''.join(chr(ord(a) ^ ord(b)) for a, b in izip_longest(s1, s2, fillvalue=' '))


def get_enumparser(enum, *exclude):
    # noinspection PyTypeChecker
    def enumparser(string):
        name = string.upper()
        if name not in enum.__members__ or name in [e.name for e in exclude]:
            raise ArgumentTypeError(' '.join([string, 'is not one of: '].extend(enum.__members__)))

        return enum.__members__.get(name)

    return enumparser

# Pretty printing of Enum values
class PEnum(Enum):
    def __str__(self):
        return self.name

YarnSchedulerType = PEnum("YarnSchedulerType", "REGULAR GREEDY SMARTG SYMBEX RACE_LOCKSTEP RACE_CONTINUOUS " +
                          "RACE_JOB RACE_NODEG SRTF PEEK PATIENCE")

class PATIENCE_QUEUE_NAMES(Enum):   #AJ_code
    # Keep the number in this order. I might be using them as queue priority at some places.
    THIN = 1
    SAMPLE = 2
    MAIN = 3

class SCHEDULING_GOALS(Enum):  #AJ_code
    AVERAGE_JCT = 0
    DEADLINE = 1
    DEFAULT = AVERAGE_JCT

class PATIENCE_ORACLE_STATE(Enum):  #AJ_code
    NO = 0
    FULL = 1
    THREE_SIGMA = 2
    SAMPLING_ORACLE = 3
    THIN_ONLY_THREE_SIGMA = 4
    #   POINT_MEDIAN will use only one feature. If application name available then will use it else, user_name
    POINT_MEDIAN = 5
    LAS = 6
    DEFAULT = NO

class VCORE_COUNT_SOURCE(Enum): #AJ_code
    ONE = 0 # This is basically constant number of VCORES for each task.
    TRACE = 1
    DEFAULT = ONE

class PATIENCE_ALL_OR_NONE_OPTIONS(Enum):  #AJ_code
    NO = 0
    NON_EVICTIVE = 1
    DEFAULT = NON_EVICTIVE

class PATIENCE_EVICTION_POLICY(Enum):  #AJ_code
    NO = 0
    EVICTIVE = 1
    SPLIT_CLUSTER = 2
    DEFAULT = NO

class PATIENCE_NODE_TYPE(Enum):  #AJ_code
    SAMPLING = 0
    MAIN = 1
    UNIVERSAL = 2
    THIN = 3

class PATIENCE_PARTITION_TUNNING(Enum):   #AJ_code
    MANUAL = 0
    AUTO = 1
    HISTORY = 2
    OFF = 3
    DEFAULT = MANUAL

class PATIENCE_COMMON_QUEUE(Enum):   #AJ_code
    OFF = 0 
    THIN_MAIN = 1 
    DEFAULT = OFF 

class PATIENCE_CLUSTER_SPLIT_OPTIONS(Enum):  #  AJ_code
    NO = 0
    YES = 1
    DEFAULT = NO

class YARN_EXECUTION_TYPES(Enum):   #   AJ_code
    SIMULATOR = 1
    SIMULATION = SIMULATOR

    COORDINATOR = 2
    TESTBED = COORDINATOR

    DEFAULT = SIMULATION

class PATIENCE_ADAPTIVE_SAMPLING_TYPES(Enum):   #   AJ_code
    NO = 0
    YES = 1
    DEFAULT = NO

class YARN_EVENT_TYPES(Enum):   #   AJ_code
    BASE_CLASS = -1

    COORDINATOR_EVENT_LOWER_LIMIT = 0
    COORDINATOR_EVENT_UPPER_LIMIT = 10
    #   Strictly greater than COORDINATOR_EVENT_LOWER_LIMIT and strictly less than COORDINATOR_EVENT_UPPER_LIMIT values are for events triggered by activity at Resource Manager or the coordinator.
    SIMULATION_START = 1
    JOB_ARRIVE = 2
    JOB_FINISH = 3
    SIMULATION_FINISH = 4
    AM_HEARTBEAT = 5
    CONTAINER_LAUNCH = 6
    AM_CONTAINER_FINISH = 7
    UPDATE_NODE_TYPES = 8
    DEADLINE_FINISH = 9

    LOCAL_NODE_EVENT_LOWER_LIMIT = 10
    LOCAL_NODE_EVENT_UPPER_LIMIT = float("inf")
    #   Strictly greater than LOCAL_NODE_EVENT_LOWER_LIMIT values are for events triggered by local node activity.
    NODE_HEARTBEAT = 11
    CONTAINER_QUEUE_JUMP = 13
    CONTAINER_FINISH = 14

class THREESIGMA_FEATURES(Enum):  # AJ_code
    # ALSO SEE the threesigma.py file for POINTMEDIAN case skiping some features.
    JOB_NAME = "job_name"   #Only google11
    # ALSO SEE the threesigma.py file for POINTMEDIAN case skiping some features.
    APPLICATION_NAME = "application_name"   #Only google11
    USER_NAME = "user_name"
    RESOURCE_REQUESTED = "resource_requested"    #Currently a ':' separated entry mem:cpu{This might involve cpu and memory or only one of them.}
    HOUR_OF_THE_DAY = "hour_of_the_day" #   Approximated on hourly basis
    DAY_OF_THE_WEEK = "day_of_the_week"
    #DAY_OF_THE_MONTH = "day_of_the_month"  #Trace analysis only
    #TASK_DUR_COV = "task_dur_cov"  #Trace analysis only
    #MONTH_OF_THE_YEAR = "month_of_the_year"    #Only 2Sigma    #Trace analysis only

class THREESIGMA_METRICS(Enum):  #  AJ_code
    AVERAGE = "average"
    RECENT_MEDIAN = "median"
    ROLLING_AVERAGE = "rolling"
    RECENT_AVERAGE = "recent_average"

class THREESIGMA_UTILITY_FUNCTIONS(Enum):  #    AJ_code
    AVERAGE_JCT = "average_jct"
    MEDIAN_JCT = "median_jct"
    POINT_MEDIAN = "point_median"

class TESTBED_MESSAGE_SEPARATORS(Enum):    #   AJ_code
    MESSAGE_END_CHARACTER = "." #   Multimessage separater
    HEADER_BODY = ";"   #   Level 1
    HEADER_INTERNAL = ":"   #   Level 2
    BODY_INTERNAL = "," #   Level 2

class TESTBED_MESSAGE_TYPES(Enum):    #   AJ_code
    ## Comment below void for now ##
    #   Firstbit = 0 => Instruction 1; => Request
    #   Secondbit and Thirdbit 00 => Void; 01 => Only from coordinator to local; 10 => Only from local to coordinator; 11 => Can go both ways.
    #   Fourthbit and Fifthbit 00 => Void; 01 => Job related; 10 => Task related; 11 => Control related;
    ## Comment above void for now ##
#    NODE_READY = 1
    NODE_HEARTBEAT = 11 #   KEEP IT SIMILAR TO YARN_EVENT_TYPES
    SCHEDULE_TASK = 12  #   KEEP IT SIMILAR TO YARN_EVENT_TYPES
    TASK_FINISH = 14
    START_NODE = 4
    STOP_NODE = 5

class TESTBED_MESSAGE_PROPERTIES(Enum):    #   AJ_code
    pass

class TESTBED_NODE_TYPES(Enum):    #   AJ_code
    DEFAULT = 0

#The values assigned below are based on what JunWoo told me over phone conversations.

DEADLINE_MULTIPLIER = 3.0

THREESIGMA_RECENT_AVERAGE_X = 20    #   AJ_code
THREESIGMA_RECENT_MEDIAN_X = 20    #   AJ_code
THREESIGMA_NUM_BINS = 80    #   AJ_code
THREESIGMA_ROLLING_AVERAGE_ALPHA = 0.6    #   AJ_code
NUM_PAST_ESTIMATE_COUNT_NMAE = 200    #   AJ_code
INVALID_VALUE = -100    #   AJ_code

JOB_RUNTIME_STRETCHER = 1

EARLY_FEEDBACK = True
EARLY_FEEDBACK_FREQ = 3
MAX_POSSIBLE_ESTIMATION_ERROR_FOR_DEADLINE_CHECK = 0

DEBUG_JID = [65353, 11315, 64066, 67654]

DAG_OTHER_STAGES_RUNTIME = "other_stages_runtime"
DAG_STAGE_RUNTIME_WARMUP_SEPARATOR = ":"
