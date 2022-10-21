#!/usr/bin/env python

import copy
import logging
from abc import ABCMeta, abstractmethod

from enum import Enum
from utils import YARN_EVENT_TYPES
from testbed.common import MAX_MSG_LEN, FalseEventTriggerException, NetworkException, MessageFormatException

event_formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s %(name)s CLK: %(clock)-20s %(message)s')
console = logging.StreamHandler()
console.setFormatter(event_formatter)
LOG = logging.getLogger("event")
LOG.addHandler(console)
LOG.propagate = False

EventResult = Enum("EventResult", "CONTINUE FINISHED PAUSE")


class EventLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, event):
        logging.LoggerAdapter.__init__(self, logger, extra={'event': event})

    def process(self, msg, kwargs):
        # Need to do it like this because the clock needs refreshing each time
        self.extra['clock'] = self.extra['event'].state.simulator.getClockMillis()
        return logging.LoggerAdapter.process(self, msg, kwargs)


class Event(object):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        self.time_millis = 0
        self.state = state
        self.log = EventLoggerAdapter(LOG, self)
        self.type = YARN_EVENT_TYPES.BASE_CLASS.value   #   This should be over-written by all the event classes for which you intend to use the name.  #   AJ_code

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_event = self.__class__(copy.deepcopy(self.state, memo))
        new_event.time_millis = self.time_millis
        return new_event

    @abstractmethod
    def handle(self):
        raise NotImplementedError()

 
    def toBeHandledTestForCoordinatorMode(self):    #   AJ_code #   TESTBED #   Call this from all those events which you don't at all want to be triggered from testbed execution mode.
        if self.state.isInCoordinatorExecMode():
            raise FalseEventTriggerException("In coordinator execution mode triggering of this event is false: "+str(self))

    def getType(self):
        return self.type

    def isLocalNodeEvent(self):
        event_type = self.getType()
        if YARN_EVENT_TYPES.LOCAL_NODE_EVENT_LOWER_LIMIT.value < event_type < YARN_EVENT_TYPES.LOCAL_NODE_EVENT_UPPER_LIMIT.value:
            return True
        else:
            return False
    
    def isCoordinatorEvent(self):
        event_type = self.getType()
        if YARN_EVENT_TYPES.COORDINATOR_EVENT_LOWER_LIMIT.value < event_type < YARN_EVENT_TYPES.COORDINATOR_EVENT_UPPER_LIMIT.value:
            return True
        else:
            return False
