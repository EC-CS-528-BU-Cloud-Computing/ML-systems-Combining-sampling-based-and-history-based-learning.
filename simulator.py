#!/usr/bin/env python

import logging

import time
import sys

from events.event import EventResult

import utils
from utils import PQueue, YARN_EXECUTION_TYPES, YARN_EVENT_TYPES

LOG = logging.getLogger("simulator")
import traceback

class Simulator(object):
    def __init__(self, executionType = YARN_EXECUTION_TYPES.SIMULATION.value):
        self.queue = PQueue()
        if executionType == YARN_EXECUTION_TYPES.SIMULATION.value:
            #self.clock_millis = 0
            self.setClockMillis(0)
        self.start_real_clock_seconds = 0
        self.finish_real_clock_seconds = 0
        self.execution_type = executionType

    def __deepcopy__(self, memo):
        new_simulator = Simulator()
        memo[id(self)] = new_simulator
        new_simulator.queue = self.queue.__deepcopy__(memo)
        #new_simulator.clock_millis = self.clock_millis
        new_simulator.setClockMillis(self.getClockMillis())
        return new_simulator

    @property
    def duration_seconds(self):
        if self.start_real_clock_seconds == 0:
            LOG.error("Simulation has not started yet, cannot compute duration!")
            return 0
        if self.finish_real_clock_seconds == 0:
            LOG.error("Simulation has not finished yet, cannot compute duration!")
            return 0

        return self.finish_real_clock_seconds - self.start_real_clock_seconds

    def setClockMillis(self, new_time):
        self.clock_millis = new_time
 
    def getClockMillis(self):
        return self.clock_millis
 
    def add_event(self, event):
        if event.type == YARN_EVENT_TYPES.CONTAINER_LAUNCH.value:
            if event.yarn_container.job.job_id == utils.DEBUG_JID[0]:
                sys.stderr.write("Pushing event in the queue: "+str(event.time_millis)+"\n")
        self.queue.push(event, event.time_millis)

    def run(self):
        printed_AJLog = 0
        self.start_real_clock_seconds = time.clock()
        LOG.warn("Currently comparing only v.cores for scheduling policy resource consideration. To include memory change the functions in YarnResources object defined in models/yarn/objects.py Total 7 functions changed.")
        try:
            while not self.queue.empty():
                queue_el = self.queue.pop()
                printed_AJLog = 1
                new_clock = queue_el[0]
                event = queue_el[1]
                if new_clock > self.getClockMillis():
                    # Update the internal clock
                    #self.clock_millis = new_clock
                    self.setClockMillis(new_clock)
                elif new_clock < self.getClockMillis() and new_clock != 0:
                    LOG.warn("Parsing event in the past: " + str(new_clock)+ ". Event is: "+str(event))

                if event is None:
                    continue
        
                # Run event callback
                # NOTE: All event handlers should return a tuple (EVENT_RESULT, optional_info) with the first element
                # being the handling result and whether the simulation should continue, and additional information.
                #
                # This is used, for example, to pass information from the simulation to the simulation runner.
                event_return = event.handle()
                try:
                    if event_return[0] is EventResult.FINISHED or event_return[0] is EventResult.PAUSE:
                        return event_return
                except TypeError:
                    print event
                    raise Exception("Invalid event return value"+str(event))

            LOG.warn("Reached queue end without receiving a FINISHED event.")
            return EventResult.FINISHED,
        finally:
            self.finish_real_clock_seconds = time.clock()
