import copy
import logging
import os

from events.event import EventResult
from events.simulation import SimulationStartEvent, SimulationFinishEvent

from events.event import Event
from models.yarn.objects import YarnContainerType, YarnErrorType, YarnErrorMode, YarnSimulation
from stats.stats import StatsWriter, StatsGenerator
import utils
from utils import PATIENCE_ORACLE_STATE, INVALID_VALUE, PATIENCE_QUEUE_NAMES, THREESIGMA_UTILITY_FUNCTIONS, YARN_EXECUTION_TYPES, YARN_EVENT_TYPES, TESTBED_MESSAGE_TYPES, PATIENCE_PARTITION_TUNNING, PATIENCE_CLUSTER_SPLIT_OPTIONS, PATIENCE_NODE_TYPE, PATIENCE_EVICTION_POLICY, SCHEDULING_GOALS
from utils import EARLY_FEEDBACK, EARLY_FEEDBACK_FREQ

class YarnSimulationStartEvent(SimulationStartEvent):
    def __init__(self, state):
        super(YarnSimulationStartEvent, self).__init__(state)
        self.type = YARN_EVENT_TYPES.SIMULATION_START.value

    def activate_jobs(self):
        # Create and add all the YarnJobArrive events to the simulation queue
        for job in self.state.jobs:
            # Some jobs might begin conditionally when other jobs finish
            if job.trace_start_ms == -1:
                continue
            self.state.simulator.add_event(self.state.generator.get_job_arrive_event(job))

    def handle(self):
        if self.log.isEnabledFor(logging.INFO):
            self.log.info("YARN_SIMULATION_STARTED")

        # Mark this as a STANDALONE simulation
        self.state.simulation_type = YarnSimulation.STANDALONE

        # Get the YarnJobs and update the state
        self.activate_jobs()

        # Create the initial node heartbeat events
        delay = 0.0
        delay_inc = 1000.0 / len(self.state.nodes)
        for node in self.state.nodes.values():
            heartbeat = YarnNodeHeartbeatEvent(self.state, node)
            heartbeat.time_millis = int(round(delay))
            delay += delay_inc
            node.next_heartbeat = heartbeat
            self.state.simulator.add_event(heartbeat)

        # Create an initial stats gathering event.
        if self.state.user_config.occupancy_stats_file is not None:
            stats_file_path = self.state.user_config.occupancy_stats_file
            if os.path.exists(stats_file_path):
                os.remove(stats_file_path)
        from events.yarn.occupancy_stats import YarnOccupancyStatsEvent
        occupancy_stats_event = YarnOccupancyStatsEvent(self.state, False)
        self.state.simulator.add_event(occupancy_stats_event)
        self.state.occupancy_stats_event = occupancy_stats_event

        return EventResult.CONTINUE,


class YarnSimulationFinishEvent(SimulationFinishEvent):
    def __init__(self, state):
        super(YarnSimulationFinishEvent, self).__init__(state)

    def cancel_recurring_events(self):
        event_queue = self.state.simulator.queue
        for node in self.state.nodes.values():
            if node.next_heartbeat is not None and node.next_heartbeat.handled is False:
                event_queue.unsafe_remove(node.next_heartbeat)
        if self.state.occupancy_stats_event is not None:
            event_queue.unsafe_remove(self.state.occupancy_stats_event)


    def cancel_deadline_events(self):
        event_queue = self.state.simulator.queue
        counter_temp = 0
        print "len_self.state.jobs: ", str(len(self.state.jobs))
        for job in self.state.jobs:
            if not job.deadline_finish_event.handled:
                counter_temp += 1
                job.deadline_finish_event.handle("YarnSimulationFinishEvent.cancel_deadline_events")
                event_queue.unsafe_remove(job.deadline_finish_event)    #   Removing deadline finish events which will occur in the future.
            if job.conservative_deadline_finish_event is not None:
                if not job.conservative_deadline_finish_event.handled:
                    job.conservative_deadline_finish_event.handle("YarnSimulationFinishEvent.cancel_deadline_events")
                    event_queue.unsafe_remove(job.conservative_deadline_finish_event)    #   Removing deadline finish events which will occur in the future.
        print "counter_temp: ", counter_temp

    def handle(self):
        super(YarnSimulationFinishEvent, self).handle()
        if self.log.isEnabledFor(logging.INFO):
            self.log.info("YARN_SIMULATION_FINISHED")

        self.cancel_recurring_events()

        if self.state.scheduling_goal is SCHEDULING_GOALS.DEADLINE.value:
            self.cancel_deadline_events()
        # Check that the simulation queue is empty.
        if not self.state.simulator.queue.empty() and \
                any(event[2] is not None for event in self.state.simulator.queue.pq):
            self.log.error(
                "Found non-heartbeat event in simulation queue after SIMULATION_FINISHED event: ")

            while not self.state.simulator.queue.empty():
                _, event = self.state.simulator.queue.pop()
                self.log.error(event)

            raise Exception("Invalid event found in queue.")

        return EventResult.FINISHED,


class YarnStandaloneSimulationFinishEvent(YarnSimulationFinishEvent):
    def __init__(self, state):
        YarnSimulationFinishEvent.__init__(self, state)

    def handle(self):
        YarnSimulationFinishEvent.handle(self)
        # Write out all statistics.
        StatsWriter(self.state, StatsGenerator(self.state)).write_stats()
        return EventResult.FINISHED,


SLS_AM_ALLOCATION_TO_CONTAINER_REQ_DELAY = 1000  # ms


class YarnNodeHeartbeatEvent(Event):
    def __init__(self, state, node=None, last_queue_head_job_id=None):
        super(YarnNodeHeartbeatEvent, self).__init__(state)
        self.node = node
        self.launched_containers = set()
        self.finished_containers = set()
        self.handled = False
        self.handled_time_millis = 0
        self.last_queue_head_job_id = last_queue_head_job_id

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_event = super(YarnNodeHeartbeatEvent, self).__deepcopy__(memo)
        new_event.handled = self.handled
        new_event.handled_time_millis = self.handled_time_millis
        new_event.last_queue_head_job_id = self.last_queue_head_job_id
        new_event.node = copy.deepcopy(self.node, memo)
        new_event.launched_containers = set(copy.deepcopy(container, memo) for container in self.launched_containers)
        new_event.finished_containers = set(copy.deepcopy(container, memo) for container in self.finished_containers)
        return new_event

    def __repr__(self):
        launched_containers_str = ",".join(map(str, self.launched_containers))
        finished_containers_str = ",".join(map(str, self.finished_containers))
        return "YARN_NODE_HEARTBEAT: <{} LAUNCHED:<{}>, FINISHED:<{}>>".format(self.node,
                                                                               launched_containers_str,
                                                                               finished_containers_str)

    def generate_next_heartbeat(self):

        # Create the next YarnNodeHeartbeatEvent for this node
        next_node_heartbeat = YarnNodeHeartbeatEvent(self.state, self.node, self.last_queue_head_job_id)
        next_node_heartbeat.time_millis = \
            (self.state.simulator.clock_millis / self.node.hb_interval_ms) * self.node.hb_interval_ms + \
            self.handled_time_millis % self.node.hb_interval_ms
        if next_node_heartbeat.time_millis <= self.state.simulator.clock_millis:
            next_node_heartbeat.time_millis += self.node.hb_interval_ms
        self.node.next_heartbeat = next_node_heartbeat

        self.state.simulator.add_event(next_node_heartbeat)

    def should_generate_next_heartbeat(self, scheduling_result):
        try:
            if not self.state.scheduler.job_queue:
                self.last_queue_head_job_id = None
                return False
        except:
            return False
        if scheduling_result[0]:
            self.last_queue_head_job_id = None
            return True
        if self.state.user_config.use_reservations:
            if self.node.reserved_application is None:
                # NOTE: When using reservations remember the last head-of-queue to avoid creating many duplicate events.
                if self.last_queue_head_job_id is None or \
                                self.last_queue_head_job_id != next(iter(self.state.scheduler.job_queue)).job_id:
                    self.last_queue_head_job_id = next(iter(self.state.scheduler.job_queue)).job_id
                    return True
            else:
                self.last_queue_head_job_id = None

        return False

    def handle(self):
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug(self.__repr__())

        # Mark event as handled.
        self.handled = True
        self.handled_time_millis = self.state.simulator.clock_millis

        # Process finished containers
        for finished_container in self.finished_containers:
            # Update job consumption
            # NOTE: This needs to be done here so that the scheduler correctly accounts for
            # job consumption after the containers are released.
            self.state.scheduler.handle_container_finished(self.node, finished_container)

            if finished_container.type is not YarnContainerType.MRAM:
                job = finished_container.job
                # Generate next AM heartbeat, if needed.
                if job.am_next_heartbeat.handled:
                    job.am_next_heartbeat.generate_next_heartbeat()
                # Add this container to the AM's next heartbeat list of released containers
                job.am_next_heartbeat.released_containers.add(finished_container)

        # Check if this isn't a RACE simulation, and if we reached the cutoff limit.
        if self.state.simulation_type is YarnSimulation.RACE:
            if self.state.race_containers_to_simulate is not None:
                containers_to_simulate = self.state.race_containers_to_simulate

                if self.state.scheduler.accept_decisions >= containers_to_simulate:
                    for job in list(self.state.scheduler.job_queue):
                        if job.am_launched:
                            # Discard remaining containers for all jobs.
                            job.pending_tasks = []
                            self.state.scheduler.job_queue.remove(job)
                        else:
                            # Allow the AM to still be scheduled
                            job.pending_tasks = job.pending_tasks[:1]
                        if job.finished:
                            continue
                        # Generate an AM heartbeat for all jobs.
                        if job.am_next_heartbeat.handled:
                            job.am_next_heartbeat.generate_next_heartbeat()

        # Run the scheduler on this node
        scheduling_result = self.state.scheduler.schedule(self.node)

        # If allocation was possible, generate the next heartbeat (give other jobs
        # the chance to schedule).
        if self.should_generate_next_heartbeat(scheduling_result):
            self.generate_next_heartbeat()

        return scheduling_result[1]


class YarnAMHeartbeatEvent(Event):
    # NOTE: The AM in SLS is a weird little beast. Every heartbeat interval, it processes
    # containers from the _previous_ heartbeat interval. Or rather, it processes the outcome
    # of the allocate() call that was run at the end of the last heartbeat.
    # What this looks like:
    # while (true) {
    #   processPreviousHBReply();
    #   allocate();
    #   sleep(HBInterval);
    # }
    # - Allocated containers:
    #   - Allocated in HB n
    #       - We call these "allocated" here
    #   - Processed and sent to NM in HB n + 1
    #       - We call these "acquired" here
    #
    # - Released containers:
    #   - Only useful to figure out when to finish AM
    #   - Released in HB n
    #       - We call these "released" here
    #   - Counted and potential AM termination sent to NM in HB n + 1
    #       - We call these "finished" here
    #
    # NOTE: At the end, the AM also needs to collect its last container.
    #
    def __init__(self, state, job=None, is_last_heartbeat=False):
        super(YarnAMHeartbeatEvent, self).__init__(state)
        self.job = job
        self.acquired_containers = set()
        self.released_containers = set()
        self.finished_containers = set()
        self.handled = False
        self.handled_time_millis = 0
        self.is_last_heartbeat = is_last_heartbeat

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_event = super(YarnAMHeartbeatEvent, self).__deepcopy__(memo)
        new_event.handled = self.handled
        new_event.handled_time_millis = self.handled_time_millis
        new_event.is_last_heartbeat = self.is_last_heartbeat
        new_event.job = copy.deepcopy(self.job, memo)
        new_event.acquired_containers = set(copy.deepcopy(container, memo) for container in self.acquired_containers)
        new_event.released_containers = set(copy.deepcopy(container, memo) for container in self.released_containers)
        new_event.finished_containers = set(copy.deepcopy(container, memo) for container in self.finished_containers)
        return new_event

    def __repr__(self):
        allocated_containers_str = ""
        if self.job.job_id in self.state.scheduler.allocated_containers:
            allocated_containers_str = ",".join(map(str, self.state.scheduler.allocated_containers[self.job.job_id]))
        acquired_containers_str = ",".join(map(str, self.acquired_containers))
        released_containers_str = ",".join(map(str, self.released_containers))
        finished_containers_str = ",".join(map(str, self.finished_containers))
        return "YARN_AM_HEARTBEAT: <{} ALLOCATED:<{}> ACQUIRED:<{}> RELEASED:<{}> FINISHED:<{}>>".format(
            self.job.get_name(), allocated_containers_str, acquired_containers_str, released_containers_str,
            finished_containers_str
        )

    def generate_next_heartbeat(self, hb_interval_ms=None, is_last_heartbeat=False):
        if hb_interval_ms is None:
            hb_interval_ms = self.job.am_hb_ms

        next_am_heartbeat = YarnAMHeartbeatEvent(self.state, self.job, is_last_heartbeat)
        next_am_heartbeat.time_millis = \
            (self.state.simulator.clock_millis / hb_interval_ms) * hb_interval_ms + \
            self.handled_time_millis % hb_interval_ms
        if next_am_heartbeat.time_millis <= self.state.simulator.clock_millis:
            next_am_heartbeat.time_millis += hb_interval_ms
        self.job.am_next_heartbeat = next_am_heartbeat

        self.state.simulator.add_event(next_am_heartbeat)

    def handle(self):
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug(self)

        # Mark event as handled.
        self.handled = True
        self.handled_time_millis = self.state.simulator.clock_millis

        # Check if the AM has already been launched
        if not self.job.am_launched:
            # Check if the AM has just been allocated
            if self.job.job_id in self.state.scheduler.allocated_containers:
                container_list = list(self.state.scheduler.allocated_containers[self.job.job_id])
                if len(container_list) != 1:
                    self.log.error("More than 1 container allocated for job with no launched AM: " +
                                   "".join(map(str, container_list)))
                    raise Exception("More than 1 container allocated for job with no launched AM.")
                am_container = next(iter(container_list))
                self.state.scheduler.allocated_containers[self.job.job_id] = set()
                # Put this container directly in the node's next heartbeat
                node = am_container.node
                if node.next_heartbeat.handled:
                    # Old heartbeat, generate a new one.
                    node.next_heartbeat.generate_next_heartbeat()
                node.next_heartbeat.launched_containers.add(am_container)
                # Emit YarnContainerLaunchEvents for all launched containers
                # Create a new YarnContainerLaunchEvent
                container_launch = YarnContainerLaunchEvent(self.state, am_container)
                # Add event to the simulator
                self.state.simulator.add_event(container_launch)

                # Mark that the AM is started.
                # This allows other non-AM containers to be considered for scheduling.
                self.job.am_launched = True
                # Generate NodeHeartbeat events for all the nodes.
                for node in self.state.nodes.values():
                    if node.next_heartbeat.handled:
                        node.next_heartbeat.generate_next_heartbeat()
            else:
                # Wait and check again
                self.generate_next_heartbeat(SLS_AM_ALLOCATION_TO_CONTAINER_REQ_DELAY)
                return EventResult.CONTINUE,

        # AM is launched, continue processing the heartbeat

        # Check if the job is done, and launch a YarnJobFinishEvent
        if self.is_last_heartbeat:
            # Mark container as finished
            am_container = next(iter(self.job.running_tasks))
            self.job.running_tasks.remove(am_container)
            self.job.finished_tasks.add(am_container)
            # Generate a YarnJobFinishEvent
            job_finish = YarnJobFinishEvent(self.state, self.job, self.state.simulator.clock_millis)
            self.state.simulator.add_event(job_finish)
            return EventResult.CONTINUE,

        # First process all the containers from the previous heartbeat: ACQUIRED and FINISHED
        # Add all the acquired containers to the heartbeats of their respective nodes
        for acquired_container in self.acquired_containers:
            node = acquired_container.node
            if node.next_heartbeat.handled:
                # Old heartbeat, generate a new one.
                node.next_heartbeat.generate_next_heartbeat()
            node.next_heartbeat.launched_containers.add(acquired_container)
            # Emit YarnContainerLaunchEvents for all launched containers
            # Create a new YarnContainerLaunchEvent
            container_launch = YarnContainerLaunchEvent(self.state, acquired_container)
            # Add event to the simulator
            self.state.simulator.add_event(container_launch)

        # Process all the finished containers
        for container in self.finished_containers:
            # Update job statistics
            self.job.running_tasks.remove(container)
            self.job.finished_tasks.add(container)

        # Next transition the containers that allocate() returns now to the next heartbeat interval
        # This means transitioning ALLOCATED containers to ACQUIRED, and RELEASED containers to FINISHED

        next_hb_acquired_containers = set()
        # Transition all of this job's allocated containers to acquired
        if self.job.job_id in self.state.scheduler.allocated_containers:
            next_hb_acquired_containers = self.state.scheduler.allocated_containers[self.job.job_id]
            self.state.scheduler.allocated_containers[self.job.job_id] = set()

        # Transition all of this job's released containers to finished
        next_hb_finished_containers = self.released_containers

        # Check if all of this job's containers are finished.
        if not self.job.pending_tasks and len(self.job.running_tasks) == 1 and not next_hb_acquired_containers:
            am_container = next(iter(self.job.running_tasks))
            if am_container.type is not YarnContainerType.MRAM:
                self.log.error(
                    "Last running container for job " + self.job.get_name() +
                    " is not an AM container: " + str(am_container))
                raise Exception("Last running container for job is not an AM container.")

            # Create a YarnNodeContainerFinishEvent for the AM container
            am_finish = YarnNodeContainerFinishEvent(self.state, am_container)
            # Add event to the simulator
            self.state.simulator.add_event(am_finish)
            # Generate last heartbeat
            self.generate_next_heartbeat(is_last_heartbeat=True)

        # Create the next YarnAMHeartbeatEvent for this job
        if len(next_hb_acquired_containers) > 0 or len(next_hb_finished_containers) > 0:
            self.generate_next_heartbeat()
            self.job.am_next_heartbeat.acquired_containers = next_hb_acquired_containers
            self.job.am_next_heartbeat.finished_containers = next_hb_finished_containers

        return EventResult.CONTINUE,


class YarnContainerLaunchEvent(Event):
    def __init__(self, state, yarn_container=None):
        super(YarnContainerLaunchEvent, self).__init__(state)
        self.yarn_container = yarn_container

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_event = super(YarnContainerLaunchEvent, self).__deepcopy__(memo)
        new_event.yarn_container = copy.deepcopy(self.yarn_container, memo)
        return new_event

    def __repr__(self):
        return "YARN_CONTAINER_LAUNCHED: {} on node {}.".format(self.yarn_container, self.yarn_container.node)

    def handle(self):
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug(self.__repr__())
        # Launch the container on the node
        self.yarn_container.launch_container(self.state.simulator.clock_millis)
        self.yarn_container.node.launch_container(self.yarn_container)
        # AM containers are not assigned a FinishEvent here, instead they end when all the other containers finish
        if self.yarn_container.type is not YarnContainerType.MRAM:
            # Check if we need to inject a random value in the duration.
            if self.state.user_config.duration_error is not None and \
                    (not self.state.user_config.duration_error_only_elastic or self.yarn_container.is_elastic):
                duration_error_perc = self.state.user_config.duration_error
                duration_error = self.yarn_container.duration * duration_error_perc / 100
                if self.state.user_config.duration_error_mode is YarnErrorMode.CONSTANT:
                    if self.state.user_config.duration_error_type is YarnErrorType.POSITIVE:
                        duration_adjustment = duration_error
                    else:
                        duration_adjustment = -duration_error
                else:
                    lower_limit = -duration_error
                    upper_limit = duration_error
                    if self.state.user_config.duration_error_type is YarnErrorType.POSITIVE:
                        lower_limit = 0
                    elif self.state.user_config.duration_error_type is YarnErrorType.NEGATIVE:
                        upper_limit = 0
                    duration_adjustment = self.yarn_container.get_random_error(lower_limit, upper_limit)
                # Create container finish event on its current node
                container_finish = YarnNodeContainerFinishEvent(self.state, self.yarn_container, duration_adjustment)
                container_finish.time_millis = self.state.simulator.clock_millis + self.yarn_container.duration + \
                    duration_adjustment
            elif self.yarn_container.duration_error != 0:
                # This container has a duration error due to an ideal memory error injection.
                # Create container finish event on its current node
                container_finish = YarnNodeContainerFinishEvent(self.state, self.yarn_container,
                                                                self.yarn_container.duration_error)
                container_finish.time_millis = self.state.simulator.clock_millis + self.yarn_container.duration + \
                    self.yarn_container.duration_error
            else:
                # Create container finish event on its current node
                container_finish = YarnNodeContainerFinishEvent(self.state, self.yarn_container)
                container_finish.time_millis = self.state.simulator.clock_millis + self.yarn_container.duration
            # Add container finish event to simulator
            self.state.simulator.add_event(container_finish)

        # Mark that the state of the cluster has changed.
        self.state.cluster_changed = True

        return EventResult.CONTINUE,


class YarnJobArriveEvent(Event):
    def __init__(self, state, job):
        super(YarnJobArriveEvent, self).__init__(state)
        self.job = job
        self.time_millis = job.trace_start_ms

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_event = super(YarnJobArriveEvent, self).__deepcopy__(memo)
        new_event.job = copy.deepcopy(self.job, memo)
        return new_event

    def __repr__(self):
        return "YARN_JOB_ARRIVED: " + str(self.job)

    def handle(self):
        # Pass on the job to the scheduler
        self.state.scheduler.handle_job_arrived(self.job)

        if self.log.isEnabledFor(logging.INFO):
            self.log.info("YARN_JOB_ARRIVED: " + str(self.job))

        # Create the first YarnAMHeartbeatEvent for this AM.
        am_heartbeat = YarnAMHeartbeatEvent(self.state, self.job)
        am_heartbeat.time_millis = self.state.simulator.clock_millis + SLS_AM_ALLOCATION_TO_CONTAINER_REQ_DELAY
        self.job.am_next_heartbeat = am_heartbeat

        self.state.simulator.add_event(am_heartbeat)

        # Generate NodeHeartbeat events for all the nodes.
        for node in self.state.nodes.values():
            if node.next_heartbeat.handled:
                node.next_heartbeat.generate_next_heartbeat()

        return EventResult.CONTINUE,



#######ADDED CLASS
class YarnDeadlineFinishEvent(Event):
    def __init__(self, state, yarn_job, trigger_time, conservative_drop = False):   #   If and YarnDeadlineFinishEvent is created with conservative_drop = True another YarnDeadlineFinishEvent with conservative_drop = False will also exist and conservative_drop = True event will only do limited things. The False one will do most of the logging.
        super(YarnDeadlineFinishEvent, self).__init__(state)
        self.job = yarn_job
        self.time_millis = trigger_time
        self.type = YARN_EVENT_TYPES.DEADLINE_FINISH.value
        self.handled = False
        self.conservative_drop_event = conservative_drop
    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_event = super(YarnDeadlineFinishEvent, self).__deepcopy__(memo)
        new_event.job = copy.deepcopy(self.job, memo)
        new_event.handled = self.handled
        self.conservative_drop_event = conservative_drop_event
        return new_event

    def __repr__(self):
        return "YARN_DEADLINE_FINISH: " + self.job.get_name()

    def handle(self, caller=""):    #   The parameter caller is being used when the YarnDeadlineFinishEvent is being handled from YarnSimulationFinishEvent. We have to handle this event because for some jobs deadline will occur after entire simulation is over. We need to know the caller to accordingly customize this function's actions.
        self.handled = True
        job_thin_or_wide = ''   #   logging variable
        if self.state.scheduler.oracle == PATIENCE_ORACLE_STATE.NO.value:
            if self.job.get_width(self.state.scheduler.width_type) < self.state.scheduler.thin_limit:
                job_thin_or_wide = " job_type: THIN"
            else:
                job_thin_or_wide = " job_type: WIDE"
        else:
            if self.job.get_width(self.state.scheduler.width_type) < self.state.thin_limit:
                job_thin_or_wide = " job_type: THIN"
            else:
                job_thin_or_wide = " job_type: WIDE"

        if self.conservative_drop_event:
            if not self.state.conservative_drop:
                raise Exception("In non_conservative mode conservative_drop_event called for handling for job: "+str(self.job)+"\n")
            if self.job in self.state.scheduler.running_jobs:
                for task in self.job.pending_tasks:
                    if task.type is not YarnContainerType.MRAM:
                        if task.getTimeExecutedSoFar() == 0:
                            if not self.job.dropped_on_deadline_miss:
                                self.state.wasteful_tasks += len(self.job.finished_tasks)
                                self.state.total_wasteful_work += sum(task.duration for task in self.job.finished_tasks)
                            self.job.dropped_on_deadline_miss = True
                            self.job.conservatively_dropped_on_deadline_miss = True
                            self.state.scheduler.drop_job_on_deadline_miss(self.job)
                            print "Job: "+self.job.get_name() + " missed the deadline conservatively.",job_thin_or_wide
                            break
        else:
            if self.state.scheduling_goal is not SCHEDULING_GOALS.DEADLINE.value:
                raise Exception("Handle YarnDeadlineFinishEvent triggered for job: "+self.job.get_name() + ". However, scheduling goal is: "+str(self.state.scheduling_goal))
            if self.log.isEnabledFor(logging.INFO):
                self.log.info("YARN_DEADLINE_FINISHED: " + self.job.get_name())
            try:
                if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:   #   This if condition is not really needed left it here for searching all DEADLINE related actions.
                    temp_original_duration = self.job.get_max_initial_task_duration_for_all_tasks()
                    temp_estimated_duration = self.job.estimates.get_estimated_running_time(self.state.oracle, self.state.scheduling_goal)
                    temp_estimation_error = abs(float(temp_estimated_duration) - float(temp_original_duration))
                    temp_estimation_error_percentage = (temp_estimation_error)/float(temp_original_duration)*100
                    sys.stdout.write("WAITING_TIME_AND_PREDICTION_ERROR, " + str(self.job.job_id) + "," + str(self.job.get_job_initial_number_of_tasks()) + "," + str(self.job.get_average_task_waiting_time()) + "," + str("-1") + "," + str(temp_original_duration) + "," + str(temp_estimated_duration) + "," + str(temp_estimation_error) + "," + str(temp_estimation_error_percentage)+"\n")   #   Print '-1' in place of job duration as it will not be available for jobs which are not over.
                    #sys.stdout.write("WAITING_TIME_AND_PREDICTION_ERROR, " + str(self.job.job_id) + "," + str(self.job.get_job_initial_number_of_tasks()) + "," + str(self.job.get_average_task_waiting_time()) + "," + str(self.job.duration_ms) + "," + str(temp_original_duration) + "," + str(temp_estimated_duration) + "," + str(temp_estimation_error) + "," + str(temp_estimation_error_percentage)+"\n")
            except ValueError or KeyError:
               pass
            if self.job.not_admitted:
                print "Job: "+self.job.get_name() + " was not admitted.",job_thin_or_wide 
            elif self.job in self.state.scheduler.running_jobs:
                self.state.scheduler.drop_job_on_deadline_miss(self.job)
                print "Job: "+self.job.get_name() + " missed the deadline.",job_thin_or_wide
                if not self.job.dropped_on_deadline_miss:
                    self.state.total_wasteful_work += sum(task.duration for task in self.job.finished_tasks)
                    self.state.wasteful_tasks += len(self.job.finished_tasks)
                self.job.dropped_on_deadline_miss = True
            else:
                print "Job: "+self.job.get_name() + " finished within the deadline.",job_thin_or_wide
                self.state.total_work_done_within_deadline += sum(task.duration for task in self.job.finished_tasks)
                self.state.successful_tasks += len(self.job.finished_tasks)
        if caller != "YarnSimulationFinishEvent.cancel_deadline_events":
            if self.state.scheduler.all_jobs_are_done():
                # Add a YarnSimulationFinishEvent
                self.state.simulator.add_event(self.state.generator.get_simulation_finish_event())
        return EventResult.CONTINUE,






class YarnJobFinishEvent(Event):
    def __init__(self, state, yarn_job=None, finish_time=0):
        super(YarnJobFinishEvent, self).__init__(state)
        self.job = yarn_job
        self.time_millis = finish_time

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_event = super(YarnJobFinishEvent, self).__deepcopy__(memo)
        new_event.job = copy.deepcopy(self.job, memo)
        return new_event

    def __repr__(self):
        return "YARN_JOB_FINISHED: " + self.job.get_name()

    def handle(self):
        if self.log.isEnabledFor(logging.INFO):
            self.log.info("YARN_JOB_FINISHED: " + self.job.get_name())

        self.job.finished = True
        # Update scheduler info
        self.state.scheduler.handle_job_completed(self.job)
        # Update job statistics
        self.job.end_ms = self.state.simulator.clock_millis
        # Check if the ending of this job doesn't trigger the beginning of another.
        if self.state.simulation_type is YarnSimulation.STANDALONE:
            for job in self.state.jobs:
                if job.after_job == self.job.job_id:
                    job.trace_start_ms = self.state.simulator.clock_millis
                    self.state.simulator.add_event(YarnJobArriveEvent(self.state, job))
        # Check if the simulation is done
        if not self.state.scheduler.all_jobs_are_done():
            # Not all jobs finished yet, continue
            return EventResult.CONTINUE,
        else:
            # Add a YarnSimulationFinishEvent
            self.state.simulator.add_event(self.state.generator.get_simulation_finish_event())

        return EventResult.CONTINUE,


class YarnNodeContainerFinishEvent(Event):
    # Event triggered when the NM becomes aware of the container finishing
    def __init__(self, state, yarn_container=None, duration_error=0):
        super(YarnNodeContainerFinishEvent, self).__init__(state)
        self.yarn_container = yarn_container
        self.duration_error = duration_error

    @property
    def node(self):
        return self.yarn_container.node

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_event = super(YarnNodeContainerFinishEvent, self).__deepcopy__(memo)
        new_event.yarn_container = copy.deepcopy(self.yarn_container, memo)
        return new_event

    def __repr__(self):
        return "YARN_NODE_CONTAINER_FINISHED: " + str(self.yarn_container) + \
               " on node " + str(self.node) + " actual duration: " + str(
            self.state.simulator.clock_millis - self.yarn_container.launched_time_millis)

    def handle(self):
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("YARN_NODE_CONTAINER_FINISHED: " + str(self.yarn_container) +
                           " on node " + str(self.node) + " actual duration: " + str(
                self.state.simulator.clock_millis - self.yarn_container.launched_time_millis))

        # Mark container as finished
        self.yarn_container.finish_container(self.state.simulator.clock_millis)
        # Remove container from node
        self.yarn_container.node.remove_container(self.yarn_container)

        # Add this container to the node's next heartbeat list of finished containers
        if self.node.next_heartbeat is None:
            self.log.error("Container finished on node that has no upcoming heartbeat.")
            raise Exception("Container finished on node that has no upcoming heartbeat.")

        if self.node.next_heartbeat.handled:
            self.node.next_heartbeat.generate_next_heartbeat()
        self.node.next_heartbeat.finished_containers.add(self.yarn_container)

        # Mark that the state of the cluster has changed.
        self.state.cluster_changed = True

        return EventResult.CONTINUE,



class UpdateNodeTypesEvent(Event):  # For each break execution allocate a new container
    def __init__(self, state, trigger_time, trigger_period):
        super(UpdateNodeTypesEvent, self).__init__(state)
        if self.state.auto_tunable_partition != PATIENCE_PARTITION_TUNNING.HISTORY.value:
            raise Exception("A UpdateNodeTypesEvent should only be created for PATIENCE_PARTITION_TUNNING.HISTORY.value. Current auto_tunable_partition = "+str(self.state.auto_tunable_partition))
        self.type = YARN_EVENT_TYPES.UPDATE_NODE_TYPES.value
        self.time_millis = trigger_time
        self.trigger_period = trigger_period

    def handle(self):
        new_thin_node_fraction = self.get_new_thin_node_fraction()

        for rack in self.state.racks:
            num_sampling_node = 0
            if self.state.sampling_node_percentage >0:
                new_sampling_node_fraction = self.state.sampling_node_percentage
                num_sampling_node = (len(self.state.racks[rack].nodes)*new_sampling_node_fraction)/100
            num_thin_node = 1*math.ceil((len(self.state.racks[rack].nodes)*new_thin_node_fraction)/100)
            if num_thin_node < 1:
                num_thin_node = 1
            count = 0
            print "UpdateNodeTypesEvent. num_thin_node: ", num_thin_node, " num_sampling_node: ", num_sampling_node
            for node in self.state.racks[rack].nodes:
                if self.state.split_cluster == PATIENCE_CLUSTER_SPLIT_OPTIONS.NO.value or self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value:
                    node.set_node_type(PATIENCE_NODE_TYPE.UNIVERSAL.value)
                elif self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.HISTORY.value:
                    if count < num_thin_node:
                        node.set_node_type(PATIENCE_NODE_TYPE.THIN.value)
                        if self.state.eviction_policy != PATIENCE_EVICTION_POLICY.NO.value:
                            raise Exception("Nodes being update with eviction_policy: "+str(self.state.eviction_policy)+". This is not permissible. Please read the comments below in the source file.")
                        ##  This is being incomplete. As we need to update state of the rack object. This will work fine with the NO eviction policy. But if we evict this needs to be handled. 
                        #self.state.racks[rack].add_thin_node(node)
                    elif count < num_sampling_node + num_thin_node:
                        node.set_node_type(PATIENCE_NODE_TYPE.SAMPLING.value)
                        ##  This is being incomplete. As we need to update state of the rack object. This will work fine with the NO eviction policy. But if we evict this needs to be handled. 
                        #self.state.racks[rack].add_sampling_node(node)
                    else:
                    #elif count > num_thin_node:
                    #elif count > num_sampling_node and count < (num_sampling_node + num_thin_node):
                        node.set_node_type(PATIENCE_NODE_TYPE.MAIN.value)
                        ##  This is being incomplete. As we need to update state of the rack object. This will work fine with the NO eviction policy. But if we evict this needs to be handled. 
                        #self.state.racks[rack].add_main_node(node)
                else:
                    raise Exception("A UpdateNodeTypesEvent should only be handled for PATIENCE_PARTITION_TUNNING.HISTORY.value. Current auto_tunable_partition = "+str(self.state.auto_tunable_partition)) 
                count += 1
        
        self.generate_next_node_type_update_event()
        return EventResult.CONTINUE,

    def get_new_thin_node_fraction(self):
        thin_task_measure = self.state.scheduler.unscheduled_thin_job_tasks
        main_task_measure = self.state.scheduler.unscheduled_main_job_tasks + self.state.scheduler.unscheduled_sampling_tasks
        to_return = float(thin_task_measure)/float(thin_task_measure + main_task_measure)
        #if to_return <= 0:
        #    print "Negative new_thin_node_fraction - thin_task_measure: ", thin_task_measure, " num_main_tasks: ", self.state.scheduler.unscheduled_main_job_tasks, " num_sampling_tasks: ", self.state.scheduler.unscheduled_sampling_tasks
        return to_return

    def generate_next_node_type_update_event(self):
        # Create the next UpdateNodeTypesEvent for this node
        next_node_type_update_event_trigger_time = self.time_millis + self.trigger_period
        next_node_type_update_event = UpdateNodeTypesEvent(self.state, next_node_type_update_event_trigger_time, self.trigger_period)
        self.state.simulator.add_event(next_node_type_update_event)

