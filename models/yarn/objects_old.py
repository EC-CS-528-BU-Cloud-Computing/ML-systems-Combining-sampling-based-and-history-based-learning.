#!/usr/bin/env python

import copy
import logging
from functools import total_ordering
from random import Random

from utils import PEnum
from utils import INVALID_VALUE, PATIENCE_ORACLE_STATE, PATIENCE_ALL_OR_NONE_OPTIONS, PATIENCE_QUEUE_NAMES, PATIENCE_NODE_TYPE, PATIENCE_PARTITION_TUNNING, PATIENCE_COMMON_QUEUE, SCHEDULING_GOALS, PATIENCE_EVICTION_POLICY
from utils import YarnSchedulerType, YARN_EXECUTION_TYPES

YarnSchedulerType = PEnum("YarnSchedulerType", "REGULAR GREEDY SMARTG SYMBEX RACE_LOCKSTEP RACE_CONTINUOUS " +
                          "RACE_JOB RACE_NODEG SRTF PEEK PATIENCE")

LOG = logging.getLogger('yarn_objects')

YarnAMType = PEnum("YarnAMType", "MAPREDUCE GENERIC")
YarnContainerType = PEnum("YarnContainerType", "MRAM MAP REDUCE")
YarnErrorType = PEnum("YarnErrorType", "MIXED POSITIVE NEGATIVE")
YarnErrorMode = PEnum("YarnErrorMode", "CONSTANT RANDOM")
YarnSimulation = PEnum("YarnSimulation", "STANDALONE SYMBEX RACE PEEK")

JOB_RANDOM_SEED = 129831982379


@total_ordering
class YarnResource(object):
    def __init__(self, memory_mb, vcores):
        self.memory_mb = memory_mb
        self.vcores = vcores

    def __copy__(self):
        return YarnResource(self.memory_mb, self.vcores)

    def __deepcopy__(self, memo):
        return self.__copy__()

    def __add__(self, other):
        return YarnResource(self.memory_mb + other.memory_mb, self.vcores + other.vcores)

    def __sub__(self, other):
        return YarnResource(self.memory_mb - other.memory_mb, self.vcores - other.vcores)

    def __iadd__(self, other):
        self.memory_mb += other.memory_mb
        self.vcores += other.vcores
        return self

    def __isub__(self, other):
        self.memory_mb -= other.memory_mb
        self.vcores -= other.vcores
        return self

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        return self.memory_mb == other.memory_mb and self.vcores == other.vcores

    def __le__(self, other):
        return self.memory_mb <= other.memory_mb and self.vcores <= other.vcores

    def __lt__(self, other):
        return self.memory_mb < other.memory_mb or \
               (self.memory_mb == other.memory_mb and self.vcores < other.vcores)

    def __str__(self):
        return "<{}MB, {} cores>".format(self.memory_mb, self.vcores)

    def __repr__(self):
        return self.__str__()


class YarnRack(object):
    def __init__(self, name):
        self.name = name
        self.nodes = set()
        self.capacity = YarnResource(0, 0)
        self.available = YarnResource(0, 0)

        self.main_nodes = set() #These will be repeated nodes
        self.main_node_capacity = YarnResource(0,0)
        #self.main_node_available_capacity = YarnResource(0,0)
        self.main_node_capacity_used_for_main_jobs = YarnResource(0,0)
 
        self.sampling_nodes = set() #These will be repeated nodes
        self.sampling_node_capacity = YarnResource(0,0)
        #self.sampling_node_available_capacity = YarnResource(0,0) 
        self.sampling_node_capacity_used_in_sampling = YarnResource(0,0)
 
        self.thin_nodes = set() #These will be repeated nodes
        self.thin_node_capacity = YarnResource(0,0)
        #self.thin_node_available_capacity = YarnResource(0,0) 
        self.thin_node_capacity_used_for_thin_jobs = YarnResource(0,0)


    def __deepcopy__(self, memo):
        return self

    def add_node(self, node):
        self.nodes.add(node)
        self.capacity += node.capacity
        self.available += node.capacity

    def add_main_node(self, node):
        self.main_nodes.add(node)
        self.main_node_capacity += node.capacity
        #self.main_node_available_capacity += node.capacity

    def add_sampling_node(self, node):
        self.sampling_nodes.add(node)
        self.sampling_node_capacity += node.capacity
        #self.sampling_node_available_capacity += node.capacity
 
    def add_thin_node(self, node):
        self.thin_nodes.add(node)
        self.thin_node_capacity += node.capacity
        #self.thin_node_available_capacity += node.capacity
 
    def get_specific_node_type_resource_availability(self, node_type):
        if node_type == PATIENCE_NODE_TYPE.MAIN.value:
            return self.main_node_capacity - self.main_node_capacity_used_for_main_jobs
        elif node_type == PATIENCE_NODE_TYPE.SAMPLING.value:
            return self.sampling_node_capacity - self.sampling_node_capacity_used_in_sampling
        elif node_type == PATIENCE_NODE_TYPE.THIN.value:
            return self.thin_node_capacity - self.thin_node_capacity_used_for_thin_jobs

    def update_specific_type_resource_availability(self, capacity, node_type, add_remove_multiplier):
        if add_remove_multiplier != 1 and add_remove_multiplier != -1:
            raise Exception("Conflicting parameters add and remove in update_specific_type_resource. add: "+str(add_remove_multiplier))
        if node_type == PATIENCE_NODE_TYPE.MAIN.value:
            if add_remove_multiplier == 1:
                self.main_node_capacity_used_for_main_jobs += capacity
            elif add_remove_multiplier == -1:
                self.main_node_capacity_used_for_main_jobs -= capacity
        elif node_type == PATIENCE_NODE_TYPE.SAMPLING.value:
            if add_remove_multiplier == 1:
                self.sampling_node_capacity_used_in_sampling += capacity
            elif add_remove_multiplier == -1:
                self.sampling_node_capacity_used_in_sampling -= capacity
        elif node_type == PATIENCE_NODE_TYPE.THIN.value:
            if add_remove_multiplier == 1:
                self.thin_node_capacity_used_for_thin_jobs += capacity
            elif add_remove_multiplier == -1:
                self.thin_node_capacity_used_for_thin_jobs -= capacity


class YarnNode(object):
    def __init__(self, name, resource, rack, hb_interval_ms, node_id):
        # Parameters from SLS
        self.capacity = YarnResource(resource.memory_mb, resource.vcores)
        self.rack = rack
        self.name = name
        self.hb_interval_ms = hb_interval_ms
        # Parameters for YARN execution
        self.node_id = node_id
        self.available = copy.copy(resource)
        self.allocated_containers = set()
        self.running_containers = set()
        self.next_heartbeat = None
        self.reserved_application = None
        self.reserved_task_type = None
        self.reserved_start_ms = 0
        self.resource_time_dict = {}    #   AJ_code
        self.state = None   #   AJ_code
        self.node_type = None   #   AJ_code
        self.temp_node_type = None   #   AJ_code    #   This is used when self.node_type is set to UNIVERSAL_NODE and some secondary node type is being assigned.
        self.remote_socket = None #   AJ_code #   This will be used in TESTBED
        self.remote_address = None #   AJ_code #   This will be used in TESTBED


    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_node = YarnNode(self.name, self.capacity, self.rack, self.hb_interval_ms, self.node_id)
        memo[id(self)] = new_node
        new_node.available = YarnResource(self.available.memory_mb, self.available.vcores)
        new_node.allocated_containers = set(copy.deepcopy(container, memo) for container in self.allocated_containers)
        new_node.running_containers = set(copy.deepcopy(container, memo) for container in self.running_containers)
        new_node.next_heartbeat = copy.deepcopy(self.next_heartbeat, memo)
        new_node.reserved_application = copy.deepcopy(self.reserved_application, memo)
        new_node.reserved_task_type = self.reserved_task_type
        new_node.set_state(self.state)  #   AJ_code
        new_node.set_node_type(self.get_node_type())  #   AJ_code
        return new_node

    def __str__(self):
        return "<{}/{}, {} app reserved: {}>".format(self.rack.name, self.name, self.available,
                                                     self.reserved_application)

    def __repr__(self):
        return self.__str__()

    def set_node_type(self, node_type_to_set):  #   AJ_code
        if node_type_to_set not in [PATIENCE_NODE_TYPE.THIN.value, PATIENCE_NODE_TYPE.SAMPLING.value, PATIENCE_NODE_TYPE.MAIN.value, PATIENCE_NODE_TYPE.UNIVERSAL.value]:
            raise Exception("Invalid value for node_type_to_set in set_node_type. node_type_to_set = "+ str(node_type_to_set))
        self.node_type = node_type_to_set

    def get_node_type(self):  #   AJ_code
        return self.node_type
    
    def set_temp_node_type(self, temp_node_type_to_set):  #   AJ_code
        if temp_node_type_to_set not in [PATIENCE_NODE_TYPE.THIN.value, PATIENCE_NODE_TYPE.SAMPLING.value, PATIENCE_NODE_TYPE.MAIN.value]:
            raise Exception("Invalid value for temp_node_type_to_set in set_temp_node_type. temp_node_type_to_set = "+ str(temp_node_type_to_set))
        #if temp_node_type_to_set == PATIENCE_NODE_TYPE.THIN.value:
        #    print "setting thin node.", temp_node_type_to_set
        self.temp_node_type = temp_node_type_to_set

    def get_temp_node_type(self):  #   AJ_code
        return self.temp_node_type

    def is_sampling_node(self):  #   AJ_code
        if self.temp_node_type != None and self.is_universal_node():
            return self.temp_node_type == PATIENCE_NODE_TYPE.SAMPLING.value
        else:
            return self.node_type == PATIENCE_NODE_TYPE.SAMPLING.value
    
    def is_main_node(self):  #   AJ_code
        if self.temp_node_type != None and self.is_universal_node():
            return self.temp_node_type == PATIENCE_NODE_TYPE.MAIN.value
        else:
            return self.node_type == PATIENCE_NODE_TYPE.MAIN.value
    
    def is_universal_node(self):  #   AJ_code
        return self.node_type == PATIENCE_NODE_TYPE.UNIVERSAL.value
    
    def is_thin_node(self):  #   AJ_code
        if self.temp_node_type != None and self.is_universal_node():
            return self.temp_node_type == PATIENCE_NODE_TYPE.THIN.value
        else:
            return self.node_type == PATIENCE_NODE_TYPE.THIN.value

    def set_state(self, state): #   AJ_code
        if self.state != None:
            raise Exception("In node: "+self.name+ " attempt to reset the state.")
        self.state = state
        if self.state.scheduler_type is YarnSchedulerType.PATIENCE and not self.state.isInCoordinatorExecMode():
            if self.state.simulator.getClockMillis() not in self.state.overall_cummulative_time_wise_resource_availability_dict:
                self.state.overall_cummulative_time_wise_resource_availability_dict[self.state.simulator.getClockMillis()] = YarnResource(0,0)
            self.state.overall_cummulative_time_wise_resource_availability_dict[self.state.simulator.getClockMillis()] += self.available 

    def update_task_wise_resource_usage(self, task, allocate = False, finish = False, evict = False):   #   AJ_code #   This function will be called with argument finish = True in case of evict too.
        #   This function is to update resource usage for specific nodes.
        add_remove_sign_multiplier = 0
        if allocate:
            add_remove_sign_multiplier = 1
        elif finish or evict:
            add_remove_sign_multiplier = -1

        to_call_rack_specific_resource_update = False 
        
        if self.is_thin_node() and task.job.get_job_current_queue() == PATIENCE_QUEUE_NAMES.THIN.value:
            to_call_rack_specific_resource_update = True
        if self.is_sampling_node() and task.isSamplingTask():
            to_call_rack_specific_resource_update = True
        if self.is_main_node() and task.job.get_job_current_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
            to_call_rack_specific_resource_update = True


        if to_call_rack_specific_resource_update:
            self.rack.update_specific_type_resource_availability(task.resource, self.node_type, add_remove_sign_multiplier)

    def update_state_resource_dict(self, task, allocate = False, finish = False, evict = False):   #   AJ_code #   This function will be called with argument finish = True in case of evict too.
        if self.state.isInCoordinatorExecMode():    #   This was for allOrNone not it in use now.
            return
        # Currently using exact task duration for finding out resource
        # availability. So only bocking resources while task allocate and no
        # need to do anything on finish.
        resource = task.resource
        duration = task.duration
        if allocate == finish == evict:
            raise Exception ("YarnNode.update_state_resource_dict: "+self.name+" contradictary values of arguments, allocate: "+allocate +", finish: "+finish+" and evict: "+evict)
        timeList = self.state.overall_cummulative_time_wise_resource_availability_dict.keys()
        timeList.sort()
        currentTime = self.state.simulator.getClockMillis()
        endTime = currentTime + duration
        # CleanUp dict:
        for i, time in enumerate(timeList):
            if time < currentTime:
                last_available = self.state.overall_cummulative_time_wise_resource_availability_dict[time]
                del self.state.overall_cummulative_time_wise_resource_availability_dict[time]
            else:
                break
        timeList = timeList[i+1:]

        if evict:
            if currentTime not in self.state.overall_cummulative_time_wise_resource_availability_dict:
                self.state.overall_cummulative_time_wise_resource_availability_dict[currentTime] = last_available + resource
            else:
                self.state.overall_cummulative_time_wise_resource_availability_dict[currentTime] += resource
        elif allocate:
            if currentTime not in self.state.overall_cummulative_time_wise_resource_availability_dict:
                self.state.overall_cummulative_time_wise_resource_availability_dict[currentTime] = last_available
                timeList.insert(0, currentTime)
            equalFound = False
            for i in range(0, len(timeList)):
                time = timeList[i]
                if time == endTime:
                    equalFound = True
                if time < endTime:
                    if allocate:
                        try:
                            self.state.overall_cummulative_time_wise_resource_availability_dict[time] -= resource
                        except KeyError:
                            print "For If2 If: ", time, self.state.overall_cummulative_time_wise_resource_availability_dict
                            print "timeList, currentTime, endTime", timeList, currentTime, endTime
                            import sys
                            sys.exit(0)
                else:
                    if allocate and not equalFound:
                        self.state.overall_cummulative_time_wise_resource_availability_dict[endTime] = YarnResource(0,0) 
                        self.state.overall_cummulative_time_wise_resource_availability_dict[endTime] += self.state.overall_cummulative_time_wise_resource_availability_dict[timeList[i-1]] 
                        self.state.overall_cummulative_time_wise_resource_availability_dict[endTime] += resource 
                    break

        #if time == currentTime:
        #    time_to_set = time
        #    first_index_to_start = i 
        #    equal = True
        #    break
        #elif currentTime < time:
        #    time_to_set = currentTime
        #    first_index_to_start = i 
        #    break
        #

        #time_to_set = None
        #equal = False
        #first_index_to_start = None
        #if currentTime < timeList[0]:
        #    raise Exception("You thought that this will not happen but it did occur. Now tackle this. Go to YarnNode.update_state_resource_dict")
        #    time_to_set = currentTime
        #    first_index_to_start = 0 
        #elif currentTime > timeList[-1]:
        #    time_to_set = currentTime
        #    first_index_to_start = len(timeList) - 1 
        #elif currentTime == timeList[0]:
        #    time_to_set = currentTime
        #    first_index_to_start = 0 
        #    equal = True
        #elif currentTime == timeList[-1]:
        #    time_to_set = currentTime
        #    equal = True
        #    first_index_to_start = len(timeList) - 1 
        #else:
        #    for i in range(1, len(timeList)):
        #        time = timeList[i]
        #        if time == currentTime:
        #            time_to_set = time
        #            first_index_to_start = i 
        #            equal = True
        #            break
        #        elif currentTime < time:
        #            time_to_set = currentTime
        #            first_index_to_start = i 
        #            break

        #for i in range(first_index_to_start, len(timeList)):
        #    if not equal:
        #        equal = True     # Doing this so that program doesn't come to this loop again.
        #        self.state.overall_cummulative_time_wise_resource_availability_dict[time_to_set] = YarnResource(0,0)
        #        self.state.overall_cummulative_time_wise_resource_availability_dict[time_to_set] += self.state.overall_cummulative_time_wise_resource_availability_dict[timeList[first_index_to_start - 1]]
        #        if allocate:
        #            self.state.overall_cummulative_time_wise_resource_availability_dict[time_to_set] -= resource 
        #        if finish:
        #            self.state.overall_cummulative_time_wise_resource_availability_dict[time_to_set] += resource 

        #if self.state.simulator.getClockMillis() not in self.state.overall_cummulative_time_wise_resource_availability_dict:
        #    self.state.overall_cummulative_time_wise_resource_availability_dict[self.state.simulator.getClockMillis()] = YarnResource(0,0)
        #self.state.overall_cummulative_time_wise_resource_availability_dict[self.state.simulator.getClockMillis()] += self.available 
        

    def can_evict_something_for_task(self, new_task):
        #sys.stderr.write("======================= Starting can_evict_something_for_task =======================\n")
        #   Currently this function assumes that only one task can be scheduled on a node. Currently only evicting non-sampling tasks of a job in sampling queue for sampling tasks of another job or tasks of job in main or thin queue
        if self.capacity.vcores > 1: #or (len(self.running_containers) + len(self.allocated_containers)) > 1:
            raise Exception("Following invariant of the function YarnNode.can_evict_something_for_task(self, task) broken. Invariant: Currently this function assumes that only one task can be scheduled on a node.")
        if len(self.running_containers) == 0 and len(self.allocated_containers) == 0:
            raise Exception("In YarnNode.can_evict_something_for_task(self, task). No containers are either scheduled nor running on this node. This function should not have been called.")

        # Since currently assuming only one task per node so simply check if there is an allocated task and if not then try check in running tasks

        #if self.rack.available > self.available:
        #    raise Exception("In YarnNode.can_evict_something_for_task(self, task). This function should not have been called as there is some resource available in the rack: "+ self.rack.available +". So no need to evict.")

        count = 0
        found_container_to_be_evicted = False
        container_to_be_evicted = None
        new_task_current_job_queue = new_task.job.get_job_current_queue()
        new_task_current_job_subqueue = new_task.job.get_job_current_subqueue()
        #sys.stderr.write("Checking if can_evict_something_for_task TEST. Task_job_queue: "+ str(new_task_current_job_queue) + " task_job_subqueue: "+ str(new_task_current_job_subqueue)+"\n")
        while count <2:
            #   Policy 1 -- PREFER TO EVICT ALLOCATED TASKS OVER RUNNING TASKS(Though this is not very important currently might become later.)
            #sys.stderr.write("can_evict_something_for_task. In while loop.\n")
            if count == 0:
                set_to_iterate = self.allocated_containers
                count += 1
            elif count == 1:
                set_to_iterate = self.running_containers
                count += 1
            #raise Exception("Write Condition for no eviction of task of same job.") 
            #raise Exception("Tackle the case when a job moved to Sampling Queue but some of its tasks are running which are non sampling but were scheduled while they were in sampling queue") 
            #sys.stderr.write("can_evict_something_for_task. len(set_to_iterate): "+str(len(set_to_iterate))+" count: "+str(count)+"\n")
            for container_itr in set_to_iterate:
                #sys.stderr.write("can_evict_something_for_task. In for loop.\n")
                # Policy 2 -- DO NOT EVICT A MASTER TASK FOR ANYONE
                candidate = container_itr.task
                candidate_current_job_queue = candidate.job.get_job_current_queue()
                candidate_current_job_subqueue = candidate.job.get_job_current_subqueue()
                if candidate.type is YarnContainerType.MRAM:
                    continue
 
                if self.is_thin_node():
                #    sys.stderr.write("can_evict_something_for_task. Node is thin node\n")
                    if new_task_current_job_queue == PATIENCE_QUEUE_NAMES.THIN.value:
                        if candidate_current_job_queue == PATIENCE_QUEUE_NAMES.THIN.value:
                            continue
                        else:
                            found_container_to_be_evicted = True
                            container_to_be_evicted = container_itr 
                            break
                    else:
                        continue
                elif self.is_sampling_node():
                #    sys.stderr.write("can_evict_something_for_task. Node is sample node\n")
                    if new_task_current_job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value and new_task.isSamplingTask():
                        if candidate_current_job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value and candidate.isSamplingTask():
                            continue
                        else:
                            found_container_to_be_evicted = True
                            container_to_be_evicted = container_itr 
                            break
                    else:
                        continue
                elif self.is_main_node():
                #    sys.stderr.write("can_evict_something_for_task. Node is main node\n")
                    if new_task_current_job_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
                        if candidate_current_job_queue != PATIENCE_QUEUE_NAMES.MAIN.value:
                            found_container_to_be_evicted = True
                            container_to_be_evicted = container_itr 
                            break
                        elif candidate_current_job_subqueue > new_task_current_job_subqueue:
                            found_container_to_be_evicted = True
                            container_to_be_evicted = container_itr 
                            break
                        else:
                            continue
                    else:
                    #elif new_task_current_job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value and new_task.isSamplingTask():
                        continue
                elif self.is_universal_node():
                    raise Exception("You have not yet defined policy for universal node, but the flow has reached there.")
                    pass
                #else:
                #    sys.stderr.write("can_evict_something_for_task. Unidentified node\n")
                #########################################################
                ########### Below is what NOT to evict policy ###########
                #########################################################
                
                # Policy -- ON A THIN NODE DO NOT EVICT A THIN JOB'S TASK
                #elif self.is_thin_node():
                #    if candidate_current_job_queue == PATIENCE_QUEUE_NAMES.THIN.value:
                #        continue
                #    elif candidate_current_job_queue == PATIENCE_QUEUE_NAMES.MAIN.value and new_task_current_job_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
                #        continue
                #    elif candidate_current_job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value and new_task_current_job_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
                #        continue
                #    elif candidate_current_job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value and new_task_current_job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value:
                #        continue
                ## Policy -- ON A SAMPLING NODE DO NOT EVICT A JOB'S SAMPLING TASK
                #elif self.is_sampling_node():
                #    if candidate_current_job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value and candidate.isSamplingTask():
                #        continue
                #    elif new_task_current_job_queue == PATIENCE_QUEUE_NAMES.THIN.value or new_task_current_job_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
                #        continue
                #    elif not new_task.isSamplingTask():
                #        raise Exception("You are working here.")
                ## Policy -- ON A MAIN NODE DO NOT EVICT A MAIN JOB'S TASK
                #elif self.is_main_node():
                #    if candidate_current_job_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
                #        continue
                ## Policy 3 -- DO NOT EVICT A SAMPLING TASK FOR ANYONE ON A SAMPLING OR UNIVERSAL NODE
                #elif self.is_universal_node():
                #    elif candidate.isSamplingTask():
                #        if self.is_sampling_node() or self.is_universal_node():
                #            raise Exception("Check here what to do with different type of new tasks.")
                #            continue
                #    # Policy 4 -- DO NOT EVICT A TASK FROM SAME JOB FOR ANOTHER TASK FROM SAME JOB
                #    elif candidate.job == new_task.job:
                #        continue
                #    # Policy 5 -- DO NOT EVICT A TASK OF A JOB IN THIN QUEUE FROM UNIVERSAL OR THIN_NODE.
                #    elif candidate.job.get_job_current_queue() == PATIENCE_QUEUE_NAMES.THIN.value:
                #        if self.is_thin_node() or self.is_universal_node():
                #            continue
                #    # Policy 6 -- DO NOT EVICT A MAIN QUEUE TASK ONLY UNDER FOLLOWING TWO SUB CONDITIONS.
                #    elif candidate.job.get_job_current_queue() == PATIENCE_QUEUE_NAMES.MAIN.value:
                #        if candidate.getSchedulingQueue() == PATIENCE_QUEUE_NAMES.MAIN.value and new_task_current_job_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
                #            continue    #   THIS WILL BE FURTHER MODIFIED
                #        if self.is_main_node() and (new_task_current_job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value or new_task_current_job_queue == PATIENCE_QUEUE_NAMES.THIN.value):
                #            continue
                #        if self.is_sampling_node() and (new_task_current_job_queue == PATIENCE_QUEUE_NAMES.THIN.value):
                #            continue

                #container_to_be_evicted = container_itr 
                #found_container_to_be_evicted = True
                ##print "EVICTING container: ", container_to_be_evicted, " for task: ", new_task
                #break
                #########################################################
                ########### Above is what NOT to evict policy ###########
                #########################################################
                
            if found_container_to_be_evicted:
            #    sys.stderr.write("can_evict_something_for_task. Found for count = "+str(count)+"\n")
                break
            #else:
            #    sys.stderr.write("can_evict_something_for_task. Nothing found for count = "+str(count)+"\n")
                

        #sys.stderr.write("======================= Ending can_evict_something_for_task =======================\n")
        return found_container_to_be_evicted, container_to_be_evicted

    def evict_container(self, container_to_be_evicted): #   AJ_code
        self.remove_container(container_to_be_evicted, evicting = True)
        if self.state.scheduler_type == YarnSchedulerType.PATIENCE:
            self.update_state_resource_dict(container_to_be_evicted.task, allocate = False, finish = False, evict = True)   #AJ_code


    def book_container(self, container):
        # Adjust available resource
        self.available -= container.resource
        # Adjust rack available resource
        self.rack.available -= container.resource
        # Add container to the list of allocated containers
        self.allocated_containers.add(container)

    def launch_container(self, container):
        if container not in self.allocated_containers:
            LOG.error("Tried to launch unallocated container " + str(container))
            raise Exception("Tried to launch unallocated container")

        # Remove container from allocated list
        self.allocated_containers.remove(container)
        # Add container to running list
        self.running_containers.add(container)

    def remove_container(self, container, evicting = False):
        # Adjust available resource
        self.available += container.resource
        # Adjust rack available resource
        self.rack.available += container.resource
        # Remove container from running list
        if not evicting:        #   AJ_code
            self.running_containers.remove(container)
        elif evicting and container in self.running_containers:        #   AJ_code
            finishEvent = container.getFinishEvent()
            if finishEvent is not None:
                finishEvent.mark_container_is_evicted()
            queueJumpEvent = container.getNextQueueJumpEvent()
            if queueJumpEvent is not None:
                queueJumpEvent.mark_container_is_evicted()
            launchEvent = container.getLaunchEvent()
            if launchEvent is not None:
                launchEvent.mark_container_is_evicted()
            if finishEvent is None and queueJumpEvent is None and launchEvent is None:
                raise Exception("How can all of the finishEvent, queueJumpEvent and launchEvent be None for a running container. Something is wrong here.")
            self.running_containers.remove(container)        #   AJ_code
        elif evicting and container in self.allocated_containers:        #   AJ_code
            launch_event = container.getLaunchEvent()
            if launch_event is not None:
                launch_event.mark_container_is_evicted()
            self.allocated_containers.remove(container)        #   AJ_code
        
        self.update_task_wise_resource_usage(container.task, allocate = False, finish = True, evict = evicting)    #AJ_code


    def get_nodes_cummulative_time_wise_resource_availability_dict(self, policy, current_simulator_time_millis): #   This is the current situation and dictionary might become inconsistent if a new task is scheduled or is evicted. The time in the dict is the absolute simulator time.
            resource_time_dict = {}
            for container in self.running_containers:
            #   Currently the function:
            #   YarnRunningContainer.remaining_duration_ms_from_now(self,
            #   current_simulator_time_millis) is using the exact duration.
            #   However for the purpose of prediction in all-or-none it
            #   should be using the sampled duration. Make that edit after
            #   first testing.
                container_finish_time_from_now = container.remaining_duration_ms_from_now(current_simulator_time_millis)
                absolute_container_finish_time = current_simulator_time_millis + container_finish_time_from_now
                if absolute_container_finish_time not in resource_time_dict:
                    resource_time_dict[absolute_container_finish_time] = YarnResource(0, 0)  
                resource_time_dict[absolute_container_finish_time] += container.resource

             
            for container in self.allocated_containers:
            #   Currently the function:
            #   YarnRunningContainer.remaining_duration_ms_from_now(self,
            #   current_simulator_time_millis) is using the exact duration.
            #   However for the purpose of prediction in all-or-none it
            #   should be using the sampled duration. Make that edit after
            #   first testing.
                container_finish_time_from_now = container.remaining_duration_ms_from_now(current_simulator_time_millis)
                absolute_container_finish_time = current_simulator_time_millis + container_finish_time_from_now
                if absolute_container_finish_time not in resource_time_dict:
                    resource_time_dict[absolute_container_finish_time] = YarnResource(0, 0)  
                resource_time_dict[absolute_container_finish_time] += container.resource
            
            finish_time_list = resource_time_dict.keys()
            finish_time_list.sort()
            for i, time in enumerate(finish_time_list):
                if i == 0:
                    continue
                resource_time_dict[time] += resource_time_dict[finish_time_list[i-1]]
            return resource_time_dict

    def by_what_least_time_needed_resource_will_be_available_on_node(self, resource_needed, policy, current_simulator_time):  #AJ_code. Will return the absolute(in terms of simulator clock) least time in the standard time unit of the simulator(millisecond at the time of writing.) from now when the amount of resources requested will be available on the node. Provided nothing new is scheduled.
        
        time_to_return = current_simulator_time 
        if policy == PATIENCE_ALL_OR_NONE_OPTIONS.NON_EVICTIVE.value:
            if self.capacity < resource_needed:
                return INVALID_VALUE 
            if self.available >= resource_needed:
                return time_to_return
            else:
                resource_time_dict = get_cummulative_time_wise_resource_availability_dict(policy, current_simulator_time_millis) 
                
                finish_time_list = resource_time_dict.keys()
                finish_time_list.sort()
                for time in finish_time_list:
                    if resource_time_dict[time] >= resource_needed:
                        return time
        else:
            raise Exception("by_what_least_time_needed_resource_will_be_available_on_node reached in a non implemented block for the policy: "+str(policy))

    def assignRemoteSocketAndAddress(self, sock, address): #   AJ_code #  TESTBED
        self.remote_socket = sock
        self.remote_address = address
        print "Assigned socket: "+str(sock.getsockname())+" to node: "+str(self)+ " node_id: "+str(self.node_id) + " fileno: "+str(sock.fileno()) + " address: "+ str(address)

    def getRemoteSocketAndAddress(self): #   AJ_code #TESTBED
        return self.remote_socket, self.remote_address
    
    def getRemoteSocket(self): #   AJ_code #TESTBED
        return self.remote_socket
    
    def getRemoteAddress(self): #   AJ_code #TESTBED
        return self.remote_address




class YarnContainer(object):
    def __init__(self, resource=None, priority=None, container_type=None, job=None, duration=0, penalty=None):
        self.duration = duration
        self.resource = resource
        self.priority = priority
        self.type = container_type
        self.job = job
        self.penalty = penalty
        self.tid_within_job = None

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_container = self.__class__()
        memo[id(self)] = new_container
        new_container.duration = self.duration
        new_container.resource = copy.deepcopy(self.resource, memo)
        new_container.priority = copy.deepcopy(self.priority, memo)
        new_container.type = self.type
        new_container.job = copy.deepcopy(self.job, memo)
        new_container.penalty = self.penalty
        return new_container

    def __str__(self):
        return "<{}({}): {}ms, {}, penalty: {}>".format(self.type.name, self.priority,
                                                        self.duration, self.resource, self.penalty)

    def __repr__(self):
        return self.__str__()


class YarnRunningContainer(YarnContainer):
    def __init__(self, container_id=0, resource=None, priority=None, container_type=None, job=None,
                 node=None, task=None, duration=0):
        super(YarnRunningContainer, self).__init__(resource, priority, container_type, job, duration,
                                                   task.penalty if task is not None else None)
        self.id = container_id
        self.node = node
        self.task = task
        self.scheduled_time_millis = -1
        self.launched_time_millis = -1
        self.finished_time_millis = -1
        self.duration_error = 0
        self.finish_event = None
        self.launch_event = None
        self.is_evicted = False
        self.next_queue_jump_event = None
        self.tid_within_job = self.task.tid_within_job

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_container = super(YarnRunningContainer, self).__deepcopy__(memo)
        new_container.id = self.id
        new_container.scheduled_time_millis = self.scheduled_time_millis
        new_container.launched_time_millis = self.launched_time_millis
        new_container.finished_time_millis = self.finished_time_millis
        new_container.node = copy.deepcopy(self.node, memo)
        new_container.task = copy.deepcopy(self.task, memo)

        return new_container

    def __str__(self):
        return "<{} {}>".format(self.name, YarnContainer.__str__(self)[1:-1])

    @property
    def name(self):
        return "{}:{}".format(self.job.name, self.id)

    @property
    def is_elastic(self):
        return self.resource != self.task.ideal_resource

    def schedule_container(self, time_millis):
        self.scheduled_time_millis = time_millis

    def launch_container(self, time_millis):
        self.launched_time_millis = time_millis

    def finish_container(self, time_millis):
        self.finished_time_millis = time_millis

    @property
    def duration_ms(self):
        if self.launched_time_millis == -1:
            raise Exception("Container {}:{} not launched yet.".format(self.job.name, self.id))
        if self.finished_time_millis == -1:
            raise Exception("Container {}:{} not finished yet.".format(self.job.name, self.id))
        return self.finished_time_millis - self.launched_time_millis

    def get_random_error(self, lower_limit, upper_limit):
        random = self.job.regular_random if not self.is_elastic else self.job.elastic_random
        return random.randint(lower_limit, upper_limit)


class YarnPrototypeContainer(YarnContainer):
    def __init__(self, resource=None, priority=None, container_type=None, job=None, duration=0, num_containers=0,
                 ideal_resource=None, penalty=None):
        super(YarnPrototypeContainer, self).__init__(resource, priority, container_type, job, duration, penalty)
        self.num_containers = num_containers
        self.ideal_resource = ideal_resource if ideal_resource is not None else copy.copy(resource)

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_container = super(YarnPrototypeContainer, self).__deepcopy__(memo)
        new_container.num_containers = self.num_containers
        new_container.ideal_resource = copy.deepcopy(self.ideal_resource, memo)
        return new_container

    def __str__(self):
        return "{}, {} tasks{}>".format(
            super(YarnPrototypeContainer, self).__str__()[:-1],
            self.num_containers,
            "" if self.resource == self.ideal_resource else ", ideal: {}".format(self.ideal_resource)
        )


##ADDED CLASS
class YarnPatienceJobEstimate(object):  #   AJ_code
    def __init__(self, job):
        self.sampling_tasks_completion_time = []
        self.sampled_estimated_time = 0
        self.parent_job = job

    def add_estimation_info(self, task, scheduling_goal):
        if task.type is not YarnContainerType.MRAM:
            self.sampling_tasks_completion_time.append(task.duration_ms)
            if scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                try:
                    self.set_sampled_estimated_time(max(task.duration_ms, self.sampled_estimated_time))
                except TypeError:
                    print "In YarnPatienceJobEstimate.add_estimation_info. task.duration_ms: ",task.duration_ms, " type(task.duration_ms): ", str(type(task.duration_ms)), " self.sampled_estimated_time: ", self.sampled_estimated_time, " type(self.sampled_estimated_time): ", str(type(self.sampled_estimated_time))
            elif scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
                self.set_sampled_estimated_time(np.mean(self.sampling_tasks_completion_time))

    def set_sampled_estimated_time(self, estimated_time):
        self.sampled_estimated_time = estimated_time

    def get_estimated_running_time(self, oracle_state, scheduling_goal, secondary_estimate=False):
        if oracle_state == PATIENCE_ORACLE_STATE.NO.value:
            #if self.sampling_tasks_completion_time == []:
            #    return float("inf")
            #if scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
            #    return max(self.sampling_tasks_completion_time)
            #elif scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
            #    return np.mean(self.sampling_tasks_completion_time)
            if self.sampled_estimated_time < 0:
                raise Exception("Negative estimated running_time")
            return self.sampled_estimated_time
        elif oracle_state == PATIENCE_ORACLE_STATE.FULL.value:
            if scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                return self.parent_job.get_max_initial_task_duration_for_all_tasks()
            elif scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
                return self.parent_job.get_average_initial_task_duration_for_all_tasks()
        elif oracle_state == PATIENCE_ORACLE_STATE.THREE_SIGMA.value:
            if secondary_estimate:
                return self.parent_job.get_three_sigma_secondary_predicted_runtime()
            else:
                return self.parent_job.get_three_sigma_predicted_runtime()
        elif oracle_state == PATIENCE_ORACLE_STATE.POINT_MEDIAN.value:
            if secondary_estimate:
                return self.parent_job.get_three_sigma_secondary_predicted_runtime()
            else:
                return self.parent_job.get_three_sigma_predicted_runtime()
        elif oracle_state == PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value:
            return self.get_virtual_estimated_running_time(oracle_state, scheduling_goal)
        elif oracle_state == PATIENCE_ORACLE_STATE.THIN_ONLY_THREE_SIGMA.value:
            if self.sampling_tasks_completion_time == []:
                if secondary_estimate:
                    return self.parent_job.get_three_sigma_secondary_predicted_runtime()
                else:
                    return self.parent_job.get_three_sigma_predicted_runtime()
            else:
                if self.sampled_estimated_time < 0:
                    raise Exception("Negative estimated running_time")
                return self.sampled_estimated_time
        else:
            return None
            #raise Exception("In get_estimated_running_time with oracle state: "+str(oracle_state)+", which is invalid.")
    
    def calculate_virtual_estimated_running_time(self, oracle_state, scheduling_goal):
        if oracle_state == PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value:
            i = 0
            counter = 0
            while counter < self.parent_job.get_num_sampling_tasks():
                task = self.parent_job.pending_tasks[i]
                i += 1
                if task.type is YarnContainerType.MRAM:
                    continue
                self.sampling_tasks_completion_time.append(task.duration)
                counter += 1
            if scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                self.sampled_estimated_time = np.max(self.sampling_tasks_completion_time)
            elif scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
                self.sampled_estimated_time = np.mean(self.sampling_tasks_completion_time)
        else:
            raise Exception("In get_virtual_estimated_running_time with oracle state: "+str(PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value)+", which is invalid.")
 
    def get_virtual_estimated_running_time(self, oracle_state, scheduling_goal):
        if oracle_state == PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value:
            #if self.sampled_estimated_time == 0 and self.parent_job.get_width() >= self.state.thin_limit:
                #raise Exception("In get_virtual_estimated_running_time with oracle state: "+str(oracle_state)+" and scheduling_goal: "+str(scheduling_goal)+" and sampled_estimated_time: "+str(self.sampled_estimated_time)+" which is invalid. "+str(self.parent_job.get_num_sampling_tasks()) +" " +str(self.parent_job.job_id))
            #else:
                return self.sampled_estimated_time
        else:
            raise Exception("In get_virtual_estimated_running_time with oracle state: "+str(PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value)+", which is invalid.")





class YarnJob(object):
    def __init__(self, am_type=None, name=None, job_id=0, start_ms=0, end_ms=0, am_hb_ms=0, tasks=None, after_job=None, ThreeSigmaDummy = False, trace_duration_ms_direct = None, deadline_length=None, job_parent_id=None, job_child_id=None):
        if ThreeSigmaDummy:
            self.job_id = job_id
            temp_duration = trace_duration_ms_direct
            if temp_duration == None:
                #import sys
                #sys.stderr.write("In YarnJob.__init__: temp_duration for job: "+str(job_id)+" is: "+str(temp_duration)+" trace_duration_ms_direct: "+str(trace_duration_ms_direct)+" end_ms: "+str(end_ms)+" start_ms: "+str(start_ms)+"\n")
                temp_duration = end_ms - start_ms
            else:
                self.trace_duration_ms_direct = trace_duration_ms_direct
            self.mark_as_three_sigma_dummy_job(temp_duration)
            return
        # Parameters from SLS trace
        self.am_type = am_type
        self.name = name
        self.job_id = job_id
        self.trace_start_ms = start_ms
        self.trace_end_ms = end_ms
        self.tasks = tasks  # this remains unchanged.
        self.am_hb_ms = am_hb_ms
        self.after_job = after_job
        self.trace_duration_ms_direct = trace_duration_ms_direct
        self.parent_id = job_parent_id
        self.child_id = job_child_id

        # Parameters for YARN execution
        self.consumption = YarnResource(0, 0)
        self.pending_tasks = None  # Gets set in the YarnGenerator. Will be initialized with copy of the tasks, will get modified during execution.
        self.running_tasks = set()  # Has running_tasks
        self.finished_tasks = set()
        self.start_ms = 0
        self.end_ms = 0
        self.yarn_id = 0
        self.am_launched = False
        self.am_next_heartbeat = None
        self.next_container_id = 1
        self.finished = False
        self.elastic_random = Random(JOB_RANDOM_SEED)
        self.regular_random = Random(JOB_RANDOM_SEED)

        # Parameter for Patience policy -- all here is AJ_code
        self.sampling_end_ms = INVALID_VALUE
        self.sampling_start_ms = INVALID_VALUE
        self.current_queue = None   # AJ_code
        self.initial_number_of_tasks = 0    # AJ_code
        self.initial_number_of_total_cores = 0 # AJ_code
        self.all_task_initial_duration = 0  # AJ_code
        self.max_initial_task_duration = 0  # AJ_code
        if self.tasks is not None:
            for task in self.tasks: #   AJ_code
                self.initial_number_of_tasks += (task.num_containers)   #   AJ_code
        self.sampling_tasks_count = 0    #   AJ_code    #   value will be set at job arrival
        self.num_sampling_tasks_assigned = 0    #   AJ_code    #   value will be set at job arrival
        self.num_tasks_finished_in_sampling_queue = 0    #   AJ_code    #   value will be set at job arrival
        self.pending_resource_requirements = None
        self.estimates = YarnPatienceJobEstimate(self)
        self.set_non_evictive_all_or_none_score(INVALID_VALUE)
        self.set_one_time_impact_score(INVALID_VALUE)
        self.current_subqueue = None
        self.total_task_waiting_time = 0
        self.task_waiting_time_list = []
        self.service_attained_so_far = 0
        self.dropped_on_deadline_miss = False
        self.conservatively_dropped_on_deadline_miss = False
        self.not_admitted = False
        self.deadline_length = None    #AJ_code
        self.deadline = None    #AJ_code
        self.set_deadline_length(deadline_length)    #AJ_code
        self.deadline_finish_event = None   #AJ_code
        self.conservative_deadline_finish_event = None   #AJ_code
        self.sp = None  #   This is created for adaptive sampling.  #AJ_code
        #   Parameter for ThreeSigma
        self.three_sigma_dummy_job = False
        self.three_sigma_predicted_runtime = None
        self.three_sigma_secondary_predicted_runtime = None
    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        raise Exception("Deep copy for job is being called however, there is no implementation for copying sampling tasks set.")
        new_job = YarnJob(self.am_type, self.name, self.job_id, self.trace_start_ms, self.trace_end_ms, self.am_hb_ms,
                          self.tasks, self.after_job)
        new_job.start_ms = self.start_ms
        new_job.end_ms = self.end_ms
        new_job.yarn_id = self.yarn_id
        new_job.am_launched = self.am_launched
        new_job.next_container_id = self.next_container_id
        new_job.finished = self.finished

        new_job.parent_id = self.parent_id
        new_job.child_id = self.child_id

        memo[id(self)] = new_job
        new_job.pending_tasks = list(copy.deepcopy(task, memo) for task in self.pending_tasks)
        new_job.consumption = YarnResource(self.consumption.memory_mb, self.consumption.vcores)
        new_job.running_tasks = set(copy.deepcopy(task, memo) for task in self.running_tasks)
        new_job.finished_tasks = self.finished_tasks.copy()
        new_job.am_next_heartbeat = copy.deepcopy(self.am_next_heartbeat, memo)
        new_job.elastic_random = Random(JOB_RANDOM_SEED)
        new_job.regular_random = Random(JOB_RANDOM_SEED)
        new_job.estimates = self.estimates
        new_job.trace_duration_ms_direct = self.trace_duration_ms_direct
        new_job.set_deadline_length(self.get_deadline_length())
        new_job.set_deadline(self.get_deadline())
        new_job.deadline_finish_event = self.deadline_finish_event
        new_job.dropped_on_deadline_miss = self.dropped_on_deadline_miss
        new_job.sp = self.sp
        new_job.not_admitted = self.not_admitted
        new_job.three_sigma_predicted_runtime = self.three_sigma_predicted_runtime
        new_job.three_sigma_secondary_predicted_runtime = self.three_sigma_secondary_predicted_runtime
        if self.get_job_current_queue() == PATIENCE_QUEUE_NAMES.SAMPLE.value:
            print "\n\n\n\n\n\n\n\nIn Deep copy\n\n\n\n\n\n\n\n"
            new_job.set_job_current_subqueue(self.get_job_current_subqueue())
        return new_job

    def get_name(self):
        return "{} app_{:04d}".format(self.name, self.yarn_id)

    def get_new_container_id(self):
        result = self.next_container_id
        self.next_container_id += 1
        return result

    def isSuperParent(self):    #   SuperParent means this job has no parent and is the first job in the dag. If not dag trace then all the jobs will be identified as a super parent.
        if self.parent_id == None:
            return True
        else:
            return False
    
    def isLeaf(self):    #   Leaf means this job has no child and is the last job in the dag. If not dag trace then all the jobs will be identified as a leaf.
        if self.child_id == None:
            return True
        else:
            return False

    def getParentId(self):
        return self.parent_id
    
    def getChildId(self):
        return self.child_id
    
    #AJ_code
    def on_container_schedule(self, container, launch_time):
        waiting_time = None
        if container.task.type is not YarnContainerType.MRAM:
            waiting_time = launch_time - self.start_ms
            self.total_task_waiting_time += (waiting_time)
            self.task_waiting_time_list.append(waiting_time)
            print "waiting_time_details,", self.job_id, ",", container.task.tid_within_job, ",", waiting_time
        #if self.job_id in utils.DEBUG_JID:
            #sys.stderr.write("waiting_time_details job_id: " + str(self.job_id)+" tid_within_job: " +str(container.task.tid_within_job)+" waiting_time: "+str(waiting_time)+" launch_time: "+str(launch_time)+ " self.start_ms: "+str(self.start_ms)+"\n")
 
    #AJ_code
    def get_average_task_waiting_time(self):
        return (float(self.total_task_waiting_time)/float(self.get_job_initial_number_of_tasks()))
    #AJ_code
    def get_max_task_waiting_time(self):
        return max(self.task_waiting_time_list)
    #AJ_code
    def get_first_non_sampling_task_waiting_time(self):
        return self.task_waiting_time_list[int(self.get_num_sampling_tasks())]
    #AJ_code
    def get_first_sampling_task_waiting_time(self):
        return self.task_waiting_time_list[0]
    #AJ_code
    def get_last_non_sampling_task_waiting_time(self):
        return self.task_waiting_time_list[-1]
    #AJ_code
    def get_last_sampling_task_waiting_time(self):
        return self.task_waiting_time_list[int(self.get_num_sampling_tasks()) - 1]
 
    #AJ_code
    def set_sp(self, sp):
        self.sp = sp
 
    #AJ_code
    def get_sp(self):
        return self.sp
 
    #AJ_code
    def sampling_over(self):
        return not(self.get_sampling_end_ms() == INVALID_VALUE)

    #AJ_code
    def get_job_current_queue(self):    #   AJ_code
        #print "In YarnJob.get_job_current_queue: "+str(self.current_queue)+" jobId: "+str(self.job_id)
        return self.current_queue

    def set_job_current_queue(self, queueNum):  #   AJ_code
        self.current_queue = queueNum
 
    def get_job_current_subqueue(self):    #   AJ_code
        if self.get_job_current_queue() != PATIENCE_QUEUE_NAMES.SAMPLE.value:
            #raise Exception("Function get_job_current_subqueue is meant to be for SAMPLING queue only currently. Called for "+str(self.get_name())+" in queue "+str(self.get_job_current_queue()))
            pass
        return self.current_subqueue
    def set_job_current_subqueue(self, subQueueNum):  #   AJ_code
        if self.get_job_current_queue() != PATIENCE_QUEUE_NAMES.THIN.value and self.get_job_current_queue() != PATIENCE_QUEUE_NAMES.SAMPLE.value and self.get_job_current_queue() != PATIENCE_QUEUE_NAMES.MAIN.value:
            raise Exception("Function get_job_current_subqueue is meant to be for SAMPLING or MAIN queue only currently. Called for "+str(self.get_name())+" in queue "+str(self.get_job_current_queue()))
        self.current_subqueue = subQueueNum

    def get_job_initial_number_of_tasks(self):  #   AJ_code
        if self.initial_number_of_tasks == 0:
            for task in self.tasks: #   AJ_code
                if task.type is YarnContainerType.MRAM:
                    continue
                self.initial_number_of_tasks += (task.num_containers)   #   AJ_code
        return self.initial_number_of_tasks
    
    def get_initial_total_number_of_cores(self):  #   AJ_code
        if self.initial_number_of_total_cores == 0:
            for task in self.tasks: #   AJ_code
                if task.type is YarnContainerType.MRAM:
                    continue
                self.initial_number_of_total_cores += (task.resource.vcores)   #   AJ_code
        return self.initial_number_of_total_cores

    #AJ_code
    def get_width(self, widthType="initial_num_tasks"):    #   AJ_code
        if widthType.lower() == "initial_num_cores":
            return self.get_initial_total_number_of_cores()
        if widthType.lower() == "initial_num_tasks":
            return self.get_job_initial_number_of_tasks()
 
    def set_num_sampling_tasks(self, count):    #   AJ_code
        #print self.job_id, count
        self.sampling_tasks_count = count

    def get_num_sampling_tasks(self):    #   AJ_code
        return self.sampling_tasks_count
 
    def get_num_sampling_tasks_assigned(self):    #   AJ_code
        return self.num_sampling_tasks_assigned
 
    def increase_sampling_tasks_assigned(self, by):    #   AJ_code
        self.num_sampling_tasks_assigned += by
        return True
 
    def decrease_sampling_tasks_assigned(self, by):    #   AJ_code
        self.num_sampling_tasks_assigned -= by
        return True

    def get_num_tasks_finished_in_sampling_queue(self):    #   AJ_code
        return self.num_tasks_finished_in_sampling_queue
 
    def increase_num_tasks_finished_in_sampling_queue(self, by):    #   AJ_code
        self.num_tasks_finished_in_sampling_queue += by
        return True
    
    def get_num_sampling_tasks_to_be_assigned(self):    # AJ_code
        return self.get_num_sampling_tasks() - self.get_num_sampling_tasks_assigned()

    def set_sampling_end_ms(self, time_millis):    #   AJ_code
        self.sampling_end_ms = time_millis 
    def get_sampling_end_ms(self):    #   AJ_code
        return self.sampling_end_ms
    
    def set_sampling_start_ms(self, time_millis):    #   AJ_code
        self.sampling_start_ms = time_millis 
    def get_sampling_start_ms(self):    #   AJ_code
        return self.sampling_start_ms
    
    def get_max_initial_task_duration_for_all_tasks(self):   #  AJ_code 
        if self.max_initial_task_duration == 0:
            for task in self.tasks: #   AJ_code
                if task.type is YarnContainerType.MRAM:
                    continue
                if task.duration > self.max_initial_task_duration:
                    self.max_initial_task_duration = task.duration   #   AJ_code
        return self.max_initial_task_duration

    def get_all_task_initial_duration(self):
        if self.all_task_initial_duration == 0:
            for task in self.tasks: #   AJ_code
                if task.type is YarnContainerType.MRAM:
                    continue
                self.all_task_initial_duration += (task.duration)   #   AJ_code
        return self.all_task_initial_duration

    def get_average_initial_task_duration_for_all_tasks(self):   #  AJ_code
        all_task_initial_duration = self.get_all_task_initial_duration()
        return all_task_initial_duration/self.get_job_initial_number_of_tasks()

    def get_pending_job_requirements(self): #   AJ_code
        if self.pending_resource_requirements is None:
            raise Exception("pending_resource_requirements requested before being set.")
        return self.pending_resource_requirements
    
    def set_one_time_impact_score(self, score_to_set):  #   AJ_code
        self.one_time_impact_score = score_to_set
 
    def get_one_time_impact_score(self):   #   AJ_code
        if self.one_time_impact_score == INVALID_VALUE:
            raise Exception("one_time_impact_score requested before being set.")
        return self.one_time_impact_score 
    
    def update_service_attained_so_far(self, service_attained, caller):   #   AJ_code
        if caller != "YarnNodeContainerQueueJumpEvent.handle":
            raise Exception("Might be we are updating the service_attained_so_far more than once. Default caller is: YarnNodeContainerQueueJumpEvent.handle but called from: "+caller+" also")
        self.service_attained_so_far += service_attained
    
    def get_las_score(self):   #   AJ_code
        return self.service_attained_so_far 
    
    def get_current_impact_score(self):   #   AJ_code
        score_to_return = 0
        for task in self.pending_tasks:
            score_to_return += task.getRemainingDuration()
        return score_to_return

    def get_current_estimated_impact_score(self, oracle_state):   #   AJ_code
        score_to_return = 0
        for task in self.pending_tasks:
            score_to_return += task.getEstimatedRemainingDuration(oracle_state)
        return score_to_return
    
    def set_non_evictive_all_or_none_score(self, score_to_set):   #   AJ_code
        self.non_evictive_all_or_none_score = score_to_set
    def get_non_evictive_all_or_none_score(self):   #   AJ_code
        return self.non_evictive_all_or_none_score 
    
    def mark_all_running_tasks_as_non_sampling(self):  #   AJ_code
        for task in self.running_tasks:
            if task.node.is_sampling_node() and task.task.isSamplingTask():
                task.node.rack.update_specific_type_resource_availability(task.resource, task.node.node_type, -1)   #   last argument is -1 when task is freeing up the resource.
            task.task.markIsNotSamplingTask()

    def is_three_sigma_dummy_job(self):  #   AJ_code
        return self.three_sigma_dummy_job
    def mark_as_three_sigma_dummy_job(self, duration):  #   AJ_code
        self.finished = True
        self.three_sigma_dummy_job = True
        self.start_ms = 0
        self.end_ms = duration

    def get_three_sigma_predicted_runtime(self):
        return self.three_sigma_predicted_runtime
    def set_three_sigma_predicted_runtime(self, runtime):
        if self.three_sigma_predicted_runtime == None:
            self.three_sigma_predicted_runtime = runtime
            #This line is printing for maximum task length or job duration from trace. Some traces do not have average task length as value in job.duration field in the traces.#print "3sigma prediction stats for jid: ",str(self.job_id),"," ,self.three_sigma_predicted_runtime, ",", self.trace_duration_ms, ",",float(float(abs(self.three_sigma_predicted_runtime - self.trace_duration_ms))/float(self.trace_duration_ms))*100
        else:
            raise Exception("In YarnJob.set_three_sigma_predicted_runtime: current value is not None. Resseting it.")
    def get_three_sigma_secondary_predicted_runtime(self):
        return self.three_sigma_secondary_predicted_runtime
    def set_three_sigma_secondary_predicted_runtime(self, runtime):
        if self.three_sigma_secondary_predicted_runtime == None:
            self.three_sigma_secondary_predicted_runtime = runtime
        else:
            raise Exception("In YarnJob.set_three_sigma_secondary_predicted_runtime: current value is not None. Resseting it.")
    #def get_three_sigma_score(self):
        #return self.state.three_sigma_predictor.predict(self, early_feedback = utils.EARLY_FEEDBACK)

    @property
    def duration_ms(self):
        #raise Exception("Check if duration_ms function in models/yarn/objects.py needs to be updated.")
        if not self.finished and not (self.dropped_on_deadline_miss or self.not_admitted):
            raise Exception(' '.join(["Job", self.name, "not finished yet."]))
        if self.dropped_on_deadline_miss or self.not_admitted:
            return INVALID_VALUE
        else:
            return self.end_ms - self.start_ms
    
    #@property
    def prediction_metric_ms(self, scheduling_goal):
        if not self.finished:
            raise Exception(' '.join(["Job", self.name, "not finished yet."]))
        if self.is_three_sigma_dummy_job():
            #print "In warm_up for job_id: ",str(self.job_id), " ", str(self.end_ms - self.start_ms)
            return self.end_ms - self.start_ms
        else:
            if scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                dur_to_return = self.get_max_initial_task_duration_for_all_tasks()
            elif scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
                dur_to_return = self.get_average_initial_task_duration_for_all_tasks()
            if dur_to_return == 0 or dur_to_return == None:
                raise Exception("In job_object duration_ms is: "+ str(dur_to_return) +" which is invalid.")
            return dur_to_return

    @property
    def trace_duration_ms(self):
        raise Exception("Check if trace_duration_ms function in models/yarn/objects.py needs to be updated.")
        if self.trace_duration_ms_direct == None:
            return self.trace_end_ms - self.trace_start_ms
        else:
            return self.trace_duration_ms_direct
    
    def set_deadline_length(self, deadline_length): #   deadline_length is duration of deadline from job starting. It is not the absoulute time of deadline. This is needed as deadline is being created from trace.
        self.deadline_length = deadline_length
 
    def get_deadline_length(self):  #   deadline_length is duration of deadline from job starting. It is not the absoulute time of deadline. This is needed as deadline is being created from trace.
        return self.deadline_length
    
    def set_deadline(self, deadline):
        self.deadline = deadline
 
    def get_deadline(self):
        return self.deadline

    #@property
    #def deadline(self):
    #    return self.get_deadline()

    def __str__(self):
        return "<{}, {}, {}, {}, {}>".format(self.get_name(), self.am_type.name, self.parent_id, self.child_id, self.start_ms)
        #return "<{}, {}, [{}]>".format(self.get_name(), self.am_type.name,
        #                               ", ".join(str(task) for task in self.tasks))

    def __repr__(self):
        return self.__str__()

