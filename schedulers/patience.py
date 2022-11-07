import logging

from events.event import EventResult
from models.yarn.objects import YarnContainerType, YarnResource
from schedulers.yarn import YarnScheduler, LOG
from schedulers.srtf import YarnSRTFScheduler
from utils import PATIENCE_QUEUE_NAMES, PATIENCE_NODE_TYPE, PATIENCE_ORACLE_STATE, PATIENCE_ALL_OR_NONE_OPTIONS, PATIENCE_EVICTION_POLICY, THREESIGMA_UTILITY_FUNCTIONS, INVALID_VALUE, PATIENCE_PARTITION_TUNNING, PATIENCE_COMMON_QUEUE, SCHEDULING_GOALS, MAX_POSSIBLE_ESTIMATION_ERROR_FOR_DEADLINE_CHECK
from utils import YarnSchedulerType, EARLY_FEEDBACK, EARLY_FEEDBACK_FREQ, PATIENCE_ADAPTIVE_SAMPLING_TYPES
import sys
import numpy as np
import math
from events.yarn.yarn import UpdateNodeTypesEvent, YarnDeadlineFinishEvent
import utils
#sys.path.insert(0,'/home/ajajoo/analysis')
#import my
DEFAULT_SAMPLING_PERCENTAGE = 5.0
DEFAULT_THIN_LIMIT = 3

DEFAULT_NUM_THIN_QUEUES = 10
#DEFAULT_FIRST_THIN_QUEUE_THRESHOLD = 10**6#float("inf")#
DEFAULT_FIRST_THIN_QUEUE_THRESHOLD = int(((10**5)))#10**6#float("inf")#
DEFAULT_FIRST_THIN_QUEUE_THRESHOLD = int(DEFAULT_FIRST_THIN_QUEUE_THRESHOLD*utils.JOB_RUNTIME_STRETCHER)
print "DEFAULT_FIRST_THIN_QUEUE_THRESHOLD is: ", DEFAULT_FIRST_THIN_QUEUE_THRESHOLD
DEFAULT_THIN_QUEUE_THRESHOLD_MULTIPLIER = 10#5#
FIRST_THIN_SUBQUEUE = 0

DEFAULT_NUM_SAMPLING_QUEUES = 10
DEFAULT_FIRST_SAMPLING_QUEUE_THRESHOLD = float("inf")#10**6#
DEFAULT_FIRST_SAMPLING_QUEUE_THRESHOLD = DEFAULT_FIRST_SAMPLING_QUEUE_THRESHOLD*utils.JOB_RUNTIME_STRETCHER
print "DEFAULT_FIRST_SAMPLING_QUEUE_THRESHOLD is: ", DEFAULT_FIRST_SAMPLING_QUEUE_THRESHOLD
DEFAULT_SAMPLING_QUEUE_THRESHOLD_MULTIPLIER = 10
FIRST_SAMPLING_SUBQUEUE = 0

#   CHANGE BELOW    #
DEFAULT_NUM_MAIN_QUEUES = 10
#   CHANGE ABOVE    #
#DEFAULT_FIRST_MAIN_QUEUE_THRESHOLD = 10**6#float("inf")#    #   Please check if in the main_queue_handler the main subqueues are being sorted or not. If limit is Inf you want them to be sorted.
DEFAULT_FIRST_MAIN_QUEUE_THRESHOLD = int(((10**6)))#10**6#float("inf")#    #   Please check if in the main_queue_handler the main subqueues are being sorted or not. If limit is Inf you want them to be sorted.
DEFAULT_FIRST_MAIN_QUEUE_THRESHOLD = int(DEFAULT_FIRST_MAIN_QUEUE_THRESHOLD*utils.JOB_RUNTIME_STRETCHER)
print "DEFAULT_FIRST_MAIN_QUEUE_THRESHOLD is: ", DEFAULT_FIRST_MAIN_QUEUE_THRESHOLD
DEFAULT_MAIN_QUEUE_THRESHOLD_MULTIPLIER = 10#5#
FIRST_MAIN_SUBQUEUE = 0

class YarnPatienceScheduler(YarnScheduler):
    def __init__(self, state):
        #print "Abhi toh party shuru hui hai!"
        YarnScheduler.__init__(self, state)
        self.job_thin_queue = []
 
        self.num_main_queue = DEFAULT_NUM_MAIN_QUEUES
        self.first_main_queue_threshold = DEFAULT_FIRST_MAIN_QUEUE_THRESHOLD
        self.main_queue_threshold_multiplier = DEFAULT_MAIN_QUEUE_THRESHOLD_MULTIPLIER
        self.job_main_queue = [[] for i in xrange(self.num_main_queue)]
        self.job_main_sub_queue_limits = [(self.first_main_queue_threshold)*(self.main_queue_threshold_multiplier**i) for i in xrange(self.num_main_queue - 1)]
        self.job_main_sub_queue_limits.append(float("inf"))   # last queue has no limit
        print self.job_main_sub_queue_limits

        self.num_sampling_queue = DEFAULT_NUM_SAMPLING_QUEUES
        self.first_sampling_queue_threshold = DEFAULT_FIRST_SAMPLING_QUEUE_THRESHOLD
        self.sampling_queue_threshold_multiplier = DEFAULT_SAMPLING_QUEUE_THRESHOLD_MULTIPLIER
        self.job_sample_queue = [[] for i in xrange(self.num_sampling_queue)]
        self.job_sample_sub_queue_limits = [(self.first_sampling_queue_threshold)*(self.sampling_queue_threshold_multiplier**i) for i in xrange(self.num_sampling_queue - 1)]
        self.job_sample_sub_queue_limits.append(float("inf"))   # last queue has no limit
 
        self.num_thin_queue = DEFAULT_NUM_THIN_QUEUES
        self.first_thin_queue_threshold = DEFAULT_FIRST_THIN_QUEUE_THRESHOLD
        self.thin_queue_threshold_multiplier = DEFAULT_THIN_QUEUE_THRESHOLD_MULTIPLIER
        self.job_thin_queue = [[] for i in xrange(self.num_thin_queue)]
        self.job_thin_sub_queue_limits = [(self.first_thin_queue_threshold)*(self.thin_queue_threshold_multiplier**i) for i in xrange(self.num_thin_queue - 1)]
        self.job_thin_sub_queue_limits.append(float("inf"))   # last queue has no limit

        if state.user_config.sampling_percentage is None:
            self.sampling_percentage = 0
        else:
            self.sampling_percentage = float(state.user_config.sampling_percentage)
        self.constant_sampling = 0
        if self.sampling_percentage == 0.0:
            self.constant_sampling = 2
        self.thin_limit = state.user_config.thin_limit
        self.oracle = state.user_config.oracle
        self.width_type = "initial_num_tasks"
        self.job_scores = {}    #   This is for patience all or none currently. Will have job scores in terms of how many other jobs they are impacting
        self.all_or_none = state.user_config.all_or_none
        self.main_queue_job_wise_pending_resource_requirements_dict = {}
        self.debug_list = []
        self.log_printed_time_dict = {}
        self.firstJob = True
        self.dropped_jobs = set()
        self.not_admitted_jobs = set()

        ####   adaptive_sampling_parameters ####
        self.default_adapt_sampling_percentage = self.sampling_percentage#state.adapt_curr_sampling_percentage#5#
        self.adapt_sampling_pecentage_lower_limit = 1#self.sampling_percentage#
        self.adapt_sampling_pecentage_upper_limit = 5
        #self.adapt_sampling_pecentage_range = [i for i in range(self.adapt_sampling_pecentage_lower_limit, self.adapt_sampling_pecentage_upper_limit + 1)]
        self.adapt_sampling_pecentage_lower_switch_delta = 0.10    #   This is percentage
        self.adapt_sampling_pecentage_upper_switch_delta = 0.10    #   This is percentage
        self.sp_nrmlzd_avg_accuracy_map = {}
        self.sp_nrmlzd_avg_accuracy_map_job_count = {}
        self.sp_nrmlzd_avg_completion_time_map = {}
        self.sp_nrmlzd_avg_completion_time_map_job_count = {}
        self.sp_jid_nrmlzd_avg_completion_time_map = {}
        self.num_jobs_assigned_adaptive_sampling_rate = 0
        self.adapt_sampling_min_required_samples = 20
        self.adapt_sampling_max_past_samples = 120

        if self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value or \
           self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
            self.total_node_count = 0
            for rack in self.state.racks:
                self.total_node_count += len(self.state.racks[rack].nodes)

            if self.state.eviction_policy != PATIENCE_EVICTION_POLICY.NO.value:
                raise Exception("self.state.auto_tunable_partition = " + str(self.state.auto_tunable_partition) + "self.state.eviction_policy = " + str(self.state.eviction_policy))
            #self.current_thin_node_count = 0 self.current_sampling_node_count = 0 self.current_main_node_count = 0
            self.auto_tunned_node_types = {}
            for node_type in PATIENCE_NODE_TYPE:
                self.auto_tunned_node_types[node_type.value] = 0.0    #   This is being dealt in the code with assumption that only one task will be scheduled per node.
        if self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value or \
           self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.HISTORY.value or \
           self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
            self.unscheduled_thin_job_tasks = 0.0
            self.unscheduled_sampling_tasks = 0.0
            self.unscheduled_main_job_tasks = 0.0

        if self.state.user_config.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value:
            self.prediction_utility_function = THREESIGMA_UTILITY_FUNCTIONS.AVERAGE_JCT.value
        elif self.oracle == PATIENCE_ORACLE_STATE.POINT_MEDIAN.value:
        #   This will use only one feature. If application name is available then will use it else, user_name
            self.prediction_utility_function = THREESIGMA_UTILITY_FUNCTIONS.POINT_MEDIAN.value
        elif self.oracle == PATIENCE_ORACLE_STATE.LAS.value:
            if self.state.common_queue != PATIENCE_COMMON_QUEUE.OFF.value:
                raise NotImplementedError()
        else:
            if self.state.dag_scheduling:
                self.prediction_utility_function = THREESIGMA_UTILITY_FUNCTIONS.AVERAGE_JCT.value
            else:
                self.prediction_utility_function = None

        if self.thin_limit < 0 or self.sampling_percentage < 0:
            print "Error in parameters: thin_limit = "+str(self.thin_limit)+" sampling_percentage = " + str(self.sampling_percentage)
            sys.exit(0)
        if self.oracle != PATIENCE_ORACLE_STATE.NO.value and self.oracle != PATIENCE_ORACLE_STATE.FULL.value and self.oracle != PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value and self.oracle != PATIENCE_ORACLE_STATE.THREE_SIGMA.value and self.oracle != PATIENCE_ORACLE_STATE.THIN_ONLY_THREE_SIGMA.value and self.oracle != PATIENCE_ORACLE_STATE.POINT_MEDIAN.value and self.oracle != PATIENCE_ORACLE_STATE.LAS.value:
            print "Error in parameters: oracle = "+str(self.oracle)
            sys.exit(0)
        print "In YarnPatienceScheduler thin_limit is: ", self.thin_limit
        print "In YarnPatienceScheduler sampling_percentage is: ", self.sampling_percentage, " constant_sampling is: ", self.constant_sampling
        print "In YarnPatienceScheduler oracle state is: ", self.oracle
        print "In YarnPatienceScheduler all_or_none state is: ", self.all_or_none
        self.state.starvation_freedom = False#True#
        self.starve_queue_alloc_count = {}
        self.starve_queue_alloc_count_limit = {}
        self.reset_current_starve_queue_alloc_count()
        for i in range(self.num_main_queue):
            if i == 0:
                self.starve_queue_alloc_count_limit[i] = 100
            elif i == 1:
                self.starve_queue_alloc_count_limit[i] = 10
            else:
                self.starve_queue_alloc_count_limit[i] = 1
        if self.state.user_config.common_queue == PATIENCE_COMMON_QUEUE.THIN_MAIN.value:
            print "Common queue structure. THIN_MAIN. Main queue threshold limits being used for thin jobs as well. The policy should also be non_evictive."
        print "FREQ = ", str(EARLY_FEEDBACK_FREQ)

    #def compute_job_score(job):
    #    score = 0
    #    for task in job.pending_tasks:
    #        if task.type is YarnContainerType.MRAM:
    #            continue
    #        score += task.duration * task.num_containers
    #    return score
 
    def reset_current_starve_queue_alloc_count(self):
        for i in range(self.num_main_queue):
            self.starve_queue_alloc_count[i] = 0
 
    def get_next_starve_free_queue(self):
        for i in range(self.num_main_queue):
            if self.starve_queue_alloc_count[i] < self.starve_queue_alloc_count_limit[i]:
                self.starve_queue_alloc_count[i] += 1
                k_to_return = i
                break
        if i == self.num_main_queue - 1:
            self.reset_current_starve_queue_alloc_count()
        return k_to_return

    def drop_job_on_deadline_miss(self, job):
        temp_task_set = set()   #   this is being created to avoid the change of the job.running_tasks set while iterating, as the function evict_container_from_node will modify the set job.running_tasks.
        for task in job.running_tasks:
            temp_task_set.add(task)
        for task in temp_task_set:
            self.evict_container_from_node(task, task.node)
 
        temp_job_current_queue = job.get_job_current_queue()

        temp_job_current_subqueue = job.get_job_current_subqueue()
        if temp_job_current_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
            if job in self.job_main_queue[temp_job_current_subqueue]:
                self.job_main_queue[temp_job_current_subqueue].remove(job)
        elif temp_job_current_queue == PATIENCE_QUEUE_NAMES.THIN.value:
            if job in self.job_thin_queue[temp_job_current_subqueue]:
                self.job_thin_queue[temp_job_current_subqueue].remove(job)
        elif temp_job_current_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value:
            if job in self.job_sample_queue[temp_job_current_subqueue]:
                self.job_sample_queue[temp_job_current_subqueue].remove(job)
        else:
            raise Exception("Invalid queue")

        self.dropped_jobs.add(job)
 
    def job_will_meet_deadline(self, job, max_possible_estimation_error):   #   max_possible_estimation_error should be a fraction between 0 and 1.
        #   This function assumes that the runtime has been estimated.
        #   Should only return true if job can meet the deadline and false if it cannot.
        #return True
        if max_possible_estimation_error < 0 or max_possible_estimation_error > 1:
            raise Exception("Parameter: max_possible_estimation_error out of bound")
 
        temp_deadline = job.get_deadline()
        temp_estimated_running_time = job.estimates.get_estimated_running_time(self.oracle, self.state.scheduling_goal)
        temp_remaining_time = temp_deadline - self.state.simulator.getClockMillis()
        if temp_estimated_running_time > temp_remaining_time:   #   Currently, not considering any estimation_error
            return False
        return True

    def get_current_load_for_queue(self, queue, sub_queue, job_id = None):
        num_effective_existing_tasks = 0
        effective_remaining_runtime = 0
        num_total_existing_tasks = 0
        total_remaining_runtime = 0
        temp_arriving_job_q = queue
        temp_arriving_job_subq = sub_queue
 
        for job in self.running_jobs:
            if job.job_id == job_id:
                continue
            temp_job_q = job.get_job_current_queue()
            temp_job_subq = job.get_job_current_subqueue()
 
            for task in job.pending_tasks:
                if task.type is YarnContainerType.MRAM:
                    continue
                total_remaining_runtime += task.getRemainingDuration()
 
            for task in job.running_tasks:
                if task.type is YarnContainerType.MRAM:
                    continue
                total_remaining_runtime += task.remaining_duration_ms_from_now(self.state.simulator.getClockMillis())
                effective_remaining_runtime += task.remaining_duration_ms_from_now(self.state.simulator.getClockMillis()) #   No multiplication with SAMPLING_FACTOR here as for running tasks there entire time will be added anyways.
            
            num_total_existing_tasks += ((len(job.pending_tasks) + len(job.running_tasks) - 1)) #   -1 is for the MRAM task
            num_effective_existing_tasks += (len(job.running_tasks) - 1) #   -1 is for the MRAM task
            
            if (temp_arriving_job_q == PATIENCE_QUEUE_NAMES.SAMPLE.value and temp_job_q == PATIENCE_QUEUE_NAMES.MAIN.value and temp_job_subq > 0) or (temp_arriving_job_q == PATIENCE_QUEUE_NAMES.MAIN.value and temp_arriving_job_subq == 0 and temp_job_q == PATIENCE_QUEUE_NAMES.SAMPLE.value) or (temp_arriving_job_q == PATIENCE_QUEUE_NAMES.MAIN.value and temp_job_q == PATIENCE_QUEUE_NAMES.MAIN.value and temp_job_subq > temp_arriving_job_subq):
                continue
 
            if self.oracle == PATIENCE_ORACLE_STATE.NO.value and temp_job_q == PATIENCE_QUEUE_NAMES.SAMPLE.value:
                temp_remaining_sampled_tasks = max(0, (job.get_num_sampling_tasks() - (len(job.running_tasks) - 1)))
                num_effective_existing_tasks += temp_remaining_sampled_tasks
                effective_remaining_runtime += ((job.get_average_initial_task_duration_for_all_tasks())*temp_remaining_sampled_tasks)
            else:
                num_effective_existing_tasks += (len(job.pending_tasks))
                for task in job.pending_tasks:
                    effective_remaining_runtime += task.getRemainingDuration()
 
        return num_effective_existing_tasks, effective_remaining_runtime, num_total_existing_tasks, total_remaining_runtime
 
    def get_current_load_for_arriving_job(self, arriving_job):
        return self.get_current_load_for_queue(arriving_job.get_job_current_queue(), arriving_job.get_job_current_subqueue(), job_id = arriving_job.job_id)
    
    def handle_job_completed(self, job):
        YarnScheduler.handle_job_completed(self, job)
        self.update_adaptive_sampling_stats_on_completion(job)
 
    def update_adaptive_sampling_stats_on_completion(self, job):
        temp_sp = job.get_sp()
        if temp_sp == None:
            return

        if temp_sp not in self.sp_nrmlzd_avg_completion_time_map_job_count:
            self.sp_nrmlzd_avg_completion_time_map_job_count[temp_sp] = 0
        temp_num_jobs_so_far = self.sp_nrmlzd_avg_completion_time_map_job_count[temp_sp]
        self.sp_nrmlzd_avg_completion_time_map_job_count[temp_sp] += 1
 
        if temp_sp not in self.sp_jid_nrmlzd_avg_completion_time_map:
            self.sp_jid_nrmlzd_avg_completion_time_map[temp_sp] = {}
        temp_time_key = job.start_ms#job.get_sampling_start_ms() #
        self.sp_jid_nrmlzd_avg_completion_time_map[temp_sp][temp_time_key] = (job.duration_ms/float(job.get_all_task_initial_duration()))
        if len(self.sp_jid_nrmlzd_avg_completion_time_map[temp_sp]) > self.adapt_sampling_max_past_samples:
            temp_jids = self.sp_jid_nrmlzd_avg_completion_time_map[temp_sp].keys()
            temp_jids.sort()
            temp_jids = temp_jids[0:len(temp_jids) - self.adapt_sampling_max_past_samples + 1]
            for jid_itr in temp_jids:
                del self.sp_jid_nrmlzd_avg_completion_time_map[temp_sp][jid_itr]
 
        temp_nrmlzd_avg_completion_time = np.mean(self.sp_jid_nrmlzd_avg_completion_time_map[temp_sp].values())
        self.sp_nrmlzd_avg_completion_time_map[temp_sp] = temp_nrmlzd_avg_completion_time
        #temp_nrmlzd_avg_completion_time = self.sp_nrmlzd_avg_completion_time_map[temp_sp]
        #self.sp_nrmlzd_avg_completion_time_map[temp_sp] = (temp_nrmlzd_avg_completion_time*temp_num_jobs_so_far + (job.duration_ms/float(job.get_all_task_initial_duration())))/float(temp_num_jobs_so_far + 1)

        #if temp_sp not in self.sp_nrmlzd_avg_accuracy_map_job_count:
        #    self.sp_nrmlzd_avg_accuracy_map_job_count[temp_sp] = 0
        #temp_num_jobs_so_far = self.sp_nrmlzd_avg_accuracy_map_job_count[temp_sp]
        #self.sp_nrmlzd_avg_accuracy_map_job_count[temp_sp] += 1
 
        #if temp_sp not in self.sp_nrmlzd_avg_accuracy_map:
        #    self.sp_nrmlzd_avg_accuracy_map[temp_sp] = 
        #temp_nrmlzd_avg_accuracy = self.sp_nrmlzd_avg_accuracy_map[temp_sp]
        #self.sp_nrmlzd_avg_accuracy_map[temp_sp] = (temp_nrmlzd_avg_accuracy*temp_num_jobs_so_far + )/float(temp_num_jobs_so_far + 1)

    def handle_job_arrived(self, job):
        #if int(job.job_id) == 3085:
        #    sys.stderr.write("job_3085_track arrived_at_time: "+str(self.state.simulator.getClockMillis())+"\n")
        TRIGGER_PERIOD = 6000
        YarnScheduler.handle_job_arrived(self, job)
        if self.state.scheduling_goal is SCHEDULING_GOALS.DEADLINE.value:
            if self.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value or self.oracle == PATIENCE_ORACLE_STATE.POINT_MEDIAN.value or self.oracle == PATIENCE_ORACLE_STATE.FULL.value:
                if not self.job_will_meet_deadline(job, MAX_POSSIBLE_ESTIMATION_ERROR_FOR_DEADLINE_CHECK):
                    self.action_on_non_admitted_job(job)
                    return
        if self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.HISTORY.value:
            if self.firstJob:
                self.firstJob = False
                first_node_type_update_event = UpdateNodeTypesEvent(self.state, self.state.simulator.getClockMillis() + TRIGGER_PERIOD, TRIGGER_PERIOD)
                self.state.simulator.add_event(first_node_type_update_event)
 
        job.pending_resource_requirements = YarnResource(0,0)
        if self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value or \
           self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.HISTORY.value or \
           self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
            if self.job_is_thin(job):
            #if job.get_width(self.width_type) < self.get_thin_limit():
                self.unscheduled_thin_job_tasks += len(job.tasks) - 1    #   -1 is for AM
            else:
                self.unscheduled_main_job_tasks += len(job.tasks) - 1    #   -1 is for AM; number of sampling tasks assigned are being subtracted from unscheduled_main_job_tasks when the sampling tasks are being assigned.
        if self.all_or_none != PATIENCE_ALL_OR_NONE_OPTIONS.NO.value:
            for task in job.tasks:
                job.pending_resource_requirements += task.resource
        if self.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value or self.oracle == PATIENCE_ORACLE_STATE.POINT_MEDIAN.value or self.oracle == PATIENCE_ORACLE_STATE.FULL.value:
            if self.state.dag_scheduling:
                temp_job_child_list = self.get_list_of_all_child_jobs(job)
            if self.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value or self.oracle == PATIENCE_ORACLE_STATE.POINT_MEDIAN.value:
                job_score = self.get_three_sigma_score(job, self.state.three_sigma_predictor, self.oracle)
                if self.state.dag_scheduling:
                    for temp_child_job in temp_job_child_list:
                        job_score += self.get_three_sigma_score(temp_child_job, self.state.three_sigma_predictor, self.oracle)
            elif self.oracle == PATIENCE_ORACLE_STATE.FULL.value:
                job_score = self.get_full_oracle_score(job)
                if self.state.dag_scheduling:
                    for temp_child_job in temp_job_child_list:
                        job_score += self.get_full_oracle_score(temp_child_job)
            else:
                raise Exception("In handle_job_arrived, self.oracle: "+str(self.oracle)+". Should not have come here.")
 
            if self.state.conservative_drop:
                self.action_for_conservative_deadline_drop(job)
                #conservative_deadline_finish_time = job.get_deadline() - job.estimates.get_estimated_running_time(self.oracle,SCHEDULING_GOALS.AVERAGE_JCT.value)# self.state.scheduling_goal)    #   Here we are assuming that job's deadline was set in YarnJobArriveEvent.handle; Conservative deadline drop event will be a new YarnDeadlineFinishEvent which will drop jobs a conservative deadline if there is atleast one never scheduled task.
                #conservative_deadline_finish_event = YarnDeadlineFinishEvent(self.state, job, conservative_deadline_finish_time, conservative_drop=True)
                #job.conservative_deadline_finish_event = conservative_deadline_finish_event
                #self.state.simulator.add_event(conservative_deadline_finish_event)

            #if False and int(job.job_id) == 3085:
            #    sys.stderr.write("job_3085_track score: "+str(job_score)+"\n")
            job.set_one_time_impact_score(job_score)
            temp = -1
            if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                temp = -1
            elif self.state.scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
                if job_score != 0 and self.first_main_queue_threshold != float("inf"):
                    try:
                        temp = math.log((float(job_score)/self.first_main_queue_threshold), self.main_queue_threshold_multiplier)
                    except:
                        raise Exception("In patience.handle_job_arrived for THREE_SIGMA oracle job_score: "+str(job_score)+" first_main_queue_threshold: "+str(self.first_main_queue_threshold)+"\n")
            if temp < 0:
                temp = -1
            entry_subqueue = int(temp) + 1
            entry_subqueue = min(entry_subqueue, DEFAULT_NUM_MAIN_QUEUES - 1)
            if self.state.all_thin_bypass and self.job_is_thin(job):
                #if self.state.common_queue == PATIENCE_COMMON_QUEUE.OFF.value:
                #    raise Exception("Not implemented for all_thin_bypass and COMMON_QUEUE_OFF")
                #elif self.state.common_queue == PATIENCE_COMMON_QUEUE.THIN_MAIN.value:
                    entry_subqueue = 0
                #else:
                #    raise Exception("Not implemented for all_thin_bypass and no queue instruction")

            if self.job_is_thin(job) and self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value and False:
            #if job.get_width(self.width_type) < self.get_thin_limit() and self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value and False:
                job.set_job_current_queue(PATIENCE_QUEUE_NAMES.THIN.value)
                job.set_job_current_subqueue(FIRST_THIN_SUBQUEUE)
                self.job_thin_queue[FIRST_THIN_SUBQUEUE].append(job)
                self.job_thin_queue[FIRST_THIN_SUBQUEUE].sort(key=lambda x: YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal, thin_limit = self.get_thin_limit()))
            else:
                job.set_job_current_queue(PATIENCE_QUEUE_NAMES.MAIN.value)
                job.set_job_current_subqueue(entry_subqueue)
                self.job_main_queue[entry_subqueue].append(job)
                if self.state.dag_scheduling:
                    for temp_child_job in temp_job_child_list:
                        self.do_heartbeat_creating_on_job_arrival(temp_child_job)
                        temp_child_job.set_job_current_queue(PATIENCE_QUEUE_NAMES.MAIN.value)
                        temp_child_job.set_job_current_subqueue(entry_subqueue)
                        self.job_main_queue[entry_subqueue].append(temp_child_job)

                print "Job_Starting_Queue_Details, job_id: ", job.job_id, ", self.oracle: ", self.oracle, ", entry_subqueue: ", entry_subqueue
                if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                    self.job_main_queue[entry_subqueue].sort(key=lambda x: YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal, thin_limit = self.get_thin_limit()))
        elif self.oracle == PATIENCE_ORACLE_STATE.THIN_ONLY_THREE_SIGMA.value:
            if self.job_is_thin(job):
            #if job.get_width(self.width_type) < self.get_thin_limit():
                job_score = self.get_three_sigma_score(job, self.state.three_sigma_predictor, self.state.oracle)
                temp = -1
                if job_score != 0 and self.first_thin_queue_threshold != float("inf"):
                    try:
                        temp = math.log((float(job_score)/self.first_thin_queue_threshold), self.thin_queue_threshold_multiplier)
                    except:
                        raise Exception("In patience.handle_job_arrived for THIN_ONLY_THREE_SIGMA oracle job_score: "+str(job_score)+" first_main_queue_threshold: "+str(self.first_thin_queue_threshold)+"\n")
                if temp < 0:
                    temp = -1
                entry_subqueue = int(temp) + 1
 
                if self.state.common_queue == PATIENCE_COMMON_QUEUE.OFF.value:
                    job.set_job_current_queue(PATIENCE_QUEUE_NAMES.THIN.value)
                    job.set_job_current_subqueue(entry_subqueue)
                    self.job_thin_queue[entry_subqueue].append(job)
                elif self.state.common_queue == PATIENCE_COMMON_QUEUE.THIN_MAIN.value:
                    job.set_job_current_queue(PATIENCE_QUEUE_NAMES.MAIN.value)
                    job.set_job_current_subqueue(entry_subqueue)
                    self.job_main_queue[entry_subqueue].append(job)
                else:
                    raise Exception("Unidentified common_queue: "+str(self.state.common_queue))
                # in the thin_queue_handler made tillQueueJumpOnly as False as no queue jump required here
            else:
                job.set_job_current_queue(PATIENCE_QUEUE_NAMES.SAMPLE.value)
                job.set_job_current_subqueue(FIRST_SAMPLING_SUBQUEUE)
                self.job_sample_queue[FIRST_SAMPLING_SUBQUEUE].append(job)
                self.assign_sampling_tasks(job)
        elif self.oracle == PATIENCE_ORACLE_STATE.LAS.value:
            job.set_job_current_queue(PATIENCE_QUEUE_NAMES.MAIN.value)
            job.set_job_current_subqueue(FIRST_MAIN_SUBQUEUE)
            self.job_main_queue[FIRST_MAIN_SUBQUEUE].append(job)
            if self.state.dag_scheduling:
                temp_job_child_list = self.get_list_of_all_child_jobs(job)
                for temp_child_job in temp_job_child_list:
                    temp_child_job.set_job_current_queue(PATIENCE_QUEUE_NAMES.MAIN.value)
                    temp_child_job.set_job_current_subqueue(FIRST_MAIN_SUBQUEUE)
                    self.job_main_queue[FIRST_MAIN_SUBQUEUE].append(temp_child_job)
                    self.do_heartbeat_creating_on_job_arrival(temp_child_job)
        else:
            #if True:
            if self.job_is_thin(job) and (not self.state.dag_scheduling):
            #if job.get_width(self.width_type) < self.get_thin_limit():
                if self.state.user_config.common_queue == PATIENCE_COMMON_QUEUE.OFF.value:
                    job.set_job_current_queue(PATIENCE_QUEUE_NAMES.THIN.value)
                    job.set_job_current_subqueue(FIRST_THIN_SUBQUEUE)
                    self.job_thin_queue[FIRST_THIN_SUBQUEUE].append(job)
                    if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                        self.job_thin_queue[FIRST_THIN_SUBQUEUE].sort(key=lambda x: YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal, thin_limit = self.get_thin_limit()))
                elif self.state.user_config.common_queue == PATIENCE_COMMON_QUEUE.THIN_MAIN.value:
                    job.set_job_current_queue(PATIENCE_QUEUE_NAMES.MAIN.value)
                    job.set_job_current_subqueue(FIRST_MAIN_SUBQUEUE)
                    self.job_main_queue[FIRST_MAIN_SUBQUEUE].append(job)
                    if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                        self.job_main_queue[FIRST_MAIN_SUBQUEUE].sort(key=lambda x: YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal, thin_limit = self.get_thin_limit()))
                else:
                    raise Exception("Unidentified common_queue: "+str(self.state.common_queue))
            else:
                if self.oracle == PATIENCE_ORACLE_STATE.NO.value:
                    job.set_job_current_queue(PATIENCE_QUEUE_NAMES.SAMPLE.value)
                    job.set_job_current_subqueue(FIRST_SAMPLING_SUBQUEUE)
                    self.job_sample_queue[FIRST_SAMPLING_SUBQUEUE].append(job)
                    self.assign_sampling_tasks(job)
                    temp_job_child_list = self.get_list_of_all_child_jobs(job)
                    for temp_child_job in temp_job_child_list:
                        temp_child_job.set_job_current_queue(PATIENCE_QUEUE_NAMES.SAMPLE.value)
                        temp_child_job.set_job_current_subqueue(FIRST_SAMPLING_SUBQUEUE)

                elif self.oracle == PATIENCE_ORACLE_STATE.FULL.value:
                    raise Exception("In handle_job_arrived, self.oracle: "+str(self.oracle)+". Should not have come here. The FULL oracle should have been handled above it self.")
                    job.set_job_current_queue(PATIENCE_QUEUE_NAMES.MAIN.value)
                    job.set_job_current_subqueue(FIRST_MAIN_SUBQUEUE)
                    self.job_main_queue[FIRST_MAIN_SUBQUEUE].append(job)
                    raise Exception("Correct the job placement in the main queue subqueue. In handle_job_arrived.")
                elif self.oracle == PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value:
                    #raise Exception("Thin jobs are not being properly handled in handle_virtual_sampling_completed. In handle_job_arrived.")
                    self.assign_sampling_tasks(job)
                    self.handle_virtual_sampling_completed(job)
                else:
                    raise Exception("Unknown Oracle state: oracle = "+str(self.oracle))
        num_effective_existing_tasks, effective_remaining_runtime, num_total_existing_tasks, total_remaining_runtime = self.get_current_load_for_arriving_job(job)
        print "Existing_load: ", job.job_id, num_effective_existing_tasks, effective_remaining_runtime, num_total_existing_tasks, total_remaining_runtime, job.get_job_current_queue(), job.get_job_current_subqueue(), (self.state.simulator.getClockMillis() - job.start_ms)
 
    def get_full_oracle_score(self, job): 
        if self.state.oracle is not PATIENCE_ORACLE_STATE.FULL.value:
            raise Exception("A function meant for full oracle called with oracle_state: ", str(self.state.oracle))
 
        #if job.get_width(self.width_type) < self.get_thin_limit():
        #    Work on returning max task length for the thin tasks
        #    for task in job.pending_tasks:
        #        if task.type is YarnContainerType.MRAM:
        #            continue
        #        score += task.duration * task.num_containers
        #    return score
 
        job_score = 0
        if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
            job_score = self.compute_job_deadline_score(job, self.state.oracle)
        elif self.state.scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
            for task in job.pending_tasks:
                if task.type is YarnContainerType.MRAM:
                    continue
                job_score += task.duration * task.num_containers
        return job_score
 
    def get_three_sigma_score(self, job, predictor, oracle_state, set_calculated_values_in_job = True): #   The set_calculated_values_in_job variable is being set False for dag scheducling in sampling. It is not being set from other calls. It should be True for others, hence setting default value to true.
        runtime = job.get_three_sigma_predicted_runtime()
        if runtime == None:
            if EARLY_FEEDBACK and job.job_id%EARLY_FEEDBACK_FREQ != 0:
                temp_early_feedback = False
            else:
                temp_early_feedback = True
            predicted_runtime = predictor.predict(job, self.prediction_utility_function, early_feedback = temp_early_feedback)    #   THREESIGMA_UTILITY_FUNCTIONS.AVERAGE_JCT.value) Or in case of Point_median three sigma_point median function will be passed.
            if self.state.secondary_three_sigma_predictor is not None:
                secondary_predicted_runtime = self.state.secondary_three_sigma_predictor.predict(job, self.prediction_utility_function, early_feedback = temp_early_feedback)    #   THREESIGMA_UTILITY_FUNCTIONS.AVERAGE_JCT.value) Or in case of Point_median three sigma_point median function will be passed.
                if set_calculated_values_in_job or True: 
                    job.set_three_sigma_secondary_predicted_runtime(secondary_predicted_runtime)
            if set_calculated_values_in_job or True:
                job.set_three_sigma_predicted_runtime(predicted_runtime)
        else:
           raise Exception("We are going to set None score as 3Sigma score for the get_three_sigma_score")

        if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
            job_score = self.compute_job_deadline_score(job, oracle_state)
        elif self.state.scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
            predicted_runtime = job.get_three_sigma_predicted_runtime()
            if predicted_runtime == None:
                print "Problem in predicted_runtime: "+str(job) + " predicted_runtime: "+ str(predicted_runtime) + " get_job_initial_number_of_tasks: "+ str(job.get_job_initial_number_of_tasks())
            job_score = job.get_job_initial_number_of_tasks()*predicted_runtime

        if set_calculated_values_in_job:
            job.set_one_time_impact_score(job_score)
            #score_to_return = job.get_current_estimated_impact_score(oracle_state)
            score_to_return = job.get_one_time_impact_score()
            #sys.stderr.write("job_track_score job_id: "+str(job.job_id)+ " score: " + str(score) + " predicted_runtime: "+str(predicted_runtime) + " num_tasks: "+str(job.get_job_initial_number_of_tasks()) + " set_score: "+str(job.get_job_initial_number_of_tasks()*predicted_runtime)+ "\n")
        else:
            score_to_return = job_score

        if score_to_return == INVALID_VALUE:
            raise Exception("INVALID_VALUE returned from ThreeSigmaPredictor.predict in YarnSRTFScheduler.get_three_sigma_score")
        return score_to_return

    def get_overall_cummulative_time_wise_resource_availability_dict(self):
        overall_resource_time_dict = {}
        for node in self.state.nodes:
            temp_node_wise_resource_time_dict = self.state.nodes[node].get_nodes_cummulative_time_wise_resource_availability_dict(self.all_or_none, self.state.simulator.getClockMillis())
            for time in temp_node_wise_resource_time_dict:
                if time not in overall_resource_time_dict:
                    overall_resource_time_dict[time] = YarnResource(0,0)
                overall_resource_time_dict[time] += temp_node_wise_resource_time_dict[time]
            
        time_list = overall_resource_time_dict.keys()
        time_list.sort()
        for i, time in enumerate(time_list):
            if i == 0:
                continue
            overall_resource_time_dict[time] += overall_resource_time_dict[time_list[i-1]]
        if 0 not in overall_resource_time_dict:
            overall_resource_time_dict[0] = YarnResource(0,0)
        return overall_resource_time_dict
         
    def by_what_least_time_needed_resource_will_be_available_in_cluster(self, resource_needed, policy): #   Returns the absolute simulator completion time
        clusterCapacity = YarnResource(0,0)
        clusterAvailable = YarnResource(0,0)
        for node in self.state.nodes:
            clusterCapacity += node.capacity

        time_to_return = self.state.simulator.getClockMillis()
        if policy == PATIENCE_ALL_OR_NONE_OPTIONS.NON_EVICTIVE.value:
            if clusterCapacity < resource_needed:
                print "The resource need queried exceeds the cluster capacity."
                return INVALID_VALUE 
            if clusterAvailable >= resource_needed:
                return time_to_return
            else:
                overall_resource_time_dict = get_overall_cummulative_time_wise_resource_availability_dict(policy, self.state.simulator.getClockMillis()) 
                time_list = overall_resource_time_dict.keys()
                time_list.sort()
                for time in time_list:
                    if overall_resource_time_dict[time] >= resource_needed:
                        return time
        else:
            raise Exception("by_what_least_time_needed_resource_will_be_available_in_cluster reached in a non implemented block for the policy: "+str(policy)) 

    def has_active_jobs(self):
        thin_queue_bool = bool(self.job_thin_queue[0])
        for i in xrange(1, self.num_thin_queue):
            thin_queue_bool = thin_queue_bool or bool(self.job_thin_queue[i])
        
        sample_queue_bool = bool(self.job_sample_queue[0])
        for i in xrange(1, self.num_sampling_queue):
            sample_queue_bool = sample_queue_bool or bool(self.job_sample_queue[i])
        
        main_queue_bool = bool(self.job_main_queue[0])
        for i in xrange(1, self.num_main_queue):
            main_queue_bool = main_queue_bool or bool(self.job_main_queue[i])
        
        if main_queue_bool or sample_queue_bool or thin_queue_bool:
            return True
        else:
            return False
 
    def set_thin_limit(self, thin_limit):
        self.thin_limit = thin_limit

    def get_scheduler_name(self):
        return "PATIENCE"

    def get_thin_limit(self):
        return self.thin_limit

    def get_runtime_for_queue_and_sub_queue(self, queue, sub_queue):
        #if queue != PATIENCE_QUEUE_NAMES.SAMPLE.value:
        #    raise Exception("get_runtime_for_queue_and_sub_queue should currently be called only for sample queue. It is called for queue: "+str(queue)+" subqueue: "+str(sub_queue))
        if queue == PATIENCE_QUEUE_NAMES.THIN.value:
            if sub_queue == 0 or sub_queue == self.num_thin_queue - 1:
                return self.job_thin_sub_queue_limits[sub_queue], self.get_thin_limit()
            else:
                try:
                    return self.job_thin_sub_queue_limits[sub_queue] - self.job_thin_sub_queue_limits[sub_queue - 1], self.get_thin_limit()
                except IndexError:
                    print sub_queue, len(self.job_thin_sub_queue_limits), len(self.job_thin_queue)
        elif queue == PATIENCE_QUEUE_NAMES.SAMPLE.value:
            if sub_queue == 0 or sub_queue == self.num_sampling_queue - 1:
                return self.job_sample_sub_queue_limits[sub_queue], self.get_thin_limit()
            else:
                return self.job_sample_sub_queue_limits[sub_queue] - self.job_sample_sub_queue_limits[sub_queue - 1], self.get_thin_limit()
        elif queue == PATIENCE_QUEUE_NAMES.MAIN.value:
            try:
                if sub_queue == 0 or sub_queue == self.num_main_queue - 1:
                    return self.job_main_sub_queue_limits[sub_queue], self.get_thin_limit()
                else:
                    return self.job_main_sub_queue_limits[sub_queue] - self.job_main_sub_queue_limits[sub_queue - 1], self.get_thin_limit()
            except TypeError:
                print len(self.job_main_sub_queue_limits)
    def evict_container_from_node(self, yarn_container, node):  #   the container here is of YarnRunningContainer type.
        #   This function is reverse of handle_container_allocation
        #   print "In evict_container_from_node"
        job = yarn_container.job
        task = yarn_container.task
        node.evict_container(yarn_container)
        yarn_container.takeEvictionAction(self.state.simulator.getClockMillis())
        if self.state.scheduler_type is YarnSchedulerType.PATIENCE:
            job.pending_resource_requirements += yarn_container.resource
        job.consumption -= yarn_container.resource
        job.running_tasks.remove(yarn_container)
        task.takeEvictionAction()
        if task not in job.pending_tasks:
            job.pending_tasks.append(task)
        else:
            sys.stderr.write("In evict_container_from_node: task didn't exist\n")
        task.num_containers += 1
        job_queue = job.get_job_current_queue()
        job_subqueue = job.get_job_current_subqueue()
        if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
            if self.state.eviction_policy == PATIENCE_EVICTION_POLICY.NO.value:
                ##  Here assumption is that this evict with deadline miss is being called when job has missed the deadline so the job doesn't need to be appended again.
                ##  However, here we remove all the events associated with the container or task from the simulation event queue.
                #temp_event = yarn_container.getNextQueueJumpEvent()
                #if temp_event is not None:
                    #print "Unsafe remove 1"
                    #self.state.simulator.queue.unsafe_remove(temp_event)
                    #pass
                #temp_event = yarn_container.getFinishEvent()
                #if temp_event is not None:
                    #print "Unsafe remove 2"
                    #self.state.simulator.queue.unsafe_remove(temp_event)
                    #pass
                pass
            else:
                raise Exception("In deadline mode we do not have provision to evict tasks inidividually. So far we evict entire job on deadline miss and the job is not appended directly.")
        else:
            if job_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
                if job not in self.job_main_queue:
                    self.append_job_to_main_queue_or_update_main_subqueue(job, "evict_container_from_node")
            elif job_queue == PATIENCE_QUEUE_NAMES.THIN.value:
                for idx, subqueue in enumerate(self.job_thin_queue):
                    if job in subqueue:
                        return
                self.job_thin_queue[job_subqueue].append(job)
            else:
                pass
        #del yarn_container  # this is reverse of the call to self.create_container_from_task() being called from handle_container_allocation
    
    def get_job_next_sample_queue_and_subqueue(self, job_queue, job_subqueue, task_queue, task_subqueue, task_has_finished = False):  #this function assumes that the a task has completed it's execution in task_subqueue and still has remaining duration
        to_return_queue = job_queue
        to_return_subqueue = job_subqueue
        if job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value:
            if not task_has_finished:
                to_return_subqueue = min((self.num_sampling_queue -1) , max(job_subqueue, task_subqueue + 1))
            else:
                to_return_subqueue = min((self.num_sampling_queue -1) , max(job_subqueue, task_subqueue))
        else:
            raise Exception("get_job_next_sample_queue_and_subqueue should not be called for a queue other than SAMPLE")
        return to_return_queue, to_return_subqueue     
    
    def get_job_next_main_queue_and_subqueue(self, job):
        job_queue = job.get_job_current_queue()
        to_return_queue = PATIENCE_QUEUE_NAMES.MAIN.value
        to_return_subqueue = 0
        if job_queue == PATIENCE_QUEUE_NAMES.MAIN.value or (job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value and job.get_sampling_end_ms() != INVALID_VALUE):
            job_score = self.get_main_queue_job_score(job, self.oracle, self.all_or_none)
            #try:
            if job_score != 0 and self.first_main_queue_threshold != float("inf"):
                try:
                    temp = math.log((float(job_score)/self.first_main_queue_threshold), self.main_queue_threshold_multiplier)
                except:
                    import sys
                    sys.stderr.write("In get_job_next_main_queue_and_subqueue. job_score: "+str(job_score)+" first_main_queue_threshold: "+str(self.first_main_queue_threshold)+"\n")
            else:
                temp = -1
            #except:
            #    print "math.log: ", job_score, self.first_main_queue_threshold, self.main_queue_threshold_multiplier
            if temp < 0:
                to_return_subqueue = 0
            else:
                to_return_subqueue = int(temp) + 1
        else:
            raise Exception("get_job_next_main_queue_and_subqueue should not be called for a job in SAMPLE queue for which sampling is not yet over." +str(job))
        return to_return_queue, min(to_return_subqueue, DEFAULT_NUM_MAIN_QUEUES - 1)

    def update_job_queue_and_subqueue(self, job, task_queue, task_subqueue, task_has_finished = False):  #this function assumes that the a task has completed it's execution in task_subqueue and still has remaining duration
        if self.oracle != PATIENCE_ORACLE_STATE.NO.value and self.oracle != PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value:
            raise Exception("update_job_queue_and_subqueue is supposed to be called for Oracle_state: "+str(PATIENCE_ORACLE_STATE.NO.value)+" but it is being called in oracle state: "+str(self.oracle))
        
        job_queue = job.get_job_current_queue()
        if job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value:
            job_subqueue = job.get_job_current_subqueue()
            new_job_queue, new_job_subqueue = self.get_job_next_sample_queue_and_subqueue(job_queue, job_subqueue, task_queue, task_subqueue, task_has_finished = task_has_finished)
            if new_job_queue != PATIENCE_QUEUE_NAMES.SAMPLE.value:
                raise Exception("Expected new_job_queue to be PATIENCE_QUEUE_NAMES.SAMPLE, however, it is: "+str(new_job_queue))
            if job_subqueue != new_job_subqueue:
                #print "Moving job: "+str(job.get_name())+" from queue: "+str(job_queue*100+job_subqueue)+" to queue: "+str(new_job_queue*100+new_job_subqueue)
                self.job_sample_queue[job_subqueue].remove(job)
                job.set_job_current_subqueue(new_job_subqueue)
                self.job_sample_queue[new_job_subqueue].append(job)
        elif job_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
            job_subqueue = job.get_job_current_subqueue()
            #new_job_queue, new_job_subqueue = self.get_job_next_main_queue_and_subqueue(job)
            if self.state.common_queue == PATIENCE_COMMON_QUEUE.OFF.value: 
                raise NotImplementedError()
            elif self.state.common_queue == PATIENCE_COMMON_QUEUE.THIN_MAIN.value:
                if job.get_width(self.width_type) >= self.get_thin_limit():
                    raise NotImplementedError()
                job_subqueue = job.get_job_current_subqueue()
                
                temp_job_subqueue_threshold = self.job_main_sub_queue_limits[job_subqueue]
                temp_job_width = job.get_width(self.width_type)
                temp_min_job_score = temp_job_subqueue_threshold*temp_job_width
                
                if temp_min_job_score != 0 and self.first_main_queue_threshold != float("inf"):
                    try:
                        temp_new_job_subqueue = math.log((float(temp_min_job_score)/self.first_main_queue_threshold), self.main_queue_threshold_multiplier)
                    except:
                        raise Exception("In patience.update_job_queue_and_subqueue for temp_min_job_score: "+str(temp_min_job_score)+" first_main_queue_threshold: "+str(self.first_main_queue_threshold)+" main_queue_threshold_multiplier: "+str(self.main_queue_threshold_multiplier)+"\n")
                if temp_new_job_subqueue < 0:
                    temp_new_job_subqueue = -1
                if temp_new_job_subqueue == float("inf"):
                    temp_new_job_subqueue = self.num_main_queue - 1
                try:
                    new_job_subqueue = min(self.num_main_queue - 1, int(temp_new_job_subqueue) + 1)
                except OverflowError:
                    raise Exception("In patience.update_job_queue_and_subqueue OverflowError for temp_min_job_score: "+str(temp_min_job_score)+" first_main_queue_threshold: "+str(self.first_main_queue_threshold)+" main_queue_threshold_multiplier: "+str(self.main_queue_threshold_multiplier)+" temp_new_job_subqueue: "+str(temp_new_job_subqueue)+" temp_job_subqueue_threshold: "+str(temp_job_subqueue_threshold)+ " temp_job_width: "+str(temp_job_width) +" job_subqueue: "+str(job_subqueue)+" job_id: "+str(job.job_id)+"\n")
                
                #new_job_subqueue = min(self.num_main_queue - 1, max(job_subqueue, task_subqueue+1))
                if new_job_subqueue != job_subqueue:
                    try:
                        self.job_main_queue[job_subqueue].remove(job)
                    except ValueError:
                        pass    #   This means that at some point in time job did not have any pending task and it was removed out of the queue. So no need to remove it.
                    job.set_job_current_subqueue(new_job_subqueue)
                    self.job_main_queue[new_job_subqueue].append(job)
                    print "Thin_job_update_case, jobid:,", job.job_id, ",new_job_subqueue:,", new_job_subqueue, ",job_subqueue:,", job_subqueue, ",job_width:,",job.get_width(self.width_type), ",job_rem_tasks:,",len(job.pending_tasks)
            else:
                raise Exception("Unidentified common_queue: "+str(self.state.common_queue))
        elif job_queue == PATIENCE_QUEUE_NAMES.THIN.value:
            if self.state.common_queue != PATIENCE_COMMON_QUEUE.OFF.value:
                raise NotImplementedError()
            job_subqueue = job.get_job_current_subqueue()
            new_job_subqueue = min(self.num_thin_queue - 1, max(job_subqueue, task_subqueue+1))
            if job_subqueue != new_job_subqueue:
                try:
                    self.job_thin_queue[job_subqueue].remove(job)
                except ValueError:
                    pass    #   This means that at some point in time job did not have any pending task and it was removed out of the queue. So no need to remove it.
                    #print len(self.job_thin_queue), job_subqueue, type(self.job_thin_queue), type(self.job_thin_queue[job_subqueue])
                job.set_job_current_subqueue(new_job_subqueue)
                self.job_thin_queue[new_job_subqueue].append(job)
            else:
                if job not in self.job_thin_queue[new_job_subqueue]:
                    self.job_thin_queue[new_job_subqueue].append(job)

    def compute_all_or_none_score_for_all_jobs_in_main_queue(self):
        #main_queue_job_wise_pending_resource_requirements_dict = {}
        #for job in self.job_main_queue:
        #    main_queue_job_wise_pending_resource_requirements_dict[job.job_id] = job.get_pending_job_requirements()

        #overall_resource_time_dict = self.get_overall_cummulative_time_wise_resource_availability_dict()
        LOG.warn("In all or none you have not yet handled the case of invalid score.")
        overall_resource_time_dict = self.state.overall_cummulative_time_wise_resource_availability_dict
        self.job_scores = {}
        for job in self.job_main_queue:
            self.job_scores[job.job_id] = self.compute_all_or_none_score_for_job(job, overall_resource_time_dict, self.main_queue_job_wise_pending_resource_requirements_dict)
            job.set_non_evictive_all_or_none_score(self.job_scores[job.job_id])

    def compute_all_or_none_score_for_job(self, job, overall_resource_time_dict, main_queue_job_wise_pending_resource_requirements_dict):
        estimated_running_time = job.estimates.get_estimated_running_time(self.oracle) # later update it to remaining time. 
        score_to_return = 0
        time_steps = overall_resource_time_dict.keys()
        time_steps.sort()
        for other_job in self.job_main_queue:
            if other_job.job_id == job.job_id:
                continue
            time_without_this_job = 0
            time_with_this_job = 0
            for time in time_steps:
                if overall_resource_time_dict[time] >= main_queue_job_wise_pending_resource_requirements_dict[other_job.job_id]:
                    time_without_this_job = time
                    break
            for time in time_steps:
                if overall_resource_time_dict[time] < main_queue_job_wise_pending_resource_requirements_dict[job.job_id]:
                    if overall_resource_time_dict[time] >= main_queue_job_wise_pending_resource_requirements_dict[other_job.job_id]:
                        time_with_this_job = time
                        break
                    else:
                        continue
                if overall_resource_time_dict[time] - main_queue_job_wise_pending_resource_requirements_dict[job.job_id] >= main_queue_job_wise_pending_resource_requirements_dict[other_job.job_id]:
                    time_with_this_job = time
                    break
            if time_without_this_job > time_with_this_job:
                print overall_resource_time_dict[time], main_queue_job_wise_pending_resource_requirements_dict[job.job_id], main_queue_job_wise_pending_resource_requirements_dict[other_job.job_id]
                raise Exception("Unexpected error in compute_all_or_none_score_for_job time_without_this_job: "+str(time_without_this_job)+" time_with_this_job: "+str(time_with_this_job))
            score_to_return += (time_with_this_job - time_without_this_job)

        return score_to_return
 
    def append_job_to_main_queue_or_update_main_subqueue(self, job, caller):
        if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
            raise Exception("append_job_to_main_queue_or_update_main_subqueue should not be called. Caller is: "+caller+" self.oracle: "+str(self.oracle) + "self.state.scheduling_goal: "+str(self.state.scheduling_goal))
        if self.oracle != PATIENCE_ORACLE_STATE.NO.value and self.oracle != PATIENCE_ORACLE_STATE.LAS.value and self.oracle != PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value:
            raise Exception("append_job_to_main_queue_or_update_main_subqueue should not be called. Caller is: "+caller+" self.oracle: "+str(self.oracle))
        if len(job.pending_tasks) == 0:
            raise Exception("No need to append the job in the main queue as it doesn't have any pending task currently. However, append_job_to_main_queue_or_update_main_subqueue still called for job: "+str(job.job_id)+" from: "+caller)
        if self.oracle == PATIENCE_ORACLE_STATE.NO.value and caller != "handle_sampling_completed":
            raise Exception("append_job_to_main_queue_or_update_main_subqueue with oracle = "+str(PATIENCE_ORACLE_STATE.NO.value)+" should only be called from handle_sampling_completed. But current caller is: "+caller)
 
        current_queue = job.get_job_current_queue()
        current_subqueue = job.get_job_current_subqueue()
        new_queue, new_subqueue = self.get_job_next_main_queue_and_subqueue(job)
        if new_queue != PATIENCE_QUEUE_NAMES.MAIN.value:
            raise Exception("In append_job_to_main_queue_or_update_main_subqueue. Call to self.get_job_next_main_queue_and_subqueue must return new_queue as PATIENCE_QUEUE_NAMES.MAIN.value. However it returned: "+ str(new_queue))
 
        if current_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
            if new_subqueue > self.num_main_queue - 1:
                raise Exception("In append_job_to_main_queue_or_update_main_subqueue unexpected new_subqueue for Main queue. new_subqueue: "+ str(new_subqueue))
            if job in self.job_main_queue[current_subqueue]:
                if current_subqueue == new_subqueue:
                    return  # No updates so no change
                else:
                    self.job_main_queue[current_subqueue].remove(job)
        elif current_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value:
            if current_subqueue > self.num_sampling_queue - 1:
                raise Exception("In append_job_to_main_queue_or_update_main_subqueue unexpected new_subqueue for Main queue. new_subqueue: "+ str(new_subqueue))
            if job in self.job_sample_queue[current_subqueue]:
                self.job_sample_queue[current_subqueue].remove(job)
        else:
            raise Exception("In append_job_to_main_queue_or_update_main_subqueue. Currently job in some unknown queue: "+str(current_queue))
 
        job.set_job_current_queue(new_queue)
        job.set_job_current_subqueue(new_subqueue)
        self.job_main_queue[new_subqueue].append(job)
                    #   DELETE the sort below after experiments #
        #self.job_main_queue[new_subqueue].sort(key=lambda x: YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal, thin_limit = self.get_thin_limit()))
                    #   DELETE the sort above after experiments #
        if not self.state.dag_scheduling:
            self.main_queue_job_wise_pending_resource_requirements_dict[job.job_id] = job.get_pending_job_requirements()

    def get_main_queue_job_score(self, job, oracle_state, all_or_none):
        if oracle_state == PATIENCE_ORACLE_STATE.LAS.value:
            if job.isSuperParent():
                score = job.get_las_score()
                if self.state.dag_scheduling:
                    temp_job_child_list = self.get_list_of_all_child_jobs(job)
                    for temp_child_job in temp_job_child_list:
                        score+= temp_child_job.get_las_score()
                if self.state.all_thin_bypass and not self.state.dag_scheduling:
                    if self.job_is_thin(job):
                        score = 0
            else:
                return self.get_main_queue_job_score(self.get_job_parent(job), oracle_state, all_or_none)
        else:
            if all_or_none == PATIENCE_ALL_OR_NONE_OPTIONS.NON_EVICTIVE.value:
                score = job.get_non_evictive_all_or_none_score()
                raise Exception("I think this should not be called. Check the function job.get_non_evictive_all_or_none_score.")
            else:
                #   IF UPDATING ANYTHING HERE UPDATE THE STATIC METHOD COMPUTE_JOB_SCORE AS WELL
                score = job.get_one_time_impact_score()
                #score = job.get_current_impact_score()
                #score = job.get_current_estimated_impact_score(oracle_state)
                #score = int(np.log10(max(1,score)))
        return score

    def compute_job_deadline_score(self, job, oracle_state):
        current_time = self.state.simulator.getClockMillis()
        deadline = job.get_deadline()
        job_length = job.estimates.get_estimated_running_time(self.oracle, self.state.scheduling_goal)
        score = max(0,(deadline - (current_time + job_length)))
        return score

    def job_is_thin(self, job):
        if job.get_width(self.width_type) < self.get_thin_limit():
            return True
        else:
            return False


    @staticmethod
    def compute_job_score(job, oracle_state, width_type, all_or_none, scheduling_goal, thin_limit = 30):
        score = 0
        if scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
            job_width = job.get_width(width_type)
            if  oracle_state == PATIENCE_ORACLE_STATE.NO.value and job_width < thin_limit:
            #if  job_width < thin_limit:
                return job_width
            else:
                return job.get_one_time_impact_score()  #   Here we are assuming that score required for deadline is set before calling the compute_job_score.
        elif scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
            if job.get_job_current_queue() == PATIENCE_QUEUE_NAMES.THIN.value:
                raise Exception ("compute_job_score should not be called for a thin job.")
                score = job.get_width(width_type)
            elif job.get_job_current_queue() == PATIENCE_QUEUE_NAMES.SAMPLE.value:
                raise Exception ("compute_job_score should not be called for a job in sampling phase.")
                if job.get_num_sampling_tasks_to_be_assigned() <= 0:
                    if job.get_width(width_type) > thinLimit:
                        #score = len(job.pending_tasks)
                        score = 10000+len(job.pending_tasks)
                #else:
                #    score = job.get_num_sampling_tasks_to_be_assigned()
            elif job.get_job_current_queue() == PATIENCE_QUEUE_NAMES.MAIN.value:
                if self.state.common_queue == PATIENCE_COMMON_QUEUE.THIN_MAIN.value:
                #if 1 == PATIENCE_COMMON_QUEUE.THIN_MAIN.value:
                    if self.job_is_thin(job):
                    #if job.get_width(self.width_type) < self.get_thin_limit():
                    #if job.get_width("initial_num_tasks") < 3:#self.get_thin_limit():
                        raise Exception ("compute_job_score should not be called for a thin job.")
                #   IF UPDATING ANYTHING HERE UPDATE THE METHOD GET_MAIN_QUEUE_JOB_SCORE AS WELL
                #score = get_main_queue_job_score(job, oracle_state, all_or_none)
                if all_or_none == PATIENCE_ALL_OR_NONE_OPTIONS.NON_EVICTIVE.value:
                    score = job.get_non_evictive_all_or_none_score()
                else:
                    #if job.get_width("initial_num_tasks") < 3:#self.get_thin_limit():
                    #    score = 0
                    #else:
                    score = job.get_one_time_impact_score()
                    #score = job.get_current_impact_score()
                    #score = job.get_current_estimated_impact_score(oracle_state)
                    #score = int(np.log10(max(1,score)))
        return score
 
    def get_num_sampling_tasks(self, job):
        if self.job_is_thin(job):
        #if job.get_width(self.width_type) < self.get_thin_limit():
            raise Exception("Now thin tasks are in different queues and should not have been called here.")
            return job.get_job_initial_number_of_tasks()
        else:
            temp_sampling_percentage = self.get_current_sampling_percentage(self.sp_nrmlzd_avg_completion_time_map, self.sp_nrmlzd_avg_completion_time_map_job_count, self.sp_nrmlzd_avg_accuracy_map, self.sp_nrmlzd_avg_accuracy_map_job_count)
            job.set_sp(temp_sampling_percentage)
            print "sp_for_job: ",job.job_id,",",temp_sampling_percentage
            return max(1,math.ceil((job.get_job_initial_number_of_tasks())*(float(temp_sampling_percentage)/100)))
            #return min(math.ceil(self.state.num_nodes*.01), max(1,math.ceil((job.get_job_initial_number_of_tasks())*(temp_sampling_percentage/100))))

    def get_current_sampling_percentage(self, ct_map, ct_map_count, acc_map, acc_map_count):
        if self.sampling_percentage == 0.0:
            raise Exception("Sampling percentage is set to zero\n")
            return self.constant_sampling
        if self.state.user_config.adaptive_sampling == PATIENCE_ADAPTIVE_SAMPLING_TYPES.YES.value:
            if self.num_jobs_assigned_adaptive_sampling_rate < self.adapt_sampling_max_past_samples:
                sampling_percentage_to_return = self.default_adapt_sampling_percentage
                print "ct_map, ",ct_map, ", sampling_percentage_to_return,", sampling_percentage_to_return, ", ct_map_count: ", self.sp_nrmlzd_avg_completion_time_map_job_count
            elif self.num_jobs_assigned_adaptive_sampling_rate >= self.adapt_sampling_max_past_samples and self.num_jobs_assigned_adaptive_sampling_rate < self.adapt_sampling_max_past_samples*2:
                sampling_percentage_to_return = self.default_adapt_sampling_percentage-1
                print "ct_map, ",ct_map, ", sampling_percentage_to_return,", sampling_percentage_to_return, ", ct_map_count: ", self.sp_nrmlzd_avg_completion_time_map_job_count
            elif self.num_jobs_assigned_adaptive_sampling_rate >= self.adapt_sampling_max_past_samples*2 and self.num_jobs_assigned_adaptive_sampling_rate < self.adapt_sampling_max_past_samples*3:
                sampling_percentage_to_return = self.default_adapt_sampling_percentage+1
                print "ct_map, ",ct_map, ", sampling_percentage_to_return,", sampling_percentage_to_return, ", ct_map_count: ", self.sp_nrmlzd_avg_completion_time_map_job_count
            else:
                sampling_percentage_to_return = None
                score_sp = {}
                for sp in ct_map:   #   calculate score in this loop
                    if ct_map_count[sp] < self.adapt_sampling_min_required_samples:
                        continue
                    score_sp[sp] = ct_map[sp]
                    #if sp in acc_map:
                        #score_sp[sp] = ct_map[sp]/acc_map[sp]
 
                temp_min_score = float("inf") 
                temp_min_sp = self.sampling_percentage
                for sp in score_sp: #   calculate rank here
                    if score_sp[sp] < temp_min_score:
                        temp_min_score = score_sp[sp]
                        temp_min_sp = sp
                should_increment = False
                should_decrement = False
                if temp_min_sp < self.adapt_sampling_pecentage_upper_limit: #   Work on this block to update
                    if (temp_min_sp - 1) in score_sp:
                        temp2_score = score_sp[temp_min_sp - 1]
                        temp_moving_up_score_diff = temp2_score - temp_min_score
                        if (temp_moving_up_score_diff) >= temp2_score*self.adapt_sampling_pecentage_upper_switch_delta and ((temp_min_sp + 1) not in score_sp):
                            should_increment = True
                if temp_min_sp > self.adapt_sampling_pecentage_lower_limit: #   Work on this block to update
                    if (temp_min_sp + 1) in score_sp:
                        temp3_score = score_sp[temp_min_sp + 1]
                        temp_moving_down_score_diff = temp3_score - temp_min_score
                        if (temp_moving_down_score_diff) >= temp3_score*self.adapt_sampling_pecentage_lower_switch_delta and ((temp_min_sp - 1) not in score_sp):
                            should_decrement = True
                if should_increment:
                    temp_min_sp += 1
                if should_decrement:
                    temp_min_sp -= 1
                if should_increment and should_decrement:
                    raise Execption("Impossible has happened.")

                sampling_percentage_to_return = temp_min_sp
                print "score_sp, ",score_sp, ", sampling_percentage_to_return,", sampling_percentage_to_return,", ct_map_count: ", ct_map_count
            self.num_jobs_assigned_adaptive_sampling_rate += 1
        else:
            sampling_percentage_to_return = self.sampling_percentage
        return sampling_percentage_to_return

    def assign_sampling_tasks(self, job):
        temp_num_sampling_tasks = self.get_num_sampling_tasks(job)
        job.set_num_sampling_tasks(temp_num_sampling_tasks)
        if self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value or \
           self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.HISTORY.value or \
           self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
            self.unscheduled_sampling_tasks += temp_num_sampling_tasks
            self.unscheduled_main_job_tasks -= temp_num_sampling_tasks  #   This is because all the main jobs tasks are added in the unscheduled_main_job_tasks in the handle_job_arrived. So removing the sampling tasks from it.

    def increment_unscheduled_tasks_for_job_by(self, job, by):
        job_queue = job.get_job_current_queue()
        if job_queue == PATIENCE_QUEUE_NAMES.THIN.value:
            self.unscheduled_thin_job_tasks += by
        elif job_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value:
            self.unscheduled_sampling_tasks += by
        elif job_queue == PATIENCE_QUEUE_NAMES.MAIN.value:
            if self.state.common_queue == PATIENCE_COMMON_QUEUE.OFF.value: 
                self.unscheduled_main_job_tasks += by
            elif self.state.common_queue == PATIENCE_COMMON_QUEUE.THIN_MAIN.value: 
                if self.job_is_thin(job):
                #if job.get_width(self.width_type) < self.get_thin_limit():
                    self.unscheduled_thin_job_tasks += by
                else:
                    self.unscheduled_main_job_tasks += by
            else:
                raise Exception("Unidentified common_queue: "+str(self.state.common_queue))

    def remove_job_from_sampling_queue(self, job):
        job_queue = job.get_job_current_queue()
        if job_queue != PATIENCE_QUEUE_NAMES.SAMPLE.value:
            raise Exception("The job coming in remove_job_from_sampling_queue should be in sampling queue only.")
        job_subqueue = job.get_job_current_subqueue()
        self.job_sample_queue[job_subqueue].remove(job)

    def if_needed_add_job_in_main_queue(self, job):
        job_queue = job.get_job_current_queue()
        if job_queue != PATIENCE_QUEUE_NAMES.MAIN.value:
            raise Exception("in if_needed_add_job_in_main_queue. Expected job queue to be MAIN queue. However, that is not the case, job_current_queue is : "+str(job_queue))
        if job not in self.job_main_queue:
            #print "Adding job to main queue from if_needed_add_job_in_main_queue"
            #raise Exception("The line below is not correct implemetation. In the main queue, you should be checking in all the sub queues to see if the job is not there add it. And if you can ensure that job never changes its subqueus then check only in the sub queue and see if to add there. As in patience and threesigma design no need to jump the queues. Queues are just to provide starvation free guarantee. The function below is changing the queues.")
            self.append_job_to_main_queue_or_update_main_subqueue(job, "if_needed_add_job_in_main_queue")

    def handle_virtual_sampling_completed(self, job):
        #raise Exception("handle_virtual_sampling_completed in patience.py. Sampling oracle in patience is deprecated. Use the one in SRTF.")
        # Update sampling finishing time
        job.set_sampling_end_ms(self.state.simulator.getClockMillis())
        # Move job to main queue
        job.set_job_current_queue(PATIENCE_QUEUE_NAMES.MAIN.value)
        job.set_job_current_subqueue(FIRST_MAIN_SUBQUEUE)
        job.mark_all_running_tasks_as_non_sampling()    #   This action is needed as we mark certain tasks to be sampling tasks, to ensure that they are not evicted. However, if they are not yet finished but number of sampling tasks are over than they should be allowed to be evicted.
        job.estimates.calculate_virtual_estimated_running_time(self.oracle, self.state.scheduling_goal)
        job.set_one_time_impact_score(job.get_job_initial_number_of_tasks()*job.estimates.get_virtual_estimated_running_time(self.oracle, self.state.scheduling_goal))
        if job.pending_tasks:
            self.append_job_to_main_queue_or_update_main_subqueue(job, "handle_virtual_sampling_completed")
        else:
            new_queue, new_subqueue = self.get_job_next_main_queue_and_subqueue(job)
            job.set_job_current_queue(new_queue)
            job.set_job_current_subqueue(new_subqueue)
        if job.get_width(self.width_type) >= self.get_thin_limit():
            pass

    def action_on_non_admitted_job(self, job):
        self.not_admitted_jobs.add(job)
        job.not_admitted = True
        temp_task_set = set()   #   this is being created to avoid the change of the job.running_tasks set while iterating, as the function evict_container_from_node will modify the set job.running_tasks.
        for task in job.running_tasks:
            temp_task_set.add(task)
        for task in temp_task_set:
            self.evict_container_from_node(task, task.node)
        print "Job: "+job.get_name() + " not admitted."
        if self.all_jobs_are_done():
            # Add a YarnSimulationFinishEvent
            self.state.simulator.add_event(self.state.generator.get_simulation_finish_event())
 
    def action_for_conservative_deadline_drop(self, job):
        temp_secondary_estimate = False
        if self.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value or self.oracle == PATIENCE_ORACLE_STATE.POINT_MEDIAN.value or self.oracle == PATIENCE_ORACLE_STATE.THIN_ONLY_THREE_SIGMA.value:
            temp_secondary_estimate = True
        conservative_deadline_finish_time = job.get_deadline() - job.estimates.get_estimated_running_time(self.oracle, SCHEDULING_GOALS.AVERAGE_JCT.value ,secondary_estimate = temp_secondary_estimate) # self.state.scheduling_goal)#     Here we are assuming that job's deadline was set in YarnJobArriveEvent.handle; Conservative deadline drop event will be a new YarnDeadlineFinishEvent which will drop jobs a conservative deadline if there is atleast one never scheduled task.
        conservative_deadline_finish_event = YarnDeadlineFinishEvent(self.state, job, conservative_deadline_finish_time, conservative_drop=True)
        job.conservative_deadline_finish_event = conservative_deadline_finish_event
        self.state.simulator.add_event(conservative_deadline_finish_event)

    def handle_sampling_completed(self, job):
        # Update sampling finishing time
        job.set_sampling_end_ms(self.state.simulator.getClockMillis())
 
        self.remove_job_from_sampling_queue(job)
 
        if self.state.scheduling_goal is SCHEDULING_GOALS.DEADLINE.value:
            if not self.job_will_meet_deadline(job, MAX_POSSIBLE_ESTIMATION_ERROR_FOR_DEADLINE_CHECK):
                self.action_on_non_admitted_job(job)
                return
            if self.state.conservative_drop:
                self.action_for_conservative_deadline_drop(job)
 
        # Move job to main queue
        job.set_job_current_queue(PATIENCE_QUEUE_NAMES.MAIN.value)
        #print "JOB_SAMPLING_FINISHED,"+job.get_name()+","+str(job.job_id)+","+str(job.get_num_sampling_tasks())+","+str(job.get_num_tasks_finished_in_sampling_queue())+","+str(job.get_job_initial_number_of_tasks())+","+str(len(job.pending_tasks))+","+str(job.get_sampling_start_ms())+","+str(job.get_sampling_end_ms())
        job.mark_all_running_tasks_as_non_sampling()    #   This action is needed as we mark certain tasks to be sampling tasks, to ensure that they are not evicted. However, if they are not yet finished but number of sampling tasks are over than they should be allowed to be evicted.
        if self.state.scheduling_goal is SCHEDULING_GOALS.DEADLINE.value:
            #   Calculating Deadline_Score
            job_score = self.compute_job_deadline_score(job, self.oracle)
            #Moved in the for loop below#job.set_one_time_impact_score(job_score)
            if job.pending_tasks:
               job.set_job_current_queue(PATIENCE_QUEUE_NAMES.MAIN.value)
               job.set_job_current_subqueue(FIRST_MAIN_SUBQUEUE)
               self.job_main_queue[FIRST_MAIN_SUBQUEUE].append(job)
               self.job_main_queue[FIRST_MAIN_SUBQUEUE].sort(key=lambda x: YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal, thin_limit = self.get_thin_limit()))
        elif self.state.scheduling_goal is SCHEDULING_GOALS.AVERAGE_JCT.value:
            job_score = job.get_job_initial_number_of_tasks()*job.estimates.get_estimated_running_time(self.oracle, self.state.scheduling_goal)
            #job_score = job.get_job_initial_number_of_tasks()*job.estimates.get_estimated_running_time(PATIENCE_ORACLE_STATE.FULL.value, self.state.scheduling_goal)
            job.set_one_time_impact_score(job_score)
            parent_job_sampling_score = job_score
            temp_all_jobs_to_queue = [job]
            if self.state.dag_scheduling:
                parent_job_three_sigma_score = self.get_three_sigma_score(job, self.state.three_sigma_predictor, PATIENCE_ORACLE_STATE.THREE_SIGMA.value, set_calculated_values_in_job = False)
                sampling_history_correction_factor = float(parent_job_sampling_score)/float(parent_job_three_sigma_score)
                temp_job_child_list = self.get_list_of_all_child_jobs(job)
 
                for temp_child_job in temp_job_child_list:
                    temp_child_job.set_sampling_end_ms(self.state.simulator.getClockMillis())
                    self.do_heartbeat_creating_on_job_arrival(temp_child_job)
                    temp_child_job_three_sigma_score = self.get_three_sigma_score(temp_child_job, self.state.three_sigma_predictor, PATIENCE_ORACLE_STATE.THREE_SIGMA.value, set_calculated_values_in_job = False)
                    temp_corrected_score = temp_child_job_three_sigma_score*sampling_history_correction_factor
                    temp_corrected_average_task_length = temp_corrected_score/temp_child_job.get_job_initial_number_of_tasks()
                    temp_child_job.estimates.set_sampled_estimated_time(temp_corrected_average_task_length)
                    job_score += (temp_corrected_score)
                
                temp_all_jobs_to_queue += temp_job_child_list

            for job_itr in temp_all_jobs_to_queue:
                job_itr.set_one_time_impact_score(job_score)
                if job_itr.pending_tasks:
                    #print "AFTER_SAMPLING_FINISH_MOVING_TO_MAIN_QUEUE,"+job.get_name()+","+str(job.job_id)+","+str(job.get_num_sampling_tasks())+","+str(job.get_num_tasks_finished_in_sampling_queue())+","+str(job.get_job_initial_number_of_tasks())+","+str(len(job.pending_tasks))+","+str(job.get_sampling_start_ms())+","+str(job.get_sampling_end_ms())
                    self.append_job_to_main_queue_or_update_main_subqueue(job_itr, "handle_sampling_completed")
                    num_effective_existing_tasks, effective_remaining_runtime, num_total_existing_tasks, total_remaining_runtime = self.get_current_load_for_arriving_job(job_itr)
                    print "Remaining_tasks_in_job_post_sampling: ", job_itr.job_id, len(job_itr.pending_tasks)
                    print "Existing_load_post_sampling: ", job_itr.job_id, num_effective_existing_tasks, effective_remaining_runtime, num_total_existing_tasks, total_remaining_runtime, job_itr.get_job_current_queue(), job_itr.get_job_current_subqueue(), (self.state.simulator.getClockMillis() - job_itr.start_ms)
                else:
                    new_queue, new_subqueue = self.get_job_next_main_queue_and_subqueue(job_itr)
                    job_itr.set_job_current_queue(new_queue)
                    job_itr.set_job_current_subqueue(new_subqueue)
        
        
        
        for job_itr in temp_all_jobs_to_queue:
            if job_itr.get_width(self.width_type) >= self.get_thin_limit():
                #pass
                if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                    original_duration = job_itr.get_max_initial_task_duration_for_all_tasks()
                elif self.state.scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
                    original_duration = job_itr.get_average_initial_task_duration_for_all_tasks()
                #job_average_task_length = job.get_average_initial_task_duration_for_all_tasks()
                print "Patience Average task length prediction stats for jid: ",str(job_itr.job_id),"," ,job_itr.estimates.get_estimated_running_time(self.state.oracle, self.state.scheduling_goal), ",", original_duration, ",",float(float(abs(job_itr.estimates.get_estimated_running_time(self.state.oracle, self.state.scheduling_goal) - original_duration))/float(original_duration))*100
                #print "Patience prediction stats for jid: ",str(job_itr.job_id),"," ,job_itr.estimates.get_estimated_running_time(self.state.oracle), ",", job_itr.trace_duration_ms, ",",float(float(abs(job_itr.estimates.get_estimated_running_time(self.state.oracle) - job_itr.trace_duration_ms))/float(job_itr.trace_duration_ms))*100

    def decrement_autotune_nodetype_count_stats_for_node(self, node, caller):
        #print "In decrement_autotune_nodetype_count_stats_for_node, caller: ", caller, " node_type: ", node.get_temp_node_type(), " current_count: ", self.auto_tunned_node_types[node.get_temp_node_type()]
        if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value or self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
            self.auto_tunned_node_types[node.get_temp_node_type()] -= 1
            #print "In decrement_autotune_nodetype_count_stats_for_node, ", self.auto_tunned_node_types[node.get_temp_node_type()]
    def handle_task_completion(self, yarn_container):
        #if yarn_container.job.job_id == 6391113248: job = yarn_container.job sys.stderr.write("handle_task_completion job_id: "+str(6391113248) + " width: "+str(len(job.tasks)) + " node.get_temp_node_type: " + str(yarn_container.node.get_temp_node_type())+ ", node_id: "+str(yarn_container.node.node_id)+ " task_type: "+ str(yarn_container.task.type) +"\n")
        #if len(yarn_container.job.tasks)< self.thin_limit:
        #    print "Calling handle_task_completion for thin job too. Node_type: ", yarn_container.node.get_temp_node_type()
        if yarn_container.task.type is not YarnContainerType.MRAM:
            self.decrement_autotune_nodetype_count_stats_for_node(yarn_container.node, "handle_task_completion")
        if yarn_container.job.get_job_current_queue() == PATIENCE_QUEUE_NAMES.SAMPLE.value:
            yarn_container.job.increase_num_tasks_finished_in_sampling_queue(1)
            yarn_container.job.estimates.add_estimation_info(yarn_container, self.state.scheduling_goal)
            if yarn_container.job.get_num_tasks_finished_in_sampling_queue() >= yarn_container.job.get_num_sampling_tasks():
                self.handle_sampling_completed(yarn_container.job)
        elif yarn_container.job.get_job_current_queue() == PATIENCE_QUEUE_NAMES.MAIN.value:
            if self.job_is_thin(yarn_container.job):
            #if yarn_container.job.get_width(self.width_type) < self.get_thin_limit():
                if self.state.common_queue == PATIENCE_COMMON_QUEUE.OFF.value:
                    if self.oracle == PATIENCE_ORACLE_STATE.DEFAULT.value:
                        raise NotImplementedError()
                elif self.state.common_queue == PATIENCE_COMMON_QUEUE.THIN_MAIN.value:
                    pass
                else:
                    raise Exception("Unidentified common_queue: "+str(self.state.common_queue))
            else:
                if yarn_container.job.pending_tasks:
                    if self.state.eviction_policy != PATIENCE_EVICTION_POLICY.NO.value:
                        self.append_job_to_main_queue_or_update_main_subqueue(yarn_container.job, "handle_task_completion")
        else:
            pass
            #raise Exception("In handle_task_completion job in un indentified queue: "+str(yarn_container.job.get_job_current_queue()))

    def thin_queue_handler(self, node, allJobsChecked, pass_count):
            if self.oracle == PATIENCE_ORACLE_STATE.LAS.value:
                return
            # first handle thin queue
            while True:
                subqueues_taversed = 0
                for subqueue in self.job_thin_queue:
                    print "thin_queue_handler.len(subqueue): ", len(subqueue)
                    subqueues_taversed += 1
                    #subqueue.sort(key=lambda x: YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal))
                    #self.job_thin_queue.sort(key=lambda x: YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal))
                    if LOG.isEnabledFor(logging.DEBUG):
                        LOG.debug("QUEUE: " + ", ".join(map(lambda x: "<" +
                                  x.get_name() + " " + str(x.am_launched) + " " +
                                  str(x.consumption) + " " + str(YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal)) +
                                  ">", self.job_queue)))
 
                    thin_queue_idx = 0
                    #while thin_queue_idx < len(self.job_thin_queue):
                    while thin_queue_idx < len(subqueue):
                        #job = self.job_thin_queue[thin_queue_idx]
                        job = subqueue[thin_queue_idx]
                        task = job.pending_tasks[0]
                        self.stats_decisions_inc(job.job_id)
                        if not job.am_launched and task.type is not YarnContainerType.MRAM:
                            self.stats_reject_decisions_inc(job.job_id)
                            thin_queue_idx += 1
                            continue
                        to_schedule = False
                        if task.resource <= node.available:
                            to_schedule = True
                        elif self.state.racks["default-rack"].get_specific_node_type_resource_availability(PATIENCE_NODE_TYPE.THIN.value) < task.resource and self.state.eviction_policy != PATIENCE_EVICTION_POLICY.NO.value: #   This is because if other nodes have resources available then eviction should not happen
                        #elif self.state.racks["default-rack"].available < task.resource and self.state.eviction_policy != PATIENCE_EVICTION_POLICY.NO.value: #   This is because if other nodes have resources available then eviction should not happen
                            #print "going to call can_evict_something_for_task"
                            can_evict, container_to_be_evicted = node.can_evict_something_for_task(task)
                            if can_evict:
                                #print "going to call evict_container_from_node"
                                self.evict_container_from_node(container_to_be_evicted, node)
                                to_schedule = True
                        if self.oracle == PATIENCE_ORACLE_STATE.THIN_ONLY_THREE_SIGMA.value:    #   No Queue jump required here.
                            tillQueueJumpOnly = False
                        else:
                            if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                                tillQueueJumpOnly = False
                            elif self.state.scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
                                tillQueueJumpOnly = False
                        if to_schedule:
                            # Adjust task, job and node properties to reflect allocation
                            if task.type is not YarnContainerType.MRAM:
                                if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.HISTORY.value:
                                    self.unscheduled_thin_job_tasks -= 1

                                if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value or self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
                                    self.unscheduled_thin_job_tasks -= 1
                                    current_temp_node_type = node.get_temp_node_type()
                                    #self.auto_tunned_node_types[current_temp_node_type] -= 1
                                    node.set_temp_node_type(PATIENCE_NODE_TYPE.THIN.value)
                                    self.auto_tunned_node_types[PATIENCE_NODE_TYPE.THIN.value] += 1

                            self.stats_accept_decisions_inc(job.job_id)
                            self.handle_container_allocation(node, task.resource, job, task,
                                                             self.state.simulator.getClockMillis(), tillQueueJumpOnly = tillQueueJumpOnly)
                            if not job.pending_tasks:
                                # All of the job's containers were processed: remove it from the queue
                                subqueue.remove(job)
                                #self.job_thin_queue.remove(job)
                            break
                        else:
                            thin_queue_idx += 1
    
                    #if thin_queue_idx == len(self.job_thin_queue):
                    #    break
                if subqueues_taversed == len(self.job_thin_queue):
                    break

    def sample_queue_handler(self, node, allJobsChecked, pass_count):
                # second handle sample queue
            if self.oracle == PATIENCE_ORACLE_STATE.LAS.value:
                return
            scheduling_round_completed = 0
            while True:
                #self.job_sample_queue.sort(key=lambda x: YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal))
                subqueues_taversed = 0
                for subqueue in self.job_sample_queue:
                    subqueues_taversed += 1
                    #subqueue.sort(key=lambda x: YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal))
                    if LOG.isEnabledFor(logging.DEBUG):
                        LOG.debug("QUEUE: " + ", ".join(map(lambda x: "<" +
                                  x.get_name() + " " + str(x.am_launched) + " " +
                                  str(x.consumption) + " " + str(YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal)) +
                                  ">", self.job_queue)))
 
                    sample_queue_idx = 0
                    #while sample_queue_idx < len(self.job_sample_queue):
                    while sample_queue_idx < len(subqueue):
                        #job = self.job_sample_queue[sample_queue_idx]
                        job = subqueue[sample_queue_idx]
                        if job.get_num_sampling_tasks_to_be_assigned() <= 0 and scheduling_round_completed < 1:
                            sample_queue_idx += 1
                            continue
                        if job.get_num_sampling_tasks_to_be_assigned() <= 0 and not allJobsChecked:
                            sample_queue_idx += 1
                            continue
                        if not job.pending_tasks:
                            sample_queue_idx += 1
                            continue
                        task = job.pending_tasks[0]
                        self.stats_decisions_inc(job.job_id)
                        if not job.am_launched and task.type is not YarnContainerType.MRAM:
                            self.stats_reject_decisions_inc(job.job_id)
                            sample_queue_idx += 1
                            continue
                        to_schedule = False
                        tillQueueJumpOnly = False
                        if task.resource <= node.available:
                            to_schedule = True
                        elif self.state.racks["default-rack"].get_specific_node_type_resource_availability(PATIENCE_NODE_TYPE.SAMPLING.value) < task.resource and self.state.eviction_policy != PATIENCE_EVICTION_POLICY.NO.value: #   This is because if other nodes have resources available then eviction should not happen
                        #elif self.state.racks["default-rack"].available < task.resource and self.state.eviction_policy != PATIENCE_EVICTION_POLICY.NO.value: #   This is because if other nodes have resources available then eviction should not happen
                            if job.get_num_sampling_tasks_to_be_assigned() > 0 and job.get_job_initial_number_of_tasks() >= self.get_thin_limit(): #   In case of sampling queue check for eviction only if you have to assign a sampling task.
                                #print "going to call can_evict_something_for_task"
                                can_evict, container_to_be_evicted = node.can_evict_something_for_task(task)
                                if can_evict:
                                    print "going to call evict_container_from_node"
                                    self.evict_container_from_node(container_to_be_evicted, node)
                                    to_schedule = True
                            else:
                                if job.get_job_initial_number_of_tasks() < self.get_thin_limit():
                                    sys.stderr.write("A thin job: "+str(job.job_id)+" not trying eviction for it in the sampling_queue_handler")
                        if to_schedule:
                            if job.get_num_sampling_tasks_to_be_assigned() > 0:
                                if task.type is not YarnContainerType.MRAM:
                                    task.markSamplingTask()
                                    if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.HISTORY.value:
                                        self.unscheduled_sampling_tasks -= 1
                                    
                                    if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value or self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
                                        self.unscheduled_sampling_tasks -= 1
                                        current_temp_node_type = node.get_temp_node_type()
                                        #self.auto_tunned_node_types[current_temp_node_type] -= 1
                                        if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value:
                                            node.set_temp_node_type(PATIENCE_NODE_TYPE.SAMPLING.value)
                                            self.auto_tunned_node_types[PATIENCE_NODE_TYPE.SAMPLING.value] += 1
                                        if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
                                            node.set_temp_node_type(PATIENCE_NODE_TYPE.MAIN.value)
                                            self.auto_tunned_node_types[PATIENCE_NODE_TYPE.MAIN.value] += 1
                                    if job.get_width(self.width_type) >= self.get_thin_limit():
                                        tillQueueJumpOnly = False
                            else:
                                if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.HISTORY.value:
                                    self.unscheduled_main_job_tasks -= 1
                                if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value or self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
                                    self.unscheduled_main_job_tasks -= 1
                                    current_temp_node_type = node.get_temp_node_type()
                                    #self.auto_tunned_node_types[current_temp_node_type] -= 1
                                    node.set_temp_node_type(PATIENCE_NODE_TYPE.MAIN.value)
                                    self.auto_tunned_node_types[PATIENCE_NODE_TYPE.MAIN.value] += 1
                            # Adjust task, job and node properties to reflect allocation
                            if task.type is not YarnContainerType.MRAM:
                                job.increase_sampling_tasks_assigned(1)
                            self.stats_accept_decisions_inc(job.job_id)
                            self.handle_container_allocation(node, task.resource, job, task,
                                                             self.state.simulator.getClockMillis(), tillQueueJumpOnly = tillQueueJumpOnly)
                            if not job.pending_tasks:
                                # All of the job's containers were processed: remove it from the queue
                                # self.job_sample_queue.remove(job)
                                # For this queue it is being taken care in handle_sampling_end 
                                pass
                            #break
                        else:
                            sample_queue_idx += 1
 
                    #if sample_queue_idx == len(self.job_sample_queue):
                    #    break
                scheduling_round_completed += 1
                if subqueues_taversed == len(self.job_sample_queue) and scheduling_round_completed == 2:
                    break
    
 
    def get_auto_tunable_next_node_type(self):
        #   maintain assigned node count
        #   update on node release
        #   compute new node
        next_temp_node_type = None

        if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value:
            w_thin = 60.0
            w_sample = 19.0
            w_main = 2.0
            var_total_psuedo_unscheduled_tasks = w_thin*self.unscheduled_thin_job_tasks + w_sample*self.unscheduled_sampling_tasks + w_main*self.unscheduled_main_job_tasks
            var_total_auto_tunned_nodes = self.auto_tunned_node_types[PATIENCE_NODE_TYPE.THIN.value] + self.auto_tunned_node_types[PATIENCE_NODE_TYPE.SAMPLING.value] + self.auto_tunned_node_types[PATIENCE_NODE_TYPE.MAIN.value]
            if var_total_auto_tunned_nodes == 0: 
                thin_node_share = float("inf") 
                sampling_node_share = float("inf")    
                main_node_share = float("inf") 
            else:
                thin_node_share = self.auto_tunned_node_types[PATIENCE_NODE_TYPE.THIN.value]/var_total_auto_tunned_nodes
                sampling_node_share = self.auto_tunned_node_types[PATIENCE_NODE_TYPE.SAMPLING.value]/var_total_auto_tunned_nodes
                main_node_share = self.auto_tunned_node_types[PATIENCE_NODE_TYPE.MAIN.value]/var_total_auto_tunned_nodes
 
            if var_total_psuedo_unscheduled_tasks == 0:
                thin_task_share = 0
                sampling_task_share = 0
                main_task_share = 0
            else:
                thin_task_share = w_thin*self.unscheduled_thin_job_tasks/var_total_psuedo_unscheduled_tasks
                sampling_task_share = w_sample*self.unscheduled_sampling_tasks/var_total_psuedo_unscheduled_tasks
                main_task_share =  w_main*self.unscheduled_main_job_tasks/var_total_psuedo_unscheduled_tasks
       
            thin_share_diff = thin_node_share - thin_task_share
            sampling_share_diff = sampling_node_share - sampling_task_share
            main_share_diff = main_node_share - main_task_share
            min_share_diff = min(thin_share_diff, sampling_share_diff, main_share_diff)
            
            if thin_share_diff == min_share_diff:
                next_temp_node_type = PATIENCE_NODE_TYPE.THIN.value
            elif sampling_share_diff == min_share_diff:
                next_temp_node_type = PATIENCE_NODE_TYPE.SAMPLING.value
            elif main_share_diff == min_share_diff:
                next_temp_node_type = PATIENCE_NODE_TYPE.MAIN.value
            else:
                raise Exception("get_auto_tunable_next_node_type cannot find which node type to assign")
        elif self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
            temp_thin_node_count = self.auto_tunned_node_types[PATIENCE_NODE_TYPE.THIN.value]
            temp_main_node_count = self.auto_tunned_node_types[PATIENCE_NODE_TYPE.MAIN.value]
            temp_total_assigned_node_count = temp_thin_node_count + temp_main_node_count
            temp_total_node_count = self.total_node_count
            thin_node_current_share = (float(temp_thin_node_count)/float(temp_total_node_count))*100
            if thin_node_current_share < self.state.thin_node_percentage:
                #print "Assigning thin node. temp_thin_node_count: ", temp_thin_node_count, " temp_total_node_count: ", temp_total_node_count, " temp_main_node_count: ", temp_main_node_count, " thin_node_current_share: ", thin_node_current_share, "self.state.thin_node_percentage: ", self.state.thin_node_percentage
                next_temp_node_type = PATIENCE_NODE_TYPE.THIN.value
            else:
                next_temp_node_type = PATIENCE_NODE_TYPE.MAIN.value

        #self.auto_tunned_node_types[next_temp_node_type] += 1
        return next_temp_node_type

    def main_queue_handler(self, node, allJobsChecked, pass_count, onlyFirstQueue = False, noFirstQueue = False, onlyQueueK = False, noQueueK = False, K = None):
                # third handle main queue
            while True:
                subqueues_taversed = 0
                for subqueue in self.job_main_queue:
                    #if onlyFirstQueue:
                    #    if subqueues_taversed > 0:
                    #        subqueues_taversed += 1
                    #        continue
                    #if noFirstQueue:
                    #    if subqueues_taversed == 0:
                    #        subqueues_taversed += 1
                    #        continue
                    if onlyQueueK:
                        if subqueues_taversed != (K - 1):
                            subqueues_taversed += 1
                            continue
                    if noQueueK:
                        if subqueues_taversed == (K - 1):
                            subqueues_taversed += 1
                            continue

                    subqueues_taversed += 1
                    if self.all_or_none == PATIENCE_ALL_OR_NONE_OPTIONS.NON_EVICTIVE.value and len(subqueue) > 1:
                        self.compute_all_or_none_score_for_all_jobs_in_main_queue()
                    #subqueue.sort(key=lambda x: YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal))
                    
                    ########################    Below is for debugging  ###########################
                    #toPrintLine = False 
                    #if len(self.job_main_queue) > 1:
                    #    for job in self.job_main_queue:
                    #        #if job.job_id not in self.debug_list:
                    #            score = YarnPatienceScheduler.compute_job_score(job, self.oracle, self.width_type, self.all_or_none)
                    #            print job.job_id, score, len(job.pending_tasks), job.get_job_initial_number_of_tasks()
                    #            self.debug_list.append(job.job_id)
                    #            toPrintLine = True

                    #if toPrintLine:
                    #    print "--------------------------------"
                    ########################    Above is for debugging  ###########################

                    if LOG.isEnabledFor(logging.DEBUG):
                        LOG.debug("QUEUE: " + ", ".join(map(lambda x: "<" +
                                  x.get_name() + " " + str(x.am_launched) + " " +
                                  str(x.consumption) + " " + str(YarnPatienceScheduler.compute_job_score(x, self.oracle, self.width_type, self.all_or_none, self.state.scheduling_goal)) +
                                  ">", self.job_queue)))
 
                    main_queue_idx = 0
                    while main_queue_idx < len(subqueue):
                        job = subqueue[main_queue_idx]
                        if len(job.pending_tasks) == 0:
                            print job.job_id, subqueues_taversed
                        task = job.pending_tasks[0]
                        self.stats_decisions_inc(job.job_id)
                        if not job.am_launched and task.type is not YarnContainerType.MRAM:
                            self.stats_reject_decisions_inc(job.job_id)
                            main_queue_idx += 1
                            continue
                        
                        #if self.state.dag_scheduling and not job.isSuperParent():  #This is for following strict dag dependency in scheduling.
                        #    temp_job_parent = self.get_job_parent(job)
                        #    if temp_job_parent not in self.completed_jobs:
                        #        main_queue_idx += 1
                        #        continue

                        to_schedule = False
                        if task.resource <= node.available:
                            to_schedule = True
                        elif self.state.racks["default-rack"].get_specific_node_type_resource_availability(PATIENCE_NODE_TYPE.MAIN.value) < task.resource and self.state.eviction_policy != PATIENCE_EVICTION_POLICY.NO.value: #   This is because if other nodes have resources available then eviction should not happen
                        #elif self.state.racks["default-rack"].available < task.resource and self.state.eviction_policy != PATIENCE_EVICTION_POLICY.NO.value: #   This is because if other nodes have resources available then eviction should not happen
                            #print "going to call can_evict_something_for_task"
                            can_evict, container_to_be_evicted = node.can_evict_something_for_task(task)
                            if can_evict:
                                print "going to call evict_container_from_node"
                                self.evict_container_from_node(container_to_be_evicted, node)
                                to_schedule = True
            

                        tillQueueJumpOnly = False   #   No need of queue jump here. As the tasks in queue to avoid starvation and not for acheiving better scheduling. You might change eviction rules to evict a task of lower priority subqueue for a higher priority subqueue
                        if self.state.common_queue == PATIENCE_COMMON_QUEUE.THIN_MAIN.value:
                            if (self.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value or self.oracle == PATIENCE_ORACLE_STATE.LAS.value):
                                raise NotImplementedError()
                            if self.job_is_thin(job):
                            #if job.get_width(self.width_type) < self.get_thin_limit():
                                tillQueueJumpOnly = True
                        if self.oracle == PATIENCE_ORACLE_STATE.LAS.value:
                            tillQueueJumpOnly = True
                        if to_schedule:
                            if task.type is not YarnContainerType.MRAM:
                                if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.HISTORY.value:
                                    self.unscheduled_main_job_tasks -= 1
                                # Adjust task, job and node properties to reflect allocation
                                if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value or self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
                                    self.unscheduled_main_job_tasks -= 1
                                    current_temp_node_type = node.get_temp_node_type()
                                    #self.auto_tunned_node_types[current_temp_node_type] -= 1
                                    node.set_temp_node_type(PATIENCE_NODE_TYPE.MAIN.value)
                                    self.auto_tunned_node_types[PATIENCE_NODE_TYPE.MAIN.value] += 1
                            self.stats_accept_decisions_inc(job.job_id)
                            self.handle_container_allocation(node, task.resource, job, task,
                                                             self.state.simulator.getClockMillis(), tillQueueJumpOnly = tillQueueJumpOnly)
                            if not job.pending_tasks:
                                # All of the job's containers were processed: remove it from the queue
                                subqueue.remove(job)
                                try:
                                    pass
                                    #del self.main_queue_job_wise_pending_resource_requirements_dict[job.job_id]
                                except KeyError:
                                    if self.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value or self.oracle == PATIENCE_ORACLE_STATE.POINT_MEDIAN.value or self.oracle == PATIENCE_ORACLE_STATE.THIN_ONLY_THREE_SIGMA.value:
                                        pass
                                    else:
                                        print "main_queue_handler unexpected error in dealing with a dictionary. Going to call sys.exit(0)"
                                        sys.exit(0)
                            break
                        else:
                            main_queue_idx += 1
 
                    #if main_queue_idx == len(self.job_main_queue):
                    #    break
                if subqueues_taversed == len(self.job_main_queue):
                    break

    def tasks_scheduling_order_has_changed(self, running_container, node):
        decision_to_return = False
        task = running_container.task
        job = task.job
        job_current_queue = job.get_job_current_queue()
        task_scheduling_queue = task.getSchedulingQueue()
        if task.type is not YarnContainerType.MRAM:
            if job_current_queue != PATIENCE_QUEUE_NAMES.SAMPLE.value:
                if node.is_sampling_node() and task_scheduling_queue == PATIENCE_QUEUE_NAMES.SAMPLE.value:
                    decision_to_return = True 
            else:
                job_subqueue = job.get_job_current_subqueue()
                task_subqueue = task.getSchedulingSubQueue()
                if task_subqueue < job_subqueue:
                    decision_to_return = True 
        
        return decision_to_return

    def schedule(self, node):
        #if node.node_id == 16: sys.stderr.write("node_id: "+str(node.node_id)+ " num_allocated_containers: "+str(len(node.allocated_containers))+ " num_running_containers: "+str(len(node.running_containers)) + " next_heartbeat_time: " + str(node.next_heartbeat.time_millis) + "\n") #for container in node.running_containers: #    sys.stderr.write("container_task_type: "+str(container.task.type)+" ") #sys.stderr.write("\n")
        
        if not self.has_active_jobs():
            return False, (EventResult.CONTINUE,)

        #temp_set_of_containers = set()
        #for container in node.running_containers:
        #    if container.type is YarnContainerType.MRAM:
        #        continue
        #    if self.tasks_scheduling_order_has_changed(container, node):
        #        temp_set_of_containers.add(container)
        #
        #for container in temp_set_of_containers:
        #    self.evict_container_from_node(container, node) 
 
        #   deal the auto_tunable case here. In autotunable mode assigning all the nodes as universal.
        if self.state.user_config.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value: #or self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
            next_node_type = self.get_auto_tunable_next_node_type()
            #if next_node_type == None:  #   Presently if get_auto_tunable_next_node_type() returns None then it means that no unscheduled task exists.
            #    return False, (EventResult.CONTINUE,)
            #node.set_temp_node_type(next_node_type)
        else:
            if node.is_thin_node():
                next_node_type = PATIENCE_NODE_TYPE.THIN.value
            elif node.is_main_node():
                next_node_type = PATIENCE_NODE_TYPE.MAIN.value
            elif node.is_sampling_node():
                next_node_type = PATIENCE_NODE_TYPE.SAMPLING.value
            elif node.is_universal_node():
                next_node_type = PATIENCE_NODE_TYPE.UNIVERSAL.value
            else:
                next_node_type = PATIENCE_NODE_TYPE.UNIVERSAL.value ##JS_hehe
        allJobsChecked = False
        pass_count = 0
        while True: #   This bigger loop is for the purpose of coming back to sampling queue for work conservation
            pass_count += 1
            #if node.is_thin_node():
            if next_node_type == PATIENCE_NODE_TYPE.THIN.value:
                raise Exception("Should not have come here. thin node not be used")
                #self.thin_queue_handler(node, allJobsChecked, pass_count)
                #self.sample_queue_handler(node, allJobsChecked, pass_count)
                #self.main_queue_handler(node, allJobsChecked, pass_count)
                self.thin_queue_handler(node, allJobsChecked, pass_count)
                self.main_queue_handler(node, allJobsChecked, pass_count, onlyQueueK = True, K = 1)#onlyFirstQueue = True)
                self.sample_queue_handler(node, allJobsChecked, pass_count)
                self.main_queue_handler(node, allJobsChecked, pass_count, noQueueK = True, K = 1)#,noFirstQueue = True)
                #raise Exception("Update sample and main order to main highest sample main rest here.")
            #elif node.is_sampling_node():
            elif next_node_type == PATIENCE_NODE_TYPE.SAMPLING.value:
                #if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.HISTORY.value:
                raise Exception("Should not have come here. Sampling node not be used in PATIENCE_PARTITION_TUNNING.HISTORY")
                self.sample_queue_handler(node, allJobsChecked, pass_count)
                self.main_queue_handler(node, allJobsChecked, pass_count)
                self.thin_queue_handler(node, allJobsChecked, pass_count)
            #elif node.is_main_node():
            elif next_node_type == PATIENCE_NODE_TYPE.MAIN.value:
                #if self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.HISTORY.value:
                    if self.state.starvation_freedom:
                        temp_K = self.get_next_starve_free_queue() + 1
                    else:
                        temp_K = 1
                    if temp_K != 1:
                        print "Starvation_freedom: temp_K: ", temp_K, " current_time_millis: ", self.state.simulator.getClockMillis()
                    self.main_queue_handler(node, allJobsChecked, pass_count, onlyQueueK = True, K = temp_K)#onlyFirstQueue = True)
                    self.sample_queue_handler(node, allJobsChecked, pass_count)
                    self.main_queue_handler(node, allJobsChecked, pass_count, noQueueK = True, K = temp_K)#,noFirstQueue = True)
                    #self.thin_queue_handler(node, allJobsChecked, pass_count)
                #else:
                #    self.main_queue_handler(node, allJobsChecked, pass_count)
                #    self.thin_queue_handler(node, allJobsChecked, pass_count)
                #    self.sample_queue_handler(node, allJobsChecked, pass_count)
            #elif node.is_universal_node():
            elif next_node_type == PATIENCE_NODE_TYPE.UNIVERSAL.value:
                if self.state.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                    self.thin_queue_handler(node, allJobsChecked, pass_count)
                    self.sample_queue_handler(node, allJobsChecked, pass_count)
                    self.main_queue_handler(node, allJobsChecked, pass_count)
                elif self.state.scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
                    raise Exception("Should not have come here universal node.")
                #raise Exception("Unidentified next_node_type: ", str(next_node_type))

            if self.has_pending_jobs() and pass_count < 2:
                allJobsChecked = True
            else:
                break
 
        #if self.state.simulator.getClockMillis() not in self.log_printed_time_dict:
        #    self.log_printed_time_dict[self.state.simulator.getClockMillis()] = 1
        #    temp_total_pending_tasks = 0.0
        #    temp_total_running_tasks = 0.0
        #    for job in self.running_jobs:
        #        temp_total_pending_tasks += len(job.pending_tasks)  #   This is valid only when number of containers in each task is 1. i.e. your trace has c.nr = 1
        #        temp_total_running_tasks += len(job.running_tasks)
        #    temp_total_active_tasks = temp_total_pending_tasks + temp_total_running_tasks
        #    if temp_total_active_tasks != 0:
        #        temp_waiting_frac = temp_total_pending_tasks/temp_total_active_tasks
        #        temp_running_frac = temp_total_running_tasks/temp_total_active_tasks
        #    else:
        #        temp_waiting_frac = float("inf")
        #        temp_running_frac = float("inf")
        #    if temp_total_running_tasks != 0:
        #        temp_waiting_by_running = temp_total_pending_tasks/temp_total_running_tasks
        #    else:
        #        temp_waiting_by_running = float("inf")
        #    print "Backlog,", self.state.simulator.getClockMillis(), ",",temp_total_pending_tasks, ",",temp_total_running_tasks, ",",temp_total_active_tasks, ",",temp_waiting_frac,",",temp_running_frac,",",temp_waiting_by_running 
        #print "Num jobs: ", len(self.state.jobs)
        #print "Node shares: ", self.auto_tunned_node_types[PATIENCE_NODE_TYPE.THIN.value], ",", self.auto_tunned_node_types[PATIENCE_NODE_TYPE.SAMPLING.value], ",", self.auto_tunned_node_types[PATIENCE_NODE_TYPE.MAIN.value], ","

        return len(node.allocated_containers) > 0, (EventResult.CONTINUE,)

    def has_pending_jobs(self):
        return self.has_active_jobs()
        #return (bool(self.job_thin_queue) or bool(self.job_sample_queue) or bool(self.job_main_queue))
