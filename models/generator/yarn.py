#!/usr/bin/env python
import logging
from random import Random
import sys

from events.yarn.elastic.peek import YarnPeekJobArriveEvent, YarnPeekSimulationFinishEvent,\
    YarnPeekSimulationStartEvent, YarnPeekStandaloneSimulationFinishEvent
from events.yarn.elastic.race import YarnRaceSimulationFinishEvent, YarnRaceContinuousStandaloneSimulationFinishEvent, \
    YarnRaceSynchronousJobArriveEvent, YarnRaceJobJobArriveEvent, YarnRaceNodeGJobArriveEvent
from events.yarn.symbex import YarnSymbexSimulationFinishEvent, YarnSymbexSimulationStartEvent, SymbexMode
from events.yarn.yarn import YarnStandaloneSimulationFinishEvent, YarnJobArriveEvent, YarnSimulationStartEvent
from models.state import SymbexState, YarnState
from models.yarn.objects import YarnContainerType, YarnSimulation, YarnPrototypeContainer, YarnResource
from models.yarn.penalties import get_penalty
from parsers.sls import SLSParser
from schedulers.oracle import YarnGlobalDecisionScheduler, YarnJobDecisionScheduler, YarnNodeGDecisionScheduler, \
    YarnPeekScheduler
from schedulers.srtf import YarnSRTFScheduler
from schedulers.patience import YarnPatienceScheduler
from schedulers.patience import DEFAULT_SAMPLING_PERCENTAGE, DEFAULT_THIN_LIMIT
from utils import PATIENCE_ORACLE_STATE 
from utils import VCORE_COUNT_SOURCE 
from utils import PATIENCE_ALL_OR_NONE_OPTIONS 
from schedulers.symbex import YarnSymbexScheduler
from schedulers.elastic import YARN_MIN_ALLOCATION_MB, YarnGreedyScheduler, YarnSmartgScheduler
#from schedulers.yarn import YarnSchedulerType, YarnRegularScheduler
from schedulers.yarn import YarnRegularScheduler
from utils import YarnSchedulerType 
from utils import PATIENCE_EVICTION_POLICY, PATIENCE_NODE_TYPE, PATIENCE_CLUSTER_SPLIT_OPTIONS, PATIENCE_PARTITION_TUNNING, PATIENCE_COMMON_QUEUE
from models.threesigma.ThreeSigma import ThreeSigmaPredictor
from utils import THREESIGMA_FEATURES, THREESIGMA_METRICS, THREESIGMA_UTILITY_FUNCTIONS
from utils import YARN_EXECUTION_TYPES, SCHEDULING_GOALS, DEADLINE_MULTIPLIER, PATIENCE_ADAPTIVE_SAMPLING_TYPES
import utils

sys.path.insert(0,'/export1/ajajoo/Desktop/Research/jobScheduling/DSS-Scheduler/DSS-me/analysis/scripts')
sys.path.insert(0, "/data/ajajoo/Desktop/Research/jobScheduling/DSS-Scheduler/DSS-me/")
import jsLib

LOG = logging.getLogger("yarn_generator")

GENERATOR_RANDOM_SEED = 23482374928374


class YarnGenerator(object):
    """ The YarnGenerator will create a list of YarnJob objects
    from the output of the SLSParser.
    Items added to the state:
    - the node configuration
    - the rack configuration
    - the scheduler used
    """

    def __init__(self, state=None):
        self.state = state
        self.random = Random(GENERATOR_RANDOM_SEED)

    def generate_state(self, simulator, args):

        user_config = args

        # Check if this is a SYMBEX simulation.
        if user_config.symbex:
            self.state = SymbexState(user_config=user_config)
            # Override scheduler type
            if user_config.symbex_mode is SymbexMode.DECISION:
                self.state.scheduler_type = YarnSchedulerType.SYMBEX
            elif user_config.symbex_mode is SymbexMode.RACE:
                self.state.scheduler_type = YarnSchedulerType.SMARTG

        else:
#            print "AJ-US-CF: YarnState it is."
            self.state = YarnState(user_config=user_config)
            self.state.experiment_identifier = ""   #AJ_code
            self.state.scheduler_type = user_config.scheduler_type
            self.state.experiment_identifier += (self.state.scheduler_type.name + "-") #AJ_code

        self.state.user_config = user_config
        self.state.simulator = simulator

        if user_config.execution_mode == YARN_EXECUTION_TYPES.COORDINATOR.value:
            #self.state.base_output_directory = "/home/ajajoo/Desktop/Research/jobScheduling/DSS-Scheduler/DSS-me/output/" #AJ_code
            self.state.base_output_directory = "/home/ajajoo/" #AJ_code
        else:
            #self.state.base_output_directory = "/home/ajajoo/Desktop/Research/jobScheduling/DSS-Scheduler/DSS-me/output/" #AJ_code
            self.state.base_output_directory = "/export1/ajajoo/Desktop/Research/jobScheduling/DSS-Scheduler/DSS-me/output/" #AJ_code
        self.state.base_results_directory = self.state.base_output_directory+"results/" #AJ_code
        self.state.base_logs_directory = self.state.base_output_directory+"logs/"   #AJ_code
        self.state.experiment_identifier += user_config.output_distinguisher + "-"
        # Get the default penalty model, if set
        default_task_penalty = None
        if user_config.penalty is not None and user_config.ib is not None:
            default_task_penalty = get_penalty(user_config.penalty, user_config.ib)
        
        if user_config.vcore_count_source is None:  #AJ_code -- whole if_else_block
            self.state.vcore_count_source = VCORE_COUNT_SOURCE.DEFAULT.value
        else:
            if user_config.vcore_count_source.lower() == "trace":
                self.state.vcore_count_source = VCORE_COUNT_SOURCE.TRACE.value
            elif user_config.vcore_count_source.lower() == "one":
                self.state.vcore_count_source = VCORE_COUNT_SOURCE.ONE.value
            else:
                print "unexpected vcore_count_source: ", user_config.vcore_count_source, "setting it to default."
                self.state.vcore_count_source = VCORE_COUNT_SOURCE.DEFAULT.value
        self.state.experiment_identifier += "vcs_"+str(self.state.vcore_count_source) + "-"
        #self.state.experiment_identifier += "vcore_cs_"+str(self.state.vcore_count_source) + "-"

        if user_config.scheduling_goal is None: #   AJ_code -- whole if_else_block
            self.state.scheduling_goal = SCHEDULING_GOALS.DEFAULT.value
        else:
            self.state.scheduling_goal = user_config.scheduling_goal
        self.state.experiment_identifier += "sg_"+str(self.state.scheduling_goal) + "-"
      
        if user_config.conservative_drop is None: #   AJ_code -- whole if_else_block
            self.state.conservative_drop = False
        else:
            self.state.conservative_drop = bool(user_config.conservative_drop)
        print  "\n\n\n","user_config.conservative: ", user_config.conservative_drop, "\n\n\n"
        self.state.experiment_identifier += "cd_"+str(int(self.state.conservative_drop)) + "-"
        if user_config.job_arrival_time_stretch_multiplier is None:
            pass
            #self.state.job_arrival_time_stretch_multiplier = 1
        else:
            self.state.job_arrival_time_stretch_multiplier = user_config.job_arrival_time_stretch_multiplier 
            if float(self.state.job_arrival_time_stretch_multiplier) != 1.0:
                self.state.experiment_identifier += "arrS_"+str(self.state.job_arrival_time_stretch_multiplier) + "-"
        if self.state.scheduling_goal is SCHEDULING_GOALS.DEADLINE.value:
            self.state.experiment_identifier += "dm_"+str(DEADLINE_MULTIPLIER) + "-"
            self.state.total_work_done_within_deadline = 0
            self.state.total_wasteful_work = 0
            self.state.successful_tasks = 0
            self.state.wasteful_tasks = 0

        if utils.JOB_RUNTIME_STRETCHER != 1.0 or utils.JOB_RUNTIME_STRETCHER != 1:
            self.state.experiment_identifier += "jrs_"+str(utils.JOB_RUNTIME_STRETCHER) + "-"

        self.state.dag_scheduling = 0
        if user_config.dag_scheduling == 1:
            self.state.dag_scheduling = user_config.dag_scheduling
            self.state.experiment_identifier += "dag_"+str(user_config.dag_scheduling) + "-"
 
        # Create a new SLSParser to read the simulation data
        print "\n\n\t\t\t\t Creating parser \n\n"
        sls_parser = SLSParser(
            sls_file=user_config.sls_trace_file,
            topo_file=user_config.network_topology_file,
            node_mem_capacity=user_config.node_mem_mb,
            node_core_capacity=user_config.node_cores,
            node_hb_ms=user_config.node_hb_ms,
            am_hb_ms=user_config.am_hb_ms,
            am_container_mb=user_config.am_container_mb,
            am_container_cores=user_config.am_container_cores,
            use_meganode=user_config.meganode,
            default_task_penalty=default_task_penalty,
            vcore_count_source=self.state.vcore_count_source,
            all_jobs_arrive_at_begining=user_config.all_jobs_arrive_at_begining,
            lines_to_read = user_config.lines_per_job_entry,
            job_arrival_time_stretch_multiplier = user_config.job_arrival_time_stretch_multiplier)
        print "\n\n\t\t\t\t Created parser \n\n"

        # Call the SLSParser to read the config files
        yarn_jobs = sls_parser.get_yarn_jobs()
        print "\n\n\t\t\t\t Got the jobs \n\n"
        yarn_topo = sls_parser.get_yarn_topo()
        print "\n\n\t\t\t\t Got the topo \n\n"


        self.state.jid_job_dict = {}
        # Parse jobs and add random user overestimation of memory
#        print "AJ-US-CF: going to the loop for jobs"
        overestimation_range = user_config.mem_overestimate_range
        for job in yarn_jobs:
            overestimation_factor = self.random.uniform(overestimation_range[0], overestimation_range[1])
            self.state.jid_job_dict[job.job_id] = job
            for task in job.tasks:
                if task.type is YarnContainerType.MRAM:
                    continue
                if task.resource.memory_mb < YARN_MIN_ALLOCATION_MB:
                    raise Exception("Job " + str(job) + " has tasks with memory" +
                                    " less than the minimum allocatable amount (" + str(YARN_MIN_ALLOCATION_MB) +
                                    ") : " + str(task.resource.memory_mb))
                task.ideal_resource.memory_mb = \
                    max(int(task.resource.memory_mb / overestimation_factor),
                        YARN_MIN_ALLOCATION_MB)
            job.pending_tasks = list(
                YarnPrototypeContainer(resource=YarnResource(container.resource.memory_mb, container.resource.vcores),
                                       priority=container.priority, container_type=container.type, job=job,
                                       duration=container.duration, num_containers=container.num_containers,
                                       ideal_resource=container.ideal_resource, penalty=container.penalty, tid_within_job=container.tid_within_job)
                for container in job.tasks)

        # Add node config
        self.state.jobs = yarn_jobs
        self.state.racks = yarn_topo[0]
        self.state.nodes = yarn_topo[1]
        self.state.num_nodes = len(self.state.nodes)
        #self.state.experiment_identifier += "jobs_"+str(len(self.state.jobs)) + "-"
        self.state.experiment_identifier += "j_"+str(len(self.state.jobs)) + "-"
        #self.state.experiment_identifier += "nodes_"+str(len(self.state.nodes)) + "-"
        self.state.experiment_identifier += "n_"+str(self.state.num_nodes) + "-"
        #self.state.experiment_identifier += "nodeCores_"+str(user_config.node_cores) + "-"
        self.state.experiment_identifier += "nc_"+str(user_config.node_cores) + "-"
        self.state.execution_mode = user_config.execution_mode
 
        if user_config.oracle is None:
            print "Setting oracle to default value as no commandline argument given."
            self.state.oracle = PATIENCE_ORACLE_STATE.DEFAULT.value
        else:
            self.state.oracle = user_config.oracle
        # Add PATIENCE specific parameters
        if self.state.scheduler_type is YarnSchedulerType.PATIENCE or (self.state.scheduler_type is YarnSchedulerType.SRTF and user_config.oracle == PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value):
            if user_config.thin_limit is None:
                print "Setting thin_limit to default value as no commandline argument given."
                self.state.thin_limit = DEFAULT_THIN_LIMIT
            else: 
                self.state.thin_limit = user_config.thin_limit
            if user_config.sampling_percentage is None:
                print "Setting sampling_percentage to default value as no commandline argument given."
                self.state.sampling_percentage = DEFAULT_SAMPLING_PERCENTAGE
            else:
                self.state.sampling_percentage = user_config.sampling_percentage
            if user_config.oracle is None:
                print "Setting oracle to default value as no commandline argument given."
                self.state.oracle = PATIENCE_ORACLE_STATE.DEFAULT.value
            else:
                self.state.oracle = user_config.oracle
            if user_config.all_or_none is None:
                print "Setting all_or_none to default value as no commandline argument given."
                self.state.all_or_none = PATIENCE_ALL_OR_NONE_OPTIONS.DEFAULT.value 
            else:
                self.state.all_or_none = user_config.all_or_none
            if user_config.eviction_policy is None:
                print "Setting eviction to default value as no commandline argument given."
                self.state.eviction_policy = PATIENCE_EVICTION_POLICY.DEFAULT.value 
            else:
                self.state.eviction_policy = user_config.eviction_policy
            if user_config.split_cluster is None:
                print "Setting split_cluster to default as no commandline argument given."
                self.state.split_cluster = PATIENCE_CLUSTER_SPLIT_OPTIONS.DEFAULT.value 
            else:
                self.state.split_cluster = user_config.split_cluster 
            if user_config.sampling_node_percentage is None:
                print "Setting sampling_node_percentage to 0 as no commandline argument given."
                self.state.sampling_node_percentage = 0
            else:
                self.state.sampling_node_percentage = user_config.sampling_node_percentage
                print "Sampling_node_percentage: ", self.state.sampling_node_percentage
            if user_config.adaptive_sampling is None:
                print "Setting adaptive_sampling to 0 as no commandline argument given."
                self.state.adaptive_sampling = 0
            else:
                self.state.adaptive_sampling = user_config.adaptive_sampling
                print "adaptive_sampling: ", self.state.adaptive_sampling
                if self.state.adaptive_sampling != PATIENCE_ADAPTIVE_SAMPLING_TYPES.NO.value:
                    self.state.experiment_identifier += "adsmp_"+str(self.state.adaptive_sampling) + "-"
            if user_config.thin_node_percentage is None:
                if self.state.split_cluster == PATIENCE_CLUSTER_SPLIT_OPTIONS.NO.value:
                    self.state.thin_node_percentage = 0 
                else:
                    raise Exception("Please define thin node percentage.")
            else:
                self.state.thin_node_percentage = user_config.thin_node_percentage 
            if user_config.auto_tunable_partition is not None:
                self.state.auto_tunable_partition = user_config.auto_tunable_partition 
                if self.state.auto_tunable_partition != PATIENCE_PARTITION_TUNNING.MANUAL.value:
                    self.state.experiment_identifier += "atp_"+str(self.state.auto_tunable_partition) + "-"
            if user_config.common_queue is not None:
                self.state.common_queue = user_config.common_queue
                if self.state.common_queue != PATIENCE_COMMON_QUEUE.OFF.value:
                    self.state.experiment_identifier += "cq_"+str(self.state.common_queue) + "-"
 
            self.state.coordinator_ip = user_config.coordinator_ip
            self.state.coordinator_listener_port = user_config.coordinator_listener_port
            #self.state.experiment_identifier += "thinLimit_"+str(self.state.thin_limit) + "-"
            self.state.experiment_identifier += "tl_"+str(self.state.thin_limit) + "-"
            self.state.experiment_identifier += "sp_"+str(self.state.sampling_percentage) + "-"
            #self.state.experiment_identifier += "oracle_"+str(self.state.oracle) + "-"
            self.state.experiment_identifier += "orac_"+str(self.state.oracle) + "-"
            #self.state.experiment_identifier += "allOrNone_"+str(self.state.all_or_none) + "-"
            self.state.experiment_identifier += "aOrN_"+str(self.state.all_or_none) + "-"
            #self.state.experiment_identifier += "eviction_"+str(self.state.eviction_policy) + "-"
            self.state.experiment_identifier += "evic_"+str(self.state.eviction_policy) + "-"
            self.state.experiment_identifier += "snp_"+str(self.state.sampling_node_percentage) + "-"
            self.state.experiment_identifier += "tnp_"+str(self.state.thin_node_percentage) + "-"
            #self.state.experiment_identifier += "am_cores_"+str(user_config.am_container_cores) + "-"
            self.state.experiment_identifier += "amc_"+str(user_config.am_container_cores) + "-"
            #self.state.experiment_identifier += "split_cluster_"+str(self.state.split_cluster) + "-"
            self.state.experiment_identifier += "sc_"+str(self.state.split_cluster) + "-"
 
            self.state.all_thin_bypass = bool(int(user_config.all_thin_bypass))
            if self.state.all_thin_bypass:
                self.state.experiment_identifier += "tb_"+str(1) + "-"

        if user_config.execution_mode == YARN_EXECUTION_TYPES.COORDINATOR.value:
            #self.state.experiment_identifier += "exec_mode_"+str(self.state.execution_mode) + "-"
            self.state.experiment_identifier += "em_"+str(self.state.execution_mode) + "-"
            self.state.coordinator_ip = user_config.coordinator_ip
            self.state.coordinator_listener_port = user_config.coordinator_listener_port

        if self.state.scheduler_type is YarnSchedulerType.PATIENCE: #   Following block deals with assignment of special sampling nodes.
           for rack in self.state.racks:
               num_sampling_node = (len(self.state.racks[rack].nodes)*self.state.sampling_node_percentage)/100
               num_thin_node = (len(self.state.racks[rack].nodes)*self.state.thin_node_percentage)/100
               count = 0
               for node in self.state.racks[rack].nodes:
                   if self.state.split_cluster == PATIENCE_CLUSTER_SPLIT_OPTIONS.NO.value or self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.AUTO.value or self.state.auto_tunable_partition == PATIENCE_PARTITION_TUNNING.OFF.value:
                       node.set_node_type(PATIENCE_NODE_TYPE.UNIVERSAL.value)
                   else:
                       if count < num_sampling_node:
                           #print "assigning sampling node"
                           node.set_node_type(PATIENCE_NODE_TYPE.SAMPLING.value)
                           self.state.racks[rack].add_sampling_node(node)
                       elif count > num_sampling_node and count < (num_sampling_node + num_thin_node):
                           #print "assigning thin node"
                           node.set_node_type(PATIENCE_NODE_TYPE.THIN.value)
                           self.state.racks[rack].add_thin_node(node)
                       else:
                           #print "assigning main node"
                           node.set_node_type(PATIENCE_NODE_TYPE.MAIN.value)
                           self.state.racks[rack].add_main_node(node)
                   count += 1
        if self.state.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value or self.state.oracle == PATIENCE_ORACLE_STATE.THIN_ONLY_THREE_SIGMA.value or self.state.oracle == PATIENCE_ORACLE_STATE.POINT_MEDIAN.value or (self.state.scheduler_type == YarnSchedulerType.PATIENCE and self.state.oracle == PATIENCE_ORACLE_STATE.NO.value and self.state.dag_scheduling):
            prediction_utility_function = None
            if self.state.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value:
                prediction_utility_function = THREESIGMA_UTILITY_FUNCTIONS.AVERAGE_JCT.value
            elif self.state.oracle == PATIENCE_ORACLE_STATE.POINT_MEDIAN.value:
            #   This will use only one feature. If application name is available then will use it else, user_name
                prediction_utility_function = THREESIGMA_UTILITY_FUNCTIONS.POINT_MEDIAN.value
            else:
                if self.state.dag_scheduling:   #   This is to use 3Sigma prediction for other non super parent stages in patience hybrid.
                    prediction_utility_function = THREESIGMA_UTILITY_FUNCTIONS.AVERAGE_JCT.value
                else:
                    prediction_utility_function = None
            self.state.three_sigma_predictor = ThreeSigmaPredictor(featuresList = [x.value for x in THREESIGMA_FEATURES], warmUpFile = user_config.threesigma_warmup_file, metricList = [x.value for x in THREESIGMA_METRICS], featuresFile = user_config.threesigma_features_file, utility_function_name = prediction_utility_function, scheduling_goal = self.state.scheduling_goal)
            if user_config.threesigma_secondary_warmup_file is not None:
                temp_secondary_scheduling_goal = SCHEDULING_GOALS.AVERAGE_JCT.value
                sys.stderr.write("Creating threesigma_secondary_warmup_object with scheduling goal = "+str(temp_secondary_scheduling_goal)+"\n")
                sys.stdout.write("Creating threesigma_secondary_warmup_object with scheduling goal = "+str(temp_secondary_scheduling_goal)+"\n")
                self.state.secondary_three_sigma_predictor = ThreeSigmaPredictor(featuresList = [x.value for x in THREESIGMA_FEATURES], warmUpFile = user_config.threesigma_secondary_warmup_file, metricList = [x.value for x in THREESIGMA_METRICS], featuresFile = user_config.threesigma_features_file, utility_function_name = prediction_utility_function, scheduling_goal = temp_secondary_scheduling_goal)
            else:
                self.state.secondary_three_sigma_predictor = None
        # Create and add the scheduler
        self.state.scheduler = self._get_scheduler(user_config, yarn_topo)
 
        # Add the generator itself
        self.state.generator = self

        return self.state

    def _get_scheduler(self, user_config, yarn_topo):
        elastic_pool = None
        if user_config.ep is not None:
            elastic_pool = user_config.ep
        scheduler_type = self.state.scheduler_type
        if scheduler_type is YarnSchedulerType.REGULAR:
            print "AJ-US-CF: Yarn regular scheduler it is"
            return YarnRegularScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.SRTF:
            #print "Currently SRTF is running Patience Scheduler"
            #print "samplingPercentage for YarnPatienceScheduler is hardcoded as 10. SET_IT_HERE_FROM_CLI"
            return YarnSRTFScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.GREEDY:
            return YarnGreedyScheduler(state=self.state, node_count=len(yarn_topo[1]), elastic_pool=elastic_pool)
        elif scheduler_type is YarnSchedulerType.SMARTG:
            return YarnSmartgScheduler(state=self.state, node_count=len(yarn_topo[1]), elastic_pool=elastic_pool)
        elif scheduler_type is YarnSchedulerType.SYMBEX:
            return YarnSymbexScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.PATIENCE:
            return YarnPatienceScheduler(state=self.state) #AJ_Code
        elif scheduler_type is YarnSchedulerType.RACE_LOCKSTEP or \
                scheduler_type is YarnSchedulerType.RACE_CONTINUOUS:
            self.state.oracle_type = scheduler_type
            return YarnGlobalDecisionScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.RACE_JOB:
            self.state.oracle_type = scheduler_type
            return YarnJobDecisionScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.RACE_NODEG:
            self.state.oracle_type = scheduler_type
            return YarnNodeGDecisionScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.PEEK:
            self.state.oracle_type = scheduler_type
            return YarnPeekScheduler(state=self.state)
        else:
            raise Exception("Invalid scheduler model specified: " + scheduler_type)

 
    def get_simulation_start_event(self):
        printed_AJLog = 1 
        if self.state.user_config.symbex:
#            print "AJ-US-CF: get_simulation_start_event if symbex"
            printed_AJLog = 0 
            return YarnSymbexSimulationStartEvent(self.state)

        if self.state.is_oracle_simulation:
            if self.state.oracle_type is YarnSchedulerType.RACE_CONTINUOUS:
                printed_AJLog = 0 
#                print "AJ-US-CF: get_simulation_start_event if ORACLE RACE_CONTINUOUS"
                return YarnRaceContinuousStandaloneSimulationFinishEvent(self.state)
            elif self.state.oracle_type is YarnSchedulerType.PEEK:
                printed_AJLog = 0 
#                print "AJ-US-CF: get_simulation_start_event if ORACLE PEEK"
                return YarnPeekSimulationStartEvent(self.state)

        #if printed_AJLog:
#            print "AJ-US-CF: no change"
        return YarnSimulationStartEvent(self.state)

    def get_simulation_finish_event(self):
        if self.state.simulation_type is YarnSimulation.STANDALONE:
            if self.state.oracle_type is YarnSchedulerType.RACE_CONTINUOUS:
                return YarnRaceContinuousStandaloneSimulationFinishEvent(self.state)
            elif self.state.oracle_type is YarnSchedulerType.PEEK:
                return YarnPeekStandaloneSimulationFinishEvent(self.state)
            else:
                return YarnStandaloneSimulationFinishEvent(self.state)
        elif self.state.simulation_type is YarnSimulation.SYMBEX:
            return YarnSymbexSimulationFinishEvent(self.state)
        elif self.state.simulation_type is YarnSimulation.RACE:
            return YarnRaceSimulationFinishEvent(self.state)
        elif self.state.simulation_type is YarnSimulation.PEEK:
            return YarnPeekSimulationFinishEvent(self.state)
        else:
            raise Exception("Unidentified Simulation type")

    def get_job_arrive_event(self, job):
        if self.state.is_oracle_simulation and self.state.simulation_type is YarnSimulation.STANDALONE:
            if self.state.oracle_type is YarnSchedulerType.RACE_LOCKSTEP:
                return YarnRaceSynchronousJobArriveEvent(self.state, job)
            elif self.state.oracle_type is YarnSchedulerType.RACE_JOB:
                return YarnRaceJobJobArriveEvent(self.state, job)
            elif self.state.oracle_type is YarnSchedulerType.RACE_NODEG:
                return YarnRaceNodeGJobArriveEvent(self.state, job)
            elif self.state.oracle_type is YarnSchedulerType.PEEK:
                return YarnPeekJobArriveEvent(self.state, job)
            else:
                raise Exception("Invalid RACE simulation.")

        else:
            return YarnJobArriveEvent(self.state, job)
