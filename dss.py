#!/usr/bin/env python

import logging
import pdb
import signal
import sys

import argparse

from models.generator.yarn import YarnGenerator
from models.yarn.objects import YarnErrorType, YarnErrorMode
from models.yarn.penalties import YarnPenalty
from schedulers.oracle import PushbackStrategy
from schedulers.yarn import YarnSchedulerType
from simulator import Simulator
from testbed.coordinator.coordinator import Coordinator
from symbex import SymbexRunner
from events.yarn.symbex import SymbexMode
from utils import parsenumlist, get_enumparser, YARN_EXECUTION_TYPES, PATIENCE_PARTITION_TUNNING, PATIENCE_COMMON_QUEUE, SCHEDULING_GOALS
import testbed.common as tbcmn
import str2bool

logging.basicConfig(level=logging.WARNING, format='%(asctime)-15s %(levelname)-8s %(name)s : %(message)s')
#logging.basicConfig(level=logging.DEBUG, format='%(asctime)-15s %(levelname)-8s %(name)s : %(message)s')

# noinspection PyUnusedLocal
def signal_handler(signum, frame):
    pdb.set_trace()


def verify_user_config(args):
    if (args.penalty is not None and args.ib is None) or \
            (args.ib is not None and args.penalty is None):
        sys.stderr.write("Must specify both PENALTY and IB value.\n")
        sys.exit(-1)
    if args.duration_error is not None:
        if args.duration_error_mode is YarnErrorMode.CONSTANT and \
                args.duration_error_type is YarnErrorType.MIXED:
            sys.stderr.write("CONSTANT duration errors cannot be of the type MIXED.")
            sys.exit(-1)
        if (args.duration_error_type is YarnErrorType.NEGATIVE or args.duration_error_type is YarnErrorType.MIXED) and \
                args.duration_error >= 100:
            sys.stderr.write("Current settings would create containers with negative duration.")
            sys.exit(-1)
    if args.mem_error is not None:
        if args.mem_error_mode is YarnErrorMode.CONSTANT and \
                args.mem_error_type is YarnErrorType.MIXED:
            sys.stderr.write("CONSTANT memory errors cannot be of the type MIXED.")
            sys.exit(-1)
        if (args.mem_error_type is YarnErrorType.NEGATIVE or args.mem_error_type is YarnErrorType.MIXED) and \
                args.mem_error >= 100:
            sys.stderr.write("Current memory error settings would create containers with negative ideal memory.")
            sys.exit(-1)
        if (args.mem_error_type is YarnErrorType.POSITIVE or args.mem_error_type is YarnErrorType.MIXED) and \
                (args.penalty is None or args.ib is None):
            sys.stderr.write("Injecting positive memory errors with no penalty or IB defined will lead to no visible"
                             " changes. Please add a penalty type.")
            sys.exit(-1)
    if args.ib_error is not None:
        if args.ib_error_mode is YarnErrorMode.CONSTANT and \
                args.ib_error_type is YarnErrorType.MIXED:
            sys.stderr.write("CONSTANT IB errors cannot be of the type MIXED.")
            sys.exit(-1)
        if (args.ib_error_type is YarnErrorType.NEGATIVE or args.ib_error_type is YarnErrorType.MIXED) and \
                args.ib_error >= 100:
            sys.stderr.write("Current IB error settings would create containers with negative ideal memory.")
            sys.exit(-1)
    if args.use_gaps and not args.use_reservations:
        sys.stderr.write("WARNING: -g/--use_gaps only makes sense if reservations are used.")


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("sls_trace_file", help="Path to SLS trace file (JSON format).")
    parser.add_argument("network_topology_file", help="Path to topo file (JSON format).")
    parser.add_argument("node_mem_mb", type=int, help="Amount of memory in MB for each node.")
    parser.add_argument("node_cores", type=int, help="Amount of virtual cores for each node.")
    parser.add_argument("node_hb_ms", type=int, help="Period of the NM heartbeats in milliseconds.")
    parser.add_argument("am_hb_ms", type=int, help="Period of the AM heartbeats in milliseconds.")
    parser.add_argument("am_container_mb", type=int, help="Amount of memory in MB for each AM container.")
    parser.add_argument("am_container_cores", type=int, help="Number of cores taken by each AM container.")
    parser.add_argument("scheduler_type", type=get_enumparser(YarnSchedulerType, YarnSchedulerType.SYMBEX),
                        choices=filter(lambda x: x is not YarnSchedulerType.SYMBEX, YarnSchedulerType),
                        help="Type of scheduler to run", default=YarnSchedulerType.REGULAR.name, nargs="?")
    parser.add_argument("output_distinguisher", help="This pharse will appear in all the output file names so that files can be distinguished.") #AJ_code
    parser.add_argument("--scheduling-goal", type=int,
                               default=SCHEDULING_GOALS.DEFAULT.value, help="scheduling can be done with the goal of different metric like average JCT, deadline etc. Please see the SCHEDULING_GOALS class in the utils.")
    parser.add_argument("--all-thin-bypass", type=int,
                               default=0, help="Taken as int. 0 means off 1 means on. When this is set to True all the sorting policies (i.e. excluding Oracle and FIFO) will treat thin jobs same as PATIENCE or SLearn. With this flag on LAS gives always counts jobs score as 0")

    patience_args = parser.add_argument_group("PATIENCE arguments")
    patience_args.add_argument("--thin-limit", type=int,
                              help="Jobs with tasks less then or equal to this will not go through probing.")
    patience_args.add_argument("--sampling-percentage", type=int,
                              help="Percentage of total number of tasks which has to be choosed for sampling.")
    patience_args.add_argument("--oracle", type=int,
                              help="utils.py defines options for it. Non oracle is the default setting.")
    patience_args.add_argument("--all-or-none", type=int,
                              help="utils.py defines options for it. non_evictive will be the default setting.")
    patience_args.add_argument("--eviction-policy", type=int,
                              help="utils.py defines options for it. evictive will be the default setting.")
    patience_args.add_argument("--sampling-node-percentage", type=int,
                              help="Percentage of total number of nodes which will to be designated for sampling. Default value will be same as sampling-percentage.")
    patience_args.add_argument("--thin-node-percentage", type=int,
                              help="Percentage of total number of nodes which will to be designated for thin.")
    patience_args.add_argument("--split-cluster", type=int,
                              help="Tells whether to assume a splitted cluster design or not. Utils.py has different options for it. Default setting will be YES.")
    patience_args.add_argument("--auto-tunable-partition", type=int,
                              help="if set to one then the share of nodes of type sampling, thin and main are determined on the fly.", default=PATIENCE_PARTITION_TUNNING.DEFAULT.value)
    patience_args.add_argument("--common-queue", type=int,
                               default=PATIENCE_COMMON_QUEUE.DEFAULT.value)
    patience_args.add_argument("--conservative-drop", type=int,
                               default=0, help="Use 0 or 1 for False or True. This variable is being used a boolean in the code.")
    patience_args.add_argument("--adaptive-sampling", type=int,
                               default=0, help="See utils.ADAPTIVE_SAMPLING_TYPES")
    three_sigma_args = parser.add_argument_group("ThreeSigma arguments")
    three_sigma_args.add_argument("--threesigma-features-file", type=str, help="This file will have 3Sigma features for warmUp jobs and normal jobs. First line is name of columns", default=None)
    three_sigma_args.add_argument("--threesigma-warmup-file", type=str, help="This file will have 3Sigma features for warmUp jobs only actually warmup ids are needed for this. First line is name of columns", default=None)
    three_sigma_args.add_argument("--threesigma-secondary-warmup-file", type=str, help="This file will have 3Sigma features for warmUp jobs only actually warmup ids are needed for this. First line is name of columns", default=None)
    
    elastic_args = parser.add_argument_group("ELASTIC arguments")
    elastic_args.add_argument("penalty", type=get_enumparser(YarnPenalty), choices=YarnPenalty,
                              help="The performance penalty model to use", nargs="?")
    elastic_args.add_argument("ib", type=float, help="The initial bump of the penalty model", nargs="?")
    elastic_args.add_argument("--ep", type=int,
                              help="Percentage of nodes which are always elastic (works only for GREEDY & SMARTG)")

    peek_args = parser.add_argument_group("PEEK arguments")
    peek_args.add_argument("-p", "--peek-pushback-strategy", type=get_enumparser(PushbackStrategy),
                           choices=PushbackStrategy, nargs="?", help="Strategy to use to avoid pushback in PEEK.",
                           default=None)

    race_args = parser.add_argument_group("RACE arguments")
    from events.yarn.elastic.race import YarnRaceMetric
    race_args.add_argument("--race-metric", type=get_enumparser(YarnRaceMetric), choices=YarnRaceMetric,
                           help="Metric by which to compare REGULAR vs. SMARTG in RACE.",
                           default=YarnRaceMetric.TOTAL_JRT.name)
    race_args.add_argument("--race-lockstep-regular", action="store_true",
                           help="Switch flag to REGULAR until end of simulation.", default=False)
    race_args.add_argument("--race-duration-range", type=parsenumlist,
                           help="Duration range (min, max) in ms of the " +
                                "RACE simulations w.r.t. the simulation timeline.",
                           default=(0, 0))
    race_args.add_argument("--race-never-degrade", action="store_true",
                           help="Always favour REGULAR if any job gets worse running times by using SMARTG.",
                           default=False)
    race_args.add_argument("--race-cutoff-perc", type=int,
                           help="Only simulate a percentage of the remaining containers.",
                           default=0)

    symbex_args = parser.add_argument_group("SYMBEX arguments")
    symbex_args.add_argument("--symbex", action="store_true",
                             help="Run a symbex-type exploration on SMARTG vs REGULAR scheduling decisions.",
                             default=False)
    symbex_args.add_argument("--symbex-mode", type=get_enumparser(SymbexMode), choices=SymbexMode,
                             help="Symbex exploration to run.", default=SymbexMode.DECISION.name)
    symbex_args.add_argument("--symbex-dfs", action="store_true",
                             help="Do a DFS-type exploration rather than a BFS one.",
                             default=False)
    symbex_args.add_argument("--symbex-workers", type=int, help="Number of concurrent symbex worker threads to run.",
                             default=1)

    errinj_args = parser.add_argument_group("Error injection arguments")
    errinj_args.add_argument("--duration-error", type=int,
                             help="Percentage by which to mis-estimate the running times of the containers.")
    errinj_args.add_argument("--duration-error-type", type=get_enumparser(YarnErrorType), choices=YarnErrorType,
                             help="Whether duration error is positive, negative or either.",
                             default=YarnErrorType.MIXED.name)
    errinj_args.add_argument("--duration-error-mode", type=get_enumparser(YarnErrorMode), choices=YarnErrorMode,
                             help="Whether duration error is constant or random.",
                             default=YarnErrorMode.CONSTANT.name)
    el_errinj_args = parser.add_argument_group("ELASTIC Error injection arguments")
    el_errinj_args.add_argument("--duration-error-only-elastic", action="store_true",
                                help="Inject duration error only for ELASTIC containers", default=False)
    el_errinj_args.add_argument("--mem-error", type=int, help="Percentage by which to mis-estimate the ideal memory " +
                                                              "of the containers.")
    el_errinj_args.add_argument("--mem-error-type", type=get_enumparser(YarnErrorType), choices=YarnErrorType,
                                help="Whether the memory misestimation error is positive, negative or either.",
                                default=YarnErrorType.MIXED.name)
    el_errinj_args.add_argument("--mem-error-mode", type=get_enumparser(YarnErrorMode), choices=YarnErrorMode,
                                help="Whether the memory misestimation error is constant or random.",
                                default=YarnErrorMode.CONSTANT.name)
    el_errinj_args.add_argument("--ib-error", type=int, help="Percentage by which to mis-estimate the IB of tasks.")
    el_errinj_args.add_argument("--ib-error-type", type=get_enumparser(YarnErrorType), choices=YarnErrorType,
                                help="Whether IB error is positive, negative or either.",
                                default=YarnErrorType.MIXED.name)
    el_errinj_args.add_argument("--ib-error-mode", type=get_enumparser(YarnErrorMode), choices=YarnErrorMode,
                                help="Whether IB error is constant or random.", default=YarnErrorMode.CONSTANT.name)

    yarn_args = parser.add_argument_group("YARN arguments")
    yarn_args.add_argument("-r", "--use-reservations", action="store_true",
                           help="Whether an app can place a reservation on a node to guarantee its priority there.",
                           default=False)
    yarn_args.add_argument("-g", "--use-gaps", action="store_true",
                           help="Used in conjunction with reservations, allows a container to be scheduled on a " +
                                "reserved node if the duration is less than the reservation duration.", default=False)
    yarn_args.add_argument("--gaps-allow-ams", action="store_true",
                           help="Used in conjunction with reservations and gaps, allows AM containers to be scheduled" +
                                " on a reserved node.", default=False)
    yarn_args.add_argument("--assign-multiple", action="store_true",
                           help="Run the simulation as if the assignMultiple flag is set (multiple assignments per " +
                                "heartbeat are allowed).", default=False)
    yarn_args.add_argument("--dag-scheduling", type=int,
                           help="If scheduling dag or not 0 for no 1 for yes", default=0)

    parser.add_argument("--oracle-debug", action="store_true",
                        help="Display DEBUG messages for all oracle simulations (generates A LOT of output).")
    parser.add_argument("--mem-overestimate-range", type=parsenumlist, help="Range (min, max) of multipliers for " +
                                                                            "memory overestimates by users.",
                        default=(1, 1))
    parser.add_argument("--mem-overestimate-assume", type=float, help="Multiplier by which the system assumes " +
                                                                      "that users overestimate memory.", default=1)
    parser.add_argument("--meganode", action="store_true", help="Pool all node resources together and " +
                                                                "run simulation on this one node.", default=False)
    parser.add_argument("--occupancy-stats-interval", type=int,
                        help="Gather occupancy stats each interval (in seconds).", default=0)
    parser.add_argument("--occupancy-stats-file", type=str, help="File in which to output occupancy stats.",
                        default=None)
    parser.add_argument("-v", "--verbose", action="count")
    parser.add_argument("--vcore-count-source", type=int, help="See class VCORE_COUNT_SOURCE in utils.", default=None)
    parser.add_argument("--all-jobs-arrive-at-begining", type=str, help="This is for an offline version of arrival of all the jobs at same time.", default=False)
    parser.add_argument("--lines-per-job-entry", type=int, help="Number of lines that define a job object in the trace.", default=None)
    parser.add_argument("--job-arrival-time-stretch-multiplier", type=float, help="The scale for stretching the arrival time in the given input trace.", default=1)
    
    testbed_args = parser.add_argument_group("Testbed arguments")
    testbed_args.add_argument("--execution-mode", type=int, help="execution mode", default=YARN_EXECUTION_TYPES.SIMULATION.value)
    testbed_args.add_argument("--coordinator-ip", type=str, help="public ip of the machine running the coordinator", default=tbcmn.COORDINATOR_IP)
    testbed_args.add_argument("--coordinator-listener-port", type=int, help="port at which the coordinator is listening for connections from nodes.", default=tbcmn.COORDINATOR_LISTENER_PORT)

    args = parser.parse_args()

    verify_user_config(args)

    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif args.verbose == 2:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.execution_mode == YARN_EXECUTION_TYPES.SIMULATION.value:
        # Start a new simulator
        simulator = Simulator()
    elif args.execution_mode == YARN_EXECUTION_TYPES.COORDINATOR.value:
        simulator = Coordinator()
    else:
        raise Exception("Unidentified execution type: "+str(args.execution_mode))
    # Create and run the new YarnGenerator
    yarn_generator = YarnGenerator()
    state = yarn_generator.generate_state(simulator, args)

    # Add the event to the simulator
    simulator.add_event(yarn_generator.get_simulation_start_event())

    # Catch SIGINT and drop to a console for debugging purposes.
    signal.signal(signal.SIGINT, signal_handler)

    if args.execution_mode == YARN_EXECUTION_TYPES.COORDINATOR.value:
        simulator.setUpCoordinator(state)
    elif args.execution_mode != YARN_EXECUTION_TYPES.SIMULATOR.value:
        raise Exception("Unidentified execution type: "+str(args.execution_mode))

    # Run the simulation
    if not args.symbex:
        simulator.run()
    else:
        SymbexRunner(state, args.symbex_workers).run()

def tracefunc(frame, event, arg, indent=[0]):
    if event == "call":
        indent[0] += 2
        print "-" * indent[0] + "> call function", frame.f_code.co_name
    elif event == "return":
        print "<" + "-" * indent[0], "exit function", frame.f_code.co_name
        indent[0] -= 2
    return tracefunc

if __name__ == '__main__':
#    sys.settrace(tracefunc)
    main()
    logging.shutdown()
