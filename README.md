# ML-systems-Combining-sampling-based-and-history-based-learning.                                                                                                       
### By Joshua Shterenberg, Vinay Metlapalli, Brendan Shortall, Miguel Ianus-Valdivia, and Ashlesha Chaudhari                                                           

Vision and Goals Of The Project:                                                                                                                                       

* The final state of the project upon completion would be a toolkit research paper, ideally in conjunction with a working GitHub repository or code base of some kind, detailing our findings. The goal of this project is to integrate sampling- and historical-based machine learning with each other to improve the efficiency of job scheduling, building on prior success of sampling-based learning in this task. In general, scheduling jobs for operations on shared clusters with limited resources is challenging. Two approaches have been proposed, history-based learning methods, learning from commonalities of jobs across time to better schedule certain jobs, and sampling-based learning methods, learning from commonalities of various jobs across “space” (their usage characteristics) to better schedule jobs as they enter (all with respect to a given service-level objective). Our project aims to combine these two approaches and determine if the opportunity costs of both approaches in tandem outperforms each model individually.                                                                                                                                   

Users/Personas Of The Project:                                                                                                                                         

* The ideal users of this project upon completion are those who are actively involved in the optimization of various scheduling tasks for operations on shared clusters with limited resources. More specifically, the user set should involve individuals who have actively employed (or have attempted to actively employ) both historical and sampling based processes for job scheduling and are looking for the most applicable way to combine the two approaches. The users of the project should have prior data on their current processes to compare their performance to the suggestions we describe in the paper.                                                                                                                                                                                                                                                                                                                                                                                                               
Scope and Features Of The Project:                                                                                                                                     

* Given the overarching goals of this project, the specific range of features and functions can best be described by a toolkit research paper in conjunction with a working opensource GitHub repository or code base detailing our findings. The research paper will feature an in depth analysis of a primary drawback of sampling-based learning and how adding a historical-based approach to it results in a better overall solution. This analysis will be supported with code, hosted in a GitHub repository, that demonstrates our solution working with a simulated job scheduler.                                                                                     

* We will not be aiming for a perfectly optimal solution to the problem. Instead we will write up a paper, with the goal of publishing it, clearly defining a significant drawback of sampling-based learning and how it can be improved by implementing a specific feature of historical-based learning. Our goal is to suggest a marginal improvement, not a comprehensive all encompassing solution, to sampling-based learning.                                                                       

Solution Concept                                                                                                                                                       

* Our solution will be heavily focused on one of the four main drawbacks of sampling-based learning. These drawbacks include the necessity and determination of a thin limit, the assumption that tasks within a job have similar characteristics and take similar time to execute, the sampling time for multistage jobs and an increased makespan despite a decreased average completion time.                                                                                                                   

* The thin limit is a threshold for the number of tasks within a job, where jobs with fewer tasks than the thin limit would be inefficient to use sample-based learning. This thin limit is not a fixed value for all sets of jobs. The proposed solution is to use history-based learning to determine the thin limit for a set of jobs.                                                                                                                                                                   

* The second significant drawback of sampling-based learning is the underlying  assumption that all tasks within a job will require similar resources and time to complete. The characteristic of jobs known as skew, the fact that tasks within some jobs are very time-consuming while others not so much, can cause sampling based learning to become less efficient than traditional machine learning techniques. To account for this, history-based learning techniques can be used on jobs with a predicted skew above a certain threshold and the sampling-based technique can be used on jobs with more uniform characteristics.                                       

* The next drawback concludes from a fundamental characteristic of sampling-based learning; That being the time utilized by the algorithm to choose and execute samples. Unfortunately, there is no remedy for this problem. However, the increased efficiency of sampling techniques outweighs the time used for sampling.                                                                                                                                                                                     
* The last drawback focuses on two measurements of job scheduling, the makespan and the average job completion time. The makespan describes the length of time from the start of work to the end. The average job completion time is simply the average time an individual job takes to complete. After running simulations of sampling-based learning it has been discovered that the average job completion time is shortened, while the makespan is increased. The cause of this discrepancy is unknown, it could simply be a bug in the system or it could be an overstepped flaw in the underlying logic. If this is the drawback the team decides to focus on, determining which of these two cases leads to this issue will be first and foremost. From there, debugging or improving the algorithm’s logic to account for this discrepancy will be the next steps.                                                                                                                                                             
* The ideal solution to the overarching problem is one that successfully combines sampling-based and history-based approaches in such a way that it minimizes the overall drawbacks of using either individually. We would like to propose a solution that is better overall (as defined by the metrics of performance, see next section) than those found in existing literature.                                                                                                                               

Acceptance criteria                                                                                                                                                     
* The metrics upon which we chose to evaluate our performance of potential solutions are the same as those outlined in the current research literature. In such papers, analysis of production cluster job traces during runtime and the analysis of the average job completion time, both on normal and “directed acyclic graph” jobs. We would define our chosen solution under the usual criteria of the minimization of the average job completion time, as well as potentially looking at the aggregate resource consumption of the same system of jobs to analyze aggregate efficiency.                                                                                                                                                                                                                                                               

Release Planning:                                                                                                                                                       
* This project is oriented towards the creation of a toolkit paper, and not a final deliverable project that will be pushed to a production phase upon its completion. As such, there will not be a continuous stream of added features and updates in various releases before the finished project. The goal of this project is not to produce a piece of software, but to make a significant contribution to the current literature surrounding methodologies for job scheduling. The first “iteration” of the project will likely be the only state, where we intend to detail an approach that combines sampling-based and history-based machine learning for this task, and implement it on the aforementioned acceptance criteria.

## ORIGINAL README BELOW:

# DSS - The Discrete Scheduler Simulator

DSS is a scheduling simulator written in Python designed to gather statistics about the execution of big-data traces on virtual topologies, mimicking the behaviour of frameworks such as Hadoop. It was initially designed to study the impact of scheduling decisions in YARN on job processing times. It has been used to explore new scheduling strategies and their scalability to very large clusters. It is designed to be fast and fully deterministic. All sources of randomness are controlled to ensure full experiment reproducibility.



DSS takes as input an execution trace in JSON format, which describes the most important characteristics:

- the <u>number of jobs</u> submitted to the cluster

- the <u>arrival time</u> of each job

- the <u>number of tasks</u> per each job

- the <u>resource requirements</u> of each class of tasks (currently only memory and CPU cores supported, tasks implicitly use 1 core each)

- the <u>execution time</u> for each class of tasks

  ​

DSS works by constructing a discrete timeline of events and parsing each event in order. Such events include _job arrivals_, _containers launched_, _containers finished_, _job completion_, _ResourceManager heartbeats_, etc. Each event can impact the remaining timeline. For example, a `JOB_ARRIVAL` event at `t = 5` can indirectly add several `CONTAINER_LAUNCHED` events at `t = 6`, `t = 8` and so forth.

------

<!--ts-->
Table of Contents
=================
   * [DSS - The Discrete Scheduler Simulator](#dss---the-discrete-scheduler-simulator)
      * [Installation](#installation)
         * [Python interpreter](#python-interpreter)
         * [Requirements](#requirements)
            * [Python2](#python2)
            * [PyPy2](#pypy2)
         * [System-wide installation (optional)](#system-wide-installation-optional)
      * [Running DSS](#running-dss)
         * [Quickstart](#quickstart)
         * [DSS Parameters](#dss-parameters)
               * [Positional arguments:](#positional-arguments)
               * [ELASTIC arguments:](#elastic-arguments)
               * [YARN arguments:](#yarn-arguments)
               * [Optional arguments:](#optional-arguments)
               * [PEEK scheduler arguments:](#peek-scheduler-arguments)
               * [Error injection arguments:](#error-injection-arguments)
               * [ELASTIC Error injection arguments:](#elastic-error-injection-arguments)
      * [Advanced usage](#advanced-usage)
         * [Supported schedulers](#supported-schedulers)
               * [REGULAR](#regular)
               * [GREEDY (uses <em>Memory Elasticity</em>)](#greedy-uses-memory-elasticity)
               * [SMARTG (uses <em>Memory Elasticity</em>)](#smartg-uses-memory-elasticity)
               * [PEEK (uses <em>Memory Elasticity</em>)](#peek-uses-memory-elasticity)
               * [SRTF](#srtf)
         * [Memory Elasticity Modelling](#memory-elasticity-modelling)
               * [POWER1, POWER2, SQUAREROOT](#power1-power2-squareroot)
               * [STEP](#step)
               * [SAWTOOTH](#sawtooth)
               * [SPARKWC](#sparkwc)

<!--te-->

------

## Installation

### Python interpreter

DSS was written for Python 2, but for best performance we recommend using [PyPy](http://pypy.org/download.html). Download and unpack in your home folder, and, optionally, place the `pypy` binary in your `$PATH`.

### Requirements

You can use `pip` to install all the dependencies of DSS.

#### Python2

```bash
pip install -r requirements.txt
```

#### PyPy2

```Bash
pypy -m ensurepip
$PYPY_HOME/bin/pip install -U pip wheel # Update pip to the latest version
$PYPY_HOME/bin/pip install -r dss/requirements.txt
```

### System-wide installation (optional)

```bash
python dss/setup.py install
```

------

## Running DSS

### Quickstart

1. Generate a topology file

   ```Bash
   dss/samples/topos/generate_topo.py 100 > topo_100 # Generate a 100 node topology
   ```

2. Generate an input trace file

   - 1000 jobs
   - Job arrival times: _exponential distribution_ [5, 5000] seconds
   - No tasks / job: _uniform distribution_ [1, 500]
   - Memory MB / task: _exponential distribution_ [100MB, 20GB]
   - Task duration: _uniform distribution_ [10, 1000] seconds
   - Random seed: 12345

   ```Bash
   dss/samples/traces/trace-generator.py 1000 5 5000 exp 1 500 unif 1 200 exp 10 1000 unif 12345 > 1000job.trace
   ```

3. Run DSS

   - Simulate a 100 node cluster with 128GB of RAM and 32 cores each
   - Use a 1000-millisecond heartbeat interval between the **NodeManager**s and the **ResourceManager**
   - Use a 1000-millisecond heartbeat interval between the **ApplicationMaster**s and the **ResourceManager**
   - **ApplicationMaster** containers use 1000 MB of RAM and 1 core each
   - Use the **regular** YARN scheduler with node reservations (**-r**)

   ```bash
   dss/dss.py -r ./1000job.trace ./topo_100 128000 32 1000 1000 1000 1 regular
   ```

4. Examine the output files

   - `job_stats.out`

     -  start / end times, job duration
     -  task stats: no tasks, task memory

     ```
     job_0, start_ms=1786000, end_ms=53564000, duration_s=51778.00, tasks = (9400MB x 399)
     job_1, start_ms=260000, end_ms=47652000, duration_s=47392.00, tasks = (6000MB x 361)
     job_2, start_ms=141000, end_ms=52012000, duration_s=51871.00, tasks = (5000MB x 400)
     ...
     ```

   - `task_stats.out`

     -  task type: **ApplicationMaster** (MRAM), MAP, REDUCE
     -  start / end times, task duration
     -  allocated memory (MB)
     -  memory allocation ratio ($allocated MB \over requested MB$) 
     -  node where task ran

     ```
     job_0:1, type=MRAM, start_ms=1787000, end_ms=53563000, duration_s=51776.00, given_memory_mb=1000, alloc_ratio=1.00, node=node22
     job_0:2, type=REDUCE, start_ms=1789000, end_ms=2279000, duration_s=490.00, given_memory_mb=9400, alloc_ratio=1.00, node=node12
     job_0:3, type=REDUCE, start_ms=2281000, end_ms=2771000, duration_s=490.00, given_memory_mb=9400, alloc_ratio=1.00, node=node12
     ...
     ```

   - `decision_stats.out`

     -  breakdown of scheduling decisions for each job
       - TOTAL: no times the scheduler was called for that job
       - ACCEPT: no times the scheduler found a node suitable for that job
       - REJECT: no times the scheduler could not find a node suitable for that job
       - RESERVE: no times the scheduler reserved a node for that job

     ```
     job_0: TOTAL: 1350, ACCEPT: 400(29.63%), REJECT: 586(43.41%), RESERVE: 364(26.96%)
     job_1: TOTAL: 1208, ACCEPT: 362(29.97%), REJECT: 518(42.88%), RESERVE: 328(27.15%)
     job_2: TOTAL: 1177, ACCEPT: 401(34.07%), REJECT: 440(37.38%), RESERVE: 336(28.55%)
     ...
     ```

   - `reservation_stats.out`

     - breakdown of node reservations by each job

     ```
     node1:
     job=job_483, start_ms=38000, end_ms=44000
     job=job_458, start_ms=44000, end_ms=151000
     ...
     node2:
     job=job_861, start_ms=37010, end_ms=38010
     job=job_483, start_ms=38010, end_ms=44010
     ...
     ```



### DSS Parameters

Listed below are several ***supported*** DSS parameters. Any other parameters not listed here are no longer maintained, and as such cannot be guaranteed to function as advertised in the help page.

```bash
usage: dss.py [-h] [-r] [-v]
              [-p [{TIMELINE_CHECK_CURRENT_JOB,PEEK_EACH_ELASTIC}]]
              [--duration-error DURATION_ERROR]
              [--duration-error-type {MIXED,POSITIVE,NEGATIVE}]
              [--duration-error-mode {CONSTANT,RANDOM}]
              [--duration-error-only-elastic][--mem-error MEM_ERROR]
              [--mem-error-type {MIXED,POSITIVE,NEGATIVE}]
              [--mem-error-mode {CONSTANT,RANDOM}][--ib-error IB_ERROR]
              [--ib-error-type {MIXED,POSITIVE,NEGATIVE}]
              [--ib-error-mode {CONSTANT,RANDOM}]
              [--assign-multiple]
              [--mem-overestimate-range MEM_OVERESTIMATE_RANGE]
              [--mem-overestimate-assume MEM_OVERESTIMATE_ASSUME][--meganode]
              [--occupancy-stats-interval OCCUPANCY_STATS_INTERVAL]
              [--occupancy-stats-file OCCUPANCY_STATS_FILE]
              sls_trace_file network_topology_file node_mem_mb node_cores
              node_hb_ms am_hb_ms am_container_mb am_container_cores
                   [{REGULAR,GREEDY,SMARTG,SRTF,PEEK}]
```

##### Positional arguments:

```
 sls_trace_file          Path to SLS trace file (JSON format).
 network_topology_file   Path to topo file (JSON format).
 node_mem_mb             Amount of memory in MB for each node.
 node_cores              Amount of virtual cores for each node.
 node_hb_ms              Period of the NM heartbeats in milliseconds.
 am_hb_ms                Period of the AM heartbeats in milliseconds.
 am_container_mb         Amount of memory in MB for each AM container.
 am_container_cores      Number of cores taken by each AM container.
 {REGULAR,GREEDY,SMARTG,SRTF,PEEK}
                         Type of scheduler to run.
```

##### ELASTIC arguments:

```
{NULL,POWER1,POWER2,SQUAREROOT,STEP,SAWTOOTH,SPARKWC}
                         The performance penalty model to use
  ib                     The initial bump of the penalty model

```

##### YARN arguments:

```
  -r, --use-reservations
                         Whether an app can place a reservation on a node to
                         guarantee its priority there.
  --assign-multiple      Run the simulation as if the assignMultiple flag is
                         set (multiple assignments per heartbeat are allowed).
```

##### Optional arguments:

```
  -h, --help            show this help message and exit
  --meganode            Pool all node resources together and run simulation on
                        this one node.
  --occupancy-stats-interval OCCUPANCY_STATS_INTERVAL
                        Gather occupancy stats each interval (in seconds).
  --occupancy-stats-file OCCUPANCY_STATS_FILE
                        File in which to output occupancy stats.
  -v, --verbose
```



##### PEEK scheduler arguments:

```
  -p [{TIMELINE_CHECK_CURRENT_JOB,PEEK_EACH_ELASTIC}], --peek-pushback-strategy [{TIMELINE_CHECK_CURRENT_JOB,PEEK_EACH_ELASTIC}]
                        Strategy to use to avoid pushback in PEEK.


```

##### Error injection arguments:

```
  --duration-error DURATION_ERROR
                        Percentage by which to mis-estimate the running times
                        of the containers.
  --duration-error-type {MIXED,POSITIVE,NEGATIVE}
                        Whether duration error is positive, negative or
                        either.
  --duration-error-mode {CONSTANT,RANDOM}
                        Whether duration error is constant or random.
```

##### ELASTIC Error injection arguments:

```
  --duration-error-only-elastic
                        Inject duration error only for ELASTIC containers
  --mem-error MEM_ERROR
                        Percentage by which to mis-estimate the ideal memory
                        of the containers.
  --mem-error-type {MIXED,POSITIVE,NEGATIVE}
                        Whether the memory misestimation error is positive,
                        negative or either.
  --mem-error-mode {CONSTANT,RANDOM}
                        Whether the memory misestimation error is constant or
                        random.
  --ib-error IB_ERROR   Percentage by which to mis-estimate the IB of tasks.
  --ib-error-type {MIXED,POSITIVE,NEGATIVE}
                        Whether IB error is positive, negative or either.
  --ib-error-mode {CONSTANT,RANDOM}
                        Whether IB error is constant or random.
```



------

## Advanced usage

### Supported schedulers

##### REGULAR

The standard (vanilla) YARN scheduler. Will assign a task to a node if the node has sufficient resources (RAM and CPU cores) at the time of assignment.

<u>Node reservations</u>

Using the **-r** argument, the YARN scheduler will reserve a node for the job at the head of the scheduling queue if the node does not have sufficient resources.

<u>Assign multiple</u>

Using the **—assign-multiple** argument, the YARN scheduler will try to assign more than the standard *1 task / node heartbeat*.

##### GREEDY (uses *Memory Elasticity*)

Based on the YARN scheduler, but will assign a task to a node even if less memory (RAM) than required is available. It still imposes a minimum **10%** of the task required memory.

Tasks allocated with less memory than required can have a different duration based on their properties (see below). For example, a task that normally uses 2GB of RAM and runs in 10 minutes, given 1.5GB of RAM might run 15 minutes.

##### SMARTG (uses *Memory Elasticity*)

Similar to **GREEDY**, but assigns memory such that the execution time of the container is minimised. Depending on the memory elasticity model of each task, the correlation between allocated memory and task execution time may not be a linear function. Therefore, slightly counter-intuitively, giving less memory can sometimes slightly _improve_ execution time.

##### PEEK (uses *Memory Elasticity*)

Uses same allocation strategy as **SMARTG**, but allocates a task with fewer resources _iff_ this decision reduces overall **job completion time**.

##### SRTF

Implements a standard **Shortest-Remaining-Time-First** scheduler, which prioritises jobs based on an estimation of the total remaining duration of their respective containers.

### Memory Elasticity Modelling

##### POWER1, POWER2, SQUAREROOT

$alloc\_ratio = {required\_mem \over allocated\_mem}$

$degradation\_factor = (alloc\_ratio)^n$, where $n$ is 1, 2, or $1 \over 2$.

##### STEP

<u>Initial Bump</u>

The maximum degradation factor expected to occur for a task if it is allocated less memory than required. Is expressed as a floating point value.

Example: A task that requires 2GB to run, and takes 10 minutes to complete, with an `IB` value of **2.1x** will take at most 21 minutes to complete if given <2GB.

$degradation\_factor = IB$, if $allocated\_mem < required\_mem$.

##### SAWTOOTH

Memory elasticity model based on **STEP** that captures the spilling behaviour of <u>Hadoop MapReduce</u> containers, particularly of **REDUCE** tasks. Requires defining the `IB` value.

##### SPARKWC

Memory elasticity model based on **STEP** that captures the spilling behaviour of <u>Spark WordCount</u> containers. Requires defining the `IB` value.
