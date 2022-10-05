# ML-systems-Combining-sampling-based-and-history-based-learning.
### By Joshua Shterenberg, Vinay Metlapalli, Brendan Shortall, Miguel Ianus-Valdivia, and Ashlesha Chaudhari

Vision and Goals Of The Project:

* The final state of the project upon completion would be a toolkit research paper, ideally in conjunction with a working GitHub repository or code base of some kind, detailing our findings. 
The goal of this project is to integrate sampling- and historical-based machine learning with each other to improve the efficiency of job scheduling, building on prior success of sampling-based learning in this task. In general, scheduling jobs for operations on shared clusters with limited resources is challenging. Two approaches have been proposed, history-based learning methods, learning from commonalities of jobs across time to better schedule certain jobs, and sampling-based learning methods, learning from commonalities of various jobs across “space” (their usage characteristics) to better schedule jobs as they enter (all with respect to a given service-level objective). Our project aims to combine these two approaches and determine if the opportunity costs of both approaches in tandem outperforms each model individually.

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

