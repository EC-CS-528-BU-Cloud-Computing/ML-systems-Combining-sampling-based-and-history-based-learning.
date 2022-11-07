import logging

from events.event import EventResult
from models.yarn.objects import YarnContainerType
from schedulers.yarn import YarnScheduler, LOG


class YarnSRTFScheduler(YarnScheduler):
    def __init__(self, state):
        YarnScheduler.__init__(self, state)
        self.job_queue = []

    def handle_job_arrived(self, job):
        YarnScheduler.handle_job_arrived(self, job)
        self.job_queue.append(job)
        if self.state.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value:
            self.job_queue.sort(key=lambda x: YarnSRTFScheduler.get_three_sigma_score(x, self.state.three_sigma_predictor, self.state.oracle))
        if self.state.oracle == PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value:
            self.assign_sampling_tasks(job)
            self.handle_virtual_sampling_completed(job)
            self.job_queue.sort(key=lambda x: YarnSRTFScheduler.get_sampling_oracle_score(x, self.state.oracle, self.state.thin_limit))

    def handle_job_completed(self, job):
        YarnScheduler.handle_job_completed(self, job)
        if self.state.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value:
            pass   #    Not needed to sort here as sort is happening at task end itself.
            #self.job_queue.sort(key=lambda x: YarnSRTFScheduler.get_three_sigma_score(x, self.state.three_sigma_predictor, self.state.oracle))
 
    def handle_container_finished(self, node, finished_container):
        YarnScheduler.handle_container_finished(self, node, finished_container)
        if self.state.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value:
            pass
            #self.job_queue.sort(key=lambda x: YarnSRTFScheduler.get_three_sigma_score(x, self.state.three_sigma_predictor, self.state.oracle))
        if self.state.oracle == PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value:
            self.job_queue.sort(key=lambda x: YarnSRTFScheduler.get_sampling_oracle_score(x, self.state.oracle, self.state.thin_limit))
 
    @staticmethod
    def get_three_sigma_score(job, predictor, oracle_state):
        runtime = job.get_three_sigma_predicted_runtime()
        if runtime == None:
            predicted_runtime = predictor.predict(job, THREESIGMA_UTILITY_FUNCTIONS.AVERAGE_JCT.value, early_feedback = utils.EARLY_FEEDBACK)
            job.set_three_sigma_predicted_runtime(predicted_runtime)
            job.set_one_time_impact_score(job.get_job_initial_number_of_tasks()*predicted_runtime)
       
        #score = job.get_current_estimated_impact_score(oracle_state)
        score = job.get_one_time_impact_score()

        if score == INVALID_VALUE:
            raise Exception("INVALID_VALUE returned from ThreeSigmaPredictor.predict in YarnSRTFScheduler.get_three_sigma_score")
        return score
 

    @staticmethod
    def compute_job_score(job):
        score = 0
        for task in job.pending_tasks:
            if task.type is YarnContainerType.MRAM:
                continue
            score += task.duration * task.num_containers

        return score

    @staticmethod
    def get_sampling_oracle_score(job, oracle_state, thin_limit):
        if job.get_width("initial_num_tasks") < thin_limit: #   thin_limit
            score = 0
        else:
            score = job.get_job_initial_number_of_tasks()*job.estimates.get_estimated_running_time(oracle_state) # later update it to remaining time. 
        return score

    def get_num_sampling_tasks(self, job):
        if job.get_width("initial_num_tasks") < self.state.thin_limit: #   thin_limit
            return 0
            #return job.get_job_initial_number_of_tasks()
        else:
            return max(1,int((job.get_job_initial_number_of_tasks())*(self.state.sampling_percentage/100))) # You can try setting these variabelas self.state.sampling_percentage instead self.sampling_percentage 

    def assign_sampling_tasks(self, job):
        job.set_num_sampling_tasks(self.get_num_sampling_tasks(job))

    def handle_virtual_sampling_completed(self, job):
        if job.get_width("initial_num_tasks") < self.state.thin_limit: #   thin_limit
            return
        job.estimates.calculate_virtual_estimated_running_time(self.state.oracle)
        
        #print "Patience Job runtime prediction stats for jid: ",str(job.job_id),"," ,job.estimates.get_estimated_running_time(self.state.oracle), ",", job.trace_duration_ms, ",",float(float(abs(job.estimates.get_estimated_running_time(self.state.oracle) - job.trace_duration_ms))/float(job.trace_duration_ms))*100
        job_average_task_length = job.get_average_initial_task_duration_for_all_tasks()
        print "Patience Average task length prediction stats for jid: ",str(job.job_id),"," ,job.estimates.get_estimated_running_time(self.state.oracle), ",", job_average_task_length, ",",float(float(abs(job.estimates.get_estimated_running_time(self.state.oracle) - job_average_task_length))/float(job_average_task_length))*100

        job.set_one_time_impact_score(job.get_job_initial_number_of_tasks()*job.estimates.get_virtual_estimated_running_time(self.state.oracle))

    def schedule(self, node):
        if not self.job_queue:
            return False, (EventResult.CONTINUE,)

        while True:
            if self.state.oracle == PATIENCE_ORACLE_STATE.THREE_SIGMA.value or self.state.oracle == PATIENCE_ORACLE_STATE.SAMPLING_ORACLE.value:
                pass
            else:
                self.job_queue.sort(key=lambda x: YarnSRTFScheduler.compute_job_score(x))

            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("QUEUE: " + ", ".join(map(lambda x: "<" +
                          x.get_name() + " " + str(x.am_launched) + " " +
                          str(x.consumption) + " " + str(YarnSRTFScheduler.compute_job_score(x)) +
                          ">", self.job_queue)))

            queue_idx = 0
            while queue_idx < len(self.job_queue):
                job = self.job_queue[queue_idx]
                if not job.pending.tasks: continue
                task = job.pending_tasks[0]
                self.stats_decisions_inc(job.job_id)
                if not job.am_launched and task.type is not YarnContainerType.MRAM:
                    self.stats_reject_decisions_inc(job.job_id)
                    queue_idx += 1
                    continue
                if task.resource <= node.available:
                    # Adjust task, job and node properties to reflect allocation
                    self.stats_accept_decisions_inc(job.job_id)
                    self.handle_container_allocation(node, task.resource, job, task,
                                                     self.state.simulator.clock_millis)
                    if not job.pending_tasks:
                        # All of the job's containers were processed: remove it from the queue
                        self.job_queue.remove(job)
                    break
                else:
                    queue_idx += 1

            if queue_idx == len(self.job_queue):
                break

        return len(node.allocated_containers) > 0, (EventResult.CONTINUE,)

    def has_pending_jobs(self):
        return bool(self.job_queue)
