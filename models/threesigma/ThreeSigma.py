import sys
import copy
import logging
from functools import total_ordering
from random import Random
import numpy as np
import os
from utils import PEnum
from utils import INVALID_VALUE, PATIENCE_ORACLE_STATE, PATIENCE_ALL_OR_NONE_OPTIONS, PATIENCE_QUEUE_NAMES, PATIENCE_NODE_TYPE, THREESIGMA_FEATURES, THREESIGMA_METRICS, THREESIGMA_RECENT_AVERAGE_X, THREESIGMA_NUM_BINS, THREESIGMA_ROLLING_AVERAGE_ALPHA, THREESIGMA_UTILITY_FUNCTIONS, SCHEDULING_GOALS
from utils import YarnSchedulerType, JOB_RUNTIME_STRETCHER
from models.threesigma.NumericHistogram import NumericHistogram
from models.yarn.objects import YarnJob
import utils

JOB_ID_KEY = "job_id"
JOB_RUNTIME_KEY = "job_runtime"
#USER_NAME_KEY = "username"
#JOB_NAME_KEY = "jobname"
#JOB_LOGICAL_NAME_KEY = "logical_jobname"
RESOURCE_RESQUESTED_SEPARATOR_MULTIPLIER = 100000

class ThreeSigmaPredictor(object):
    def __init__(self, featuresList = None, warmUpFile = None, metricList = None, featuresFile = None, utility_function_name = None, scheduling_goal = None, dag = False):
        self.features = featuresList
        self.metrics = metricList
        self.utility_function_name = utility_function_name
        self.feature_objects = {}    #  Feature name and feature object
        self.estimators = []    #   This list will have tuples of (featureName, metricName). The list will remain sorted in increasing order on basis of NMAE values
        self.NMAEs = {}
        self.completion_time_of_finished_jobs = []  #   This will also include entries for warmup jobs. So, if n jobs in warmup phase then first n entries will be on warmup phase
        self.scheduling_goal = scheduling_goal
        for feature in self.features:
            self.NMAEs[feature] = {}
            for metric in self.metrics:
                self.NMAEs[feature][metric] = None
        self.least_NMAE_estimator = {'feature':None, 'metric':None}    #    feature, metric
        self.least_NMAE_value = float("inf")
        self.job_features_object = ThreeSigmaJobFeaturesObject(inputFile = featuresFile, featuresList = self.features, scheduling_goal = self.scheduling_goal)
        self.generate_feature_objects()
        self.dag = dag
        if self.dag:
            raise Exception("Earlier we were implementing different prediction methods for DAG, however currently that is not in use. For DAG prediction a different method is being used by playing with warmup files only.")
        print "Starting Warmup"
        self.warm_up(warmUpFile, self.utility_function_name, dag = self.dag)
        print "Warmup over"

    def generate_feature_objects(self):
        for feature in self.features:
            #if feature == THREESIGMA_FEATURES.RESOURCE_REQUESTED.value: #   uncomment for cluster B google19
            #    continue   #   uncomment for cluster B google19
            self.feature_objects[feature] = FeatureObject(feature, self.metrics, self.job_features_object, self.scheduling_goal)

    def warm_up(self, warmUpFile, utility_function_name, dag = False):  #   This function feeds the predictor with jobs in the warmup file to initially set the object with some information.
        properties_to_learn = [JOB_RUNTIME_KEY] #   properties in the properties_to_learn list are what the predictor will be predicting.
        if dag:
            properties_to_learn.append(utils.DAG_OTHER_STAGES_RUNTIME)
        warmup_features_object = ThreeSigmaJobFeaturesObject(inputFile = warmUpFile, featuresList = self.features + properties_to_learn, scheduling_goal = self.scheduling_goal, dag = dag)    #   Warmup file will be same as job features file. The warmup object will also be the same with JOB_RUNTIME acting as another feature. Entries for all the jobs in warm up file should also be in the featuresFile passed to ThreeSigmaPredictor while initiating
        job_run_times = warmup_features_object.getFeatureValueArrayByFeatureName(JOB_RUNTIME_KEY)
        if dag:
            dag_other_stages_runtime = warmup_features_object.getFeatureValueArrayByFeatureName(utils.DAG_OTHER_STAGES_RUNTIME)
        for jid in job_run_times:
            temp_job = YarnJob(job_id = jid, ThreeSigmaDummy = True, start_ms = 0, end_ms = int(job_run_times[jid])*utils.JOB_RUNTIME_STRETCHER)
            temp_dag_other_stages_runtime_for_job = []
            if dag:
                temp_dag_other_stages_runtime_for_job = dag_other_stages_runtime[jid]
                for temp_idx, val in enumerate(temp_dag_other_stages_runtime_for_job):
                    temp_dag_other_stages_runtime_for_job[temp_idx] = int(temp_dag_other_stages_runtime_for_job[temp_idx]*utils.JOB_RUNTIME_STRETCHER)
            self.predict(temp_job, utility_function_name, warmUp = True, dag = dag, dag_length = len(temp_dag_other_stages_runtime_for_job))
            self.update_values_on_job_completion(temp_job, warmUp = True, warmup_features_object = warmup_features_object, utility_function_name = utility_function_name, job_completed = True, update_feature_objects = True)
            del temp_job

    def predict(self, job, utility_function_name, warmUp = False, early_feedback = False, dag = False, dag_length = None):
        #   early_feedback is being used for additional warmUp test; i.e jobs being arrived are also being used for warmup instead of their completion.
        estimated_runtime = None
        nmae_corresponding_to_estimate = float("inf")
        representative_histogram = None
        for key in self.feature_objects:
            if utility_function_name == THREESIGMA_UTILITY_FUNCTIONS.POINT_MEDIAN.value:
                #if key != THREESIGMA_FEATURES.RESOURCE_REQUESTED.value:
                #if key != THREESIGMA_FEATURES.APPLICATION_NAME.value and key != THREESIGMA_FEATURES.USER_NAME.value:
                if key != THREESIGMA_FEATURES.USER_NAME.value:
                    continue
            featureObject = self.feature_objects[key]
            estimate, nmae, histogram = featureObject.estimate(job, utility_function_name, early_feedback = early_feedback, dag = dag, dag_length = dag_length)    # Here the assumption is that each featureObject will compare all its metric and return value for metric which has least nmae for that feature value
            #if key == THREESIGMA_FEATURES.USER_NAME.value and job.job_id == 6476018843:
                #print "======================\n feature: ", key," estimate: ", estimate,"\n nmae: ",nmae,"\n histogram: ",histogram
            if nmae < nmae_corresponding_to_estimate:
                if utility_function_name == THREESIGMA_UTILITY_FUNCTIONS.POINT_MEDIAN.value:
                    if key == THREESIGMA_FEATURES.USER_NAME.value and estimated_runtime != None:
                        continue
                estimated_runtime = estimate
                nmae_corresponding_to_estimate = nmae
                representative_histogram = histogram
        estimated_utility = None
        if utility_function_name == THREESIGMA_UTILITY_FUNCTIONS.AVERAGE_JCT.value:
            if representative_histogram == None:
                estimated_utility = INVALID_VALUE
            else:
                estimated_utility = representative_histogram.get_average()
            if estimated_utility == INVALID_VALUE:
                if self.completion_time_of_finished_jobs and warmUp:
                    estimated_utility = np.mean(self.completion_time_of_finished_jobs)
                else:
                    estimated_utility = float("inf")    #As SRTF is sorting in increasing order so lower utility gets privilage
        elif utility_function_name == THREESIGMA_UTILITY_FUNCTIONS.MEDIAN_JCT.value or utility_function_name == THREESIGMA_UTILITY_FUNCTIONS.POINT_MEDIAN.value:
            if representative_histogram == None:
                estimated_utility = INVALID_VALUE
            else:
                estimated_utility = representative_histogram.get_quantile(0.5)
            if estimated_utility == INVALID_VALUE:
                if self.completion_time_of_finished_jobs:
                    estimated_utility = np.median(self.completion_time_of_finished_jobs)
                else:
                    estimated_utility = float("inf")    #As SRTF is sorting in increasing order so lower utility gets privilage
        else:
            estimated_utility = estimated_runtime

        
        #####   Test Below    #####
        if early_feedback:
            self.update_values_on_job_completion(job, update_feature_objects = early_feedback, utility_function_name = utility_function_name, job_completed = not early_feedback)
        #####   Test Above    #####

        return estimated_utility
        #return estimated_runtime

    def update_values_on_job_completion(self, job, warmUp = False, update_feature_objects = False, warmup_features_object = None, utility_function_name = None, job_completed = None):    #   Make sure that update_feature_objects = True exactly in one of the following calls from main scheduler for executing jobs (executing jobs means NON warmup jobs) part either when predict or update_values_on_job_completion  #   Variables job_completed and update_feature_objects were introduced at the time of additional warmup test.
        if job_completed == None:   #   this block was implemented at the time of additional update
            raise Exception("Something wrong in update_values_on_job_completion job_completed")
        if update_feature_objects:
            for feature in self.features:
                #if warmUp and job.job_id ==6393308149:
                #    print "======================\n warming up for job_id: ", job.job_id, "\n feature_name: ", feature
                self.feature_objects[feature].update_values_on_job_completion(job, warmUp = warmUp, warmup_features_object = warmup_features_object, utility_function_name = utility_function_name, early_feedback = not job_completed)
        if job_completed:
            self.completion_time_of_finished_jobs.append(job.prediction_metric_ms(self.scheduling_goal))

class FeatureObject(object):
    def __init__(self, featureName, metrics, job_features_object, scheduling_goal):
        if not self.feature_is_valid(featureName):
            raise Exception("In init of Estimator class: Invalid feature passed: "+str(feature))
        self.feature_name = featureName # Feature names will be described in a Enum derived class in utils
        self.job_features_object = job_features_object
        self.metrics = metrics
        self.scheduling_goal = scheduling_goal
        self.histograms = {}    #   This will have a feature-value and corresponding NumericHistogram objects. Key set in this and self.feature_value_nmae should be consistent.
        self.feature_value_nmae = {}  #   will have a dictionary for each feature value which will have entry for each metric. Key set in this and self.histograms should be consistent.
        self.jobs_predicted = {} #  This will have entry for each job for which prediction has been done but job has not yet finished. Key job id and value will be a dictionary having predicted value for each of the metric keyed by metric name
        self.jobs_feature_value = {} #  This will have entry for each job for which prediction has been done but job has not yet finished. Key job id and value will be the feature-value of the job corresponding to self.featureName 

    def feature_is_valid(self, feature):
        feature_validity = False 
        for f in THREESIGMA_FEATURES:
            if f.value == feature:
                feature_validity = True
                break
        return feature_validity
 
    def get_nmae_value_dict_for_feature_value(self, feature_value):
        pass

    def update_nmae_value_on_job_completion(self, job):
        pass

    def estimate(self, job, utilityFunction, early_feedback = False, dag = False, dag_length = None):  #   Returns estimated_point_runtime for the metric with least NMAE and corresponding nmae value for the feature-value corresponding to the job for self.featureName and the histogram
        job_feature_value = self.job_features_object.getFeatureValueForJobByFeatureName(job.job_id, self.feature_name)
        estimated_runtime = None
        nmae_corresponding_to_estimate = float("inf")
        histogram_to_return = None
        if job_feature_value in self.histograms:
            point_estimators, nmaes_dict = self.histograms[job_feature_value].get_point_estimators_and_nmaes(job)
            for metric_name in point_estimators:
                estimate = point_estimators[metric_name]
                nmae = nmaes_dict[metric_name]
                if nmae < nmae_corresponding_to_estimate:
                    estimated_runtime = estimate
                    nmae_corresponding_to_estimate = nmae
        else:
            self.create_histogram_for_feature_value(job_feature_value, early_feedback = early_feedback)
        histogram_to_return = self.histograms[job_feature_value]
        return estimated_runtime, nmae_corresponding_to_estimate, histogram_to_return

    def update_values_on_job_completion(self, job, warmUp = False, warmup_features_object = None, utility_function_name = None, early_feedback = False):
        if warmUp:
            job_feature_value = warmup_features_object.getFeatureValueForJobByFeatureName(job.job_id, self.feature_name)
        else:
            job_feature_value = self.job_features_object.getFeatureValueForJobByFeatureName(job.job_id, self.feature_name)
 
        if early_feedback:    #   This block is for additional warmup test
            if self.scheduling_goal == SCHEDULING_GOALS.DEADLINE.value:
                v = job.get_max_initial_task_duration_for_all_tasks()
            elif self.scheduling_goal == SCHEDULING_GOALS.AVERAGE_JCT.value:
                v = job.get_average_initial_task_duration_for_all_tasks()
        else:
            v = job.prediction_metric_ms(self.scheduling_goal)
        if job_feature_value in self.histograms:
            self.histograms[job_feature_value].add(v, job)
        else:
            if utility_function_name == THREESIGMA_UTILITY_FUNCTIONS.POINT_MEDIAN.value:
                return 
            #if warmUp:
                #print "Calling create_histogram_for_feature_value from update_values_on_job_completion"
                #self.create_histogram_for_feature_value(job_feature_value, job_objects_to_be_added = [job])
            #else:
                raise Exception("FeatureObject.update_values_on_job_completion: Job "+str(job.job_id)+" finished however a histogram for it's feature-value: "+job_feature_value+" for feature: "+self.feature_name+" doesn't exists.")
 
    def create_histogram_for_feature_value(self, feature_value, job_objects_to_be_added = [], early_feedback = False):   #    initial_values can be passed to be added to the histogram.
        #if feature_value == "njTE8BZMxQTFTmz+xeDNc6MGCjP2WhS6B4xK9+rTh8E=":
        #    print "In featureObject.create_histogram_for_feature_value Creating Hist for featureName:", self.feature_name, "value: ", feature_value
        self.histograms[feature_value] = NumericHistogram(ThreeSigmaMetrics = self.metrics)
        #if feature_value == "75qx40CQ+6iICCPKrIEd0h+9AqJ/V3IISPGjqMWtuIs=":
            #print "Creating Hist for featureName :", self.feature_name, "value: ", feature_value, " at: ",self.histograms[feature_value]
        self.histograms[feature_value].allocate(THREESIGMA_NUM_BINS)
        if job_objects_to_be_added:
            for job in job_objects_to_be_added:
                #if feature_value == "75qx40CQ+6iICCPKrIEd0h+9AqJ/V3IISPGjqMWtuIs=":
                    #print "Creating Hist for featureName:", self.feature_name, "value: ", feature_value, " calling add: ", job.job_id," prediction_metric_ms: ", job.prediction_metric_ms(self.scheduling_goal)
                #if job.job_id == 6394308624:
                #print "in create_histogram_for_feature_value: ", feature_value," job_id", job.job_id, " going to call add"
                if early_feedback:    #   This block is for additional warmup test
                    v = job.get_average_initial_task_duration_for_all_tasks()
                else:
                    v = job.prediction_metric_ms(self.scheduling_goal)
                self.histograms[feature_value].add(v, job, add_from_creation = True)

class ThreeSigmaJobFeaturesObject(object):   #   Create this object with providing an input file which maps job id to features.
	def __init__(self, inputFile = "", featuresList = [], light = False, scheduling_goal = None, dag = False):
	    sys.stderr.write("\n\n Please check what all features are being used in the utils.py\n\n")
            self.baseAddress = os.getcwd()#'/home/ajajoo/Desktop/Research/coflow/coflowsim-master/results/'
	    self.light = light
            self.feature_names = featuresList
            self.job_features = {}  # A dict of dicts. Outer dict is keyed by jids with a dict as value that hass all the features for the job. Inner dict is keyed by feature name and has the correspnding feature_value as the value 
            self.finished_jobs = {}     # this will be a dictionary maintaining actual completion times of finished jobs. Keyed by their ids.
            if inputFile == "":
                self.CodeGen = True
            else:
		with open(self.baseAddress+'/'+inputFile,'r') as f: #   The first line of the file is the column_name
                    file_index_colName_map = {}
                    linesRead = 0
                    for line in f.readlines():
                        temp = line.split(",")
                        for i, colName in enumerate(temp):
                            temp[i] = colName.strip()
                        if linesRead == 0:  #Make header here
                            for i, colName in enumerate(temp):
                                if colName not in self.feature_names and colName != JOB_ID_KEY and colName != JOB_RUNTIME_KEY:
                                        raise Exception("Creating ThreeSigmaJobFeaturesObject mismatch in column_name: "+colName+" not available in feature_name")
                                file_index_colName_map[colName] = i
                        else:
                            job_id = int(temp[file_index_colName_map[JOB_ID_KEY]])
                            self.job_features[job_id] = {}
                            for feature in self.feature_names:
                                temp_feature_val = temp[file_index_colName_map[feature]]
                                #if False:#feature == THREESIGMA_FEATURES.RESOURCE_REQUESTED.value:  #   This False and pass thing is only for Google19 trace clusterB. For other trace please undo it.
                                if feature == THREESIGMA_FEATURES.RESOURCE_REQUESTED.value:  #   This False and pass thing is only for Google19 trace clusterB. For other trace please undo it.
                                    #pass
                                    resources = temp_feature_val.split(":")
                                    if len(resources) == 2:
                                        self.job_features[job_id][feature] = str((int(resources[0])*RESOURCE_RESQUESTED_SEPARATOR_MULTIPLIER)+int(resources[1])) #   Features file has mem:cpu format.
                                    else:
                                        self.job_features[job_id][feature] = resources[0]
                                elif feature == utils.DAG_OTHER_STAGES_RUNTIME:
                                    temp_feature_val = temp_feature_val.split(utils.DAG_STAGE_RUNTIME_WARMUP_SEPARATOR)
                                    for temp_idx, val in enumerate(temp_feature_val):
                                        temp_feature_val[idx] = int(val)
                                    self.job_features[job_id][feature] = list(temp_feature_val)
                                else:
                                    if temp_feature_val.isdigit():
                                        temp_feature_val = int(temp_feature_val)
                                    else:
                                        try:
                                            temp_feature_val = float(temp_feature_val)
                                        except ValueError:
                                            pass
                                    self.job_features[job_id][feature] = temp_feature_val 
                        linesRead += 1

        def getFeatureValueArrayByFeatureName(self, featureName): # returns a dictionary keyed by job_id
	    toReturn = {}
	    for jobId in self.job_features:
                toReturn[jobId] = self.getFeatureValueForJobByFeatureName(jobId, featureName)
	    return toReturn

        def getFeatureValueForJobByFeatureName(self, jobId, featureName): # returns the property value
            #if jobId not in self.job_features:
            try:
                if featureName in self.job_features[jobId]:
	            toReturn = self.job_features[jobId][featureName]
                else:
                   raise Exception("Querying unexpected feature in getFeatureForJobByFeatureName")
            except:
                print "\n\n\n\n\ngetFeatureValueForJobByFeatureName: type(jobId): ", type(jobId), " ", jobId, "featureName: ", featureName, "\n\n\n\n\n"
                raise Exception("\n\n\n\n\ngetFeatureValueForJobByFeatureName: type(jobId): ")#, type(jobId), " ", jobId, "featureName: ", featureName, "\n\n\n\n\n"
                for key in self.job_features:
                    pass
                    #print type(key), key
                #sys.exit(0)
            return toReturn

        def getDictOfAllFeaturesOfAJob(self, jid):  #returns a copy of feature dicts. Any change in returned object will not be reflected in the original object.
            return dict(self.job_features[jid])

        def getListOfAllFeatureValue(self): # returns a dictionary key:job_id, value:listOfAllFeatureValues
	    toReturn = {}
	    for jobId in self.job_features:
                toReturn[jobId] = []
                for featureName in self.feature_names:
                    toReturn[jobId].append(self.getFeatureValueForJobByFeatureName(jobId, featureName))
	    return toReturn

        def getCSVListoFAllFeaturesOfAJob(self, jid):
            all_features_string = ""
            for featureName in self.feature_names:
                all_features_string += str(self.getFeatureValueForJobByFeatureName(jid, featureName))+","
            return all_features_string[:-1]

        def getDictOfAllFeatureValue(self): # returns a dictionary key:job_id, value:DictOfAllFeatureValues(key: feature_name, feature_value)
	    toReturn = {}
	    for jobId in self.job_features:
                toReturn[jobId] = {}
                for featureName in self.feature_names:
                    toReturn[jobId][featureName] = self.getFeatureValueForJobByFeatureName(jobId, featureName)
	    return toReturn
 
        def addJobToFinishedJobs(self, job_id, job_runtime):
            if job_id not in self.job_features:
                raise Exception("No feature entries for the job: "+str(job_id)+ ". However, call to add it to finished jobs list made.")
 
            if job_id in self.finished_jobs:
                raise Exception("Job: "+str(job_id)+ " already exists in finished jobs and its runtime is: "+str(self.finished_jobs[job_id])+". However, call to add it to finished jobs list made again.")

            self.finished_jobs[job_id] = job_runtime
            self.updateRMSOnJobCompletion(job_id)

        def updateNMAEOnJobCompletion(self, job_id):
            pass
 
        def get_runtime_histogram_for_feature(self, feature_name):
            for index, job_ids in enumerate(self.ids_of_finished_jobs):
               pass

        def get_estimated_runtime_histogram_for_job(self, job_id):
            min_rms_feature = None
            min_rms = float("inf")
            for feature in self.feature_names:
                if self.feature_name_rms_value[feature] < min_rms:
                    min_rms_feature = feature
            return self.get_runtime_histogram_for_feature(min_rms_feature)
