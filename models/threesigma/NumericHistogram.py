""" Just a mindless copy for easy verification. Not good style or performant. """
## Courtsey following Stackoverflow answer: https://stackoverflow.com/questions/52133285/python-equivalent-fo-hive-numerichistogram#52133807
## This Histogram implementation is similar to following java implementation of NumericHistogram in the Hive project: https://github.com/apache/hive/blob/master/ql/src/java/org/apache/hadoop/hive/ql/udf/generic/NumericHistogram.java
## Akshay has added some code later they are mostly marked as #AJ_code, However, all the code from stack overflow was added in the first commit

import random
from utils import INVALID_VALUE, THREESIGMA_FEATURES, THREESIGMA_METRICS, THREESIGMA_RECENT_AVERAGE_X, NUM_PAST_ESTIMATE_COUNT_NMAE, THREESIGMA_ROLLING_AVERAGE_ALPHA
import numpy as np

class Coord:
    def __init__(self, x, y):
        self.x = float(x)
        self.y = float(y)

    def __repr__(self): # debug
        return "Coord(" + str(self.x) + ", " + str(self.y) + ")"

    def __str__(self): # debug
        return "Coord(" + str(self.x) + ", " + str(self.y) + ")"

class Random:
    """ This class needs fixin. You'll have to do some work here to make it match your version of Java. """
    def __init__(self, seed):
        random.seed(seed)

    def nextDouble(self):
        return random.uniform(0, 1)

class NumericHistogram:
    def __init__(self, ThreeSigmaMetrics = []):
        self.nbins = 0
        self.nusedbins = 0
        self.bins = None

        self.prng = Random(31183) # This should behave the same as Java's RNG for your Java version.
        self.point_estimators = {}
        self.point_estimators_metadata = {}
        self.ThreeSigmaMetrics = ThreeSigmaMetrics
        self.jobs_predicted = {} #  This will have entry for each job for which prediction has been done but job has not yet finished. Key job id and value will be a dictionary having predicted value for each of the metric keyed by metric name
        #   Following two will be used for storing values of NMAE. The error dictionary will have a list for each metric and the observed runtime list will have actual job completion times. The order of entry in each list will be same.
        self.job_prediction_error = {}
        self.job_observed_runtimes = []
        self.NMAEs = {} #key: metric_name value: NMAE for metricName
        for metric_name in self.ThreeSigmaMetrics:
            self.point_estimators[metric_name] = None
            self.point_estimators_metadata[metric_name] = None
            self.job_prediction_error[metric_name] = []
            self.NMAEs[metric_name] = float("inf")
 
    def allocate(self, num_bins):
        self.nbins = num_bins
        self.bins = []
        self.nusedbins = 0

    def add(self, v, job, add_from_creation = False):   #   To be called on job completion or in warm_up phase to make entry for a particular job.
        #if job.job_id == 6393308149:
        #    print "from add going to call update_point_estimators_and_nmae for job_id: ", job.job_id
        self.update_point_estimators_and_nmae(v, job)
        bin = 0
        l = 0
        r = self.nusedbins
        while(l < r):
            bin = (l+r)//2
            if self.bins[bin].x > v:
                r = bin
            else:
                if self.bins[bin].x < v:
                    l = bin + 1; bin += 1
                else:
                    break

        if bin < self.nusedbins and self.bins[bin].x == v:
            self.bins[bin].y += 1
        else:
            newBin = Coord(x=v, y=1)
            if bin == len(self.bins):
                self.bins.append(newBin)
            else:
                self.bins.insert(bin, newBin)

            self.nusedbins += 1
            if (self.nusedbins > self.nbins):
                self.trim()

    def trim(self):
        while self.nusedbins > self.nbins:
            smallestdiff = self.bins[1].x - self.bins[0].x
            smallestdiffloc = 0
            smallestdiffcount = 1
            for i in range(1, self.nusedbins-1):
                diff = self.bins[i+1].x - self.bins[i].x
                if diff < smallestdiff:
                    smallestdiff = diff
                    smallestdiffloc = i
                    smallestdiffcount = 1
                else:
                    smallestdiffcount += 1
                    if diff == smallestdiff and self.prng.nextDouble() <= (1.0/smallestdiffcount):
                        smallestdiffloc = i

            d = self.bins[smallestdiffloc].y + self.bins[smallestdiffloc+1].y
            smallestdiffbin = self.bins[smallestdiffloc]
            smallestdiffbin.x *= smallestdiffbin.y / d
            smallestdiffbin.x += self.bins[smallestdiffloc+1].x / d * self.bins[smallestdiffloc+1].y
            smallestdiffbin.y = d
            self.bins.pop(smallestdiffloc+1)
            self.nusedbins -= 1

    def get_point_estimators_and_nmaes(self, job):   #AJ_code  #Call to this function is treated as a new prediction is made and this will assume if needed that some times a call will be made to add function to mark end of that job.
        runtime_dict_to_return = {}
        nmae_dict_to_return = {}
        self.jobs_predicted[job.job_id] = {}
        for metric_name in self.ThreeSigmaMetrics:
            runtime_dict_to_return[metric_name] = self.point_estimators[metric_name]
            nmae_dict_to_return[metric_name] = self.NMAEs[metric_name]
            self.jobs_predicted[job.job_id][metric_name] = self.point_estimators[metric_name]
        return runtime_dict_to_return, nmae_dict_to_return

    def update_point_estimators_and_nmae(self, v, job):   #AJ_code
        #if job.job_id == 6393308149:
        #    print "======================\n update_point_estimators_and_nmae in NumericHistogram job_id: ", job.job_id, "\n v: ",v 
        if job.job_id in self.jobs_predicted:
            self.job_observed_runtimes.append(v)
            observed_runtime_mean = np.mean(self.job_observed_runtimes)
            for metric_name in self.ThreeSigmaMetrics:
                if self.jobs_predicted[job.job_id][metric_name] != None:
                    self.job_prediction_error[metric_name].append(abs(self.jobs_predicted[job.job_id][metric_name] - v))
                
                if len(self.job_prediction_error[metric_name]) > NUM_PAST_ESTIMATE_COUNT_NMAE:
                    self.job_prediction_error[metric_name] = self.job_prediction_error[metric_name][-1*NUM_PAST_ESTIMATE_COUNT_NMAE:]
                
                self.NMAEs[metric_name] = np.mean(self.job_prediction_error[metric_name])/observed_runtime_mean
        else:
            pass

        #### Earlier following functions were with _and_nmae appended to it ####
        self.update_average_point_estimator(v, job)
        self.update_rolling_average_point_estimator(v, job)
        self.update_recent_average_point_estimator(v, job)
        self.update_median_point_estimator(v, job)
        #self.update_average_point_estimator_and_nmae(v, job)
        #self.update_rolling_average_point_estimator_and_nmae(v, job)
        #self.update_recent_average_point_estimator_and_nmae(v, job)
        #self.update_median_point_estimator_and_nmae(v, job)
 
    def update_average_point_estimator(self, v, job):    #AJ_code
        metric_name = THREESIGMA_METRICS.AVERAGE.value
        if self.point_estimators[metric_name] == None:
            self.point_estimators[metric_name] = v
            self.point_estimators_metadata[metric_name] = 1
        else:
            self.point_estimators[metric_name] = ((self.point_estimators[metric_name]*self.point_estimators_metadata[metric_name]) + v)/(self.point_estimators_metadata[metric_name] + 1)
            self.point_estimators_metadata[metric_name] += 1
 
    def update_rolling_average_point_estimator(self, v, job):    #AJ_code
        metric_name = THREESIGMA_METRICS.ROLLING_AVERAGE.value
        if self.point_estimators[metric_name] == None:
            self.point_estimators[metric_name] = v
        else:
            self.point_estimators[metric_name] = ((self.point_estimators[metric_name]*THREESIGMA_ROLLING_AVERAGE_ALPHA) + (1 - THREESIGMA_ROLLING_AVERAGE_ALPHA)*v)

    def update_recent_average_point_estimator(self, v, job): #AJ_code
        metric_name = THREESIGMA_METRICS.RECENT_AVERAGE.value
        if self.point_estimators[metric_name] == None:
            self.point_estimators[metric_name] = v
            self.point_estimators_metadata[metric_name] = [v] 
        else:
            self.point_estimators_metadata[metric_name].append(v)
            if len(self.point_estimators_metadata[metric_name]) > THREESIGMA_RECENT_AVERAGE_X:
                self.point_estimators_metadata[metric_name] = self.point_estimators_metadata[metric_name][-1*THREESIGMA_RECENT_AVERAGE_X:]
            self.point_estimators[metric_name] = np.mean(self.point_estimators[metric_name])

    def update_median_point_estimator(self, v, job): #AJ_code
        metric_name = THREESIGMA_METRICS.RECENT_MEDIAN.value
        if self.point_estimators[metric_name] == None:
            self.point_estimators[metric_name] = v
            self.point_estimators_metadata[metric_name] = [v]
        else:
            self.point_estimators_metadata[metric_name].append(v)
            if len(self.point_estimators_metadata[metric_name]) > THREESIGMA_RECENT_AVERAGE_X:
                self.point_estimators_metadata[metric_name] = self.point_estimators_metadata[metric_name][-1*THREESIGMA_RECENT_AVERAGE_X:]
            self.point_estimators[metric_name] = np.median(self.point_estimators[metric_name])
    
    def get_quantile(self, q):  #AJ_code
        #@param q The requested quantile, must be strictly within the range (0,1).
        #@return The quantile value.
        sum = 0.0
        csum = 0.0
        for i in range(self.nusedbins):
            sum += self.bins[i].y
        
        for i in range(self.nusedbins):
            csum += self.bins[i].y
            if csum/sum >= q:
                if i == 0:
                    return self.bins[0].x;
                else:
                    csum -= self.bins[i].y;
                    to_return = self.bins[i-1].x + (q*sum - csum)*(self.bins[i].x - self.bins[i-1].x)/(self.bins[i].y);
                    return to_return

        return INVALID_VALUE
        #raise Exception("NumericHistogram.get_quantile(q). Code should not have reached here. q = ", +str(q))

    def get_average(self):  #AJ_code
        total_samples = 0.0
        sum = 0.0
        for i in range(self.nusedbins):
            total_samples += self.bins[i].y
            sum += (self.bins[i].x)*(self.bins[i].y)
 
        if total_samples == 0.0:
            return INVALID_VALUE
        else:
            return sum/total_samples

    def get_moving_average(self, alpha):    #AJ_code
        #@param alpha represents degree of weighting decrease, must be strictly within the range (0,1).
        #@return The moving_average.
        average_to_return = 0
        for i in range(self.nusedbins):
            if i == 0:
                average_to_return = self.bins[i].x
            else:
                average_to_return = alpha*average_to_return + (1 - alpha)*self.bins[i].x
        
        return average_to_return
