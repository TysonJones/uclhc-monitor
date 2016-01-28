#!/usr/bin/env python

# Author:       Tyson Jones, January 2016 (MURPA student of Prof Frank Wuerthwein, UCSD).
#               Feel free to contact me at  tjon14@student.monash.edu

# Purpose:      To be periodically run (by CRON) on each submit site.
#               Queries condor_q and condor_history (since last arbitrary run time), collates
#               any (custom formatted) job/classad data into time bins (default, 6 mins) and
#               pushes these to the front-end (influxDB).

# TODO: if database doesn't exist, create it
# TODO: limit the size of outbox
# TODO: data retention

import sys; sys.dont_write_bytecode = True

#import classad
import time
import json
import os




#PRINT_DEBUGS = True


class SpecialClassAds:
    """
    the condor labels of important job classad fields. These are needed by the
    daemon even if not explicitly mentioned in the user defined custom metrics.
    Jobs may not have all these fields, but they are attempted collection.
    """
    #TYPE = "MyType" # = "Job"
    JOB_ID = "GlobalJobId"
    JOB_START_DATE = "JobStartDate"
    JOB_STATUS = "JobStatus"
    LAST_JOB_STATUS = "LastJobStatus"
    SERVER_TIME = "ServerTime"
    ENTERED_STATUS_TIME = "EnteredCurrentStatus"

    REQUIRED = [JOB_ID,
                JOB_START_DATE,
                JOB_STATUS,
                LAST_JOB_STATUS,
                SERVER_TIME,
                ENTERED_STATUS_TIME]



class Filenames:
    """the filenames of all auxillary files used by the main Daemon"""
    CONFIG = "config"
    LAST_BIN_TIME = "last_bin_time"
    OUTBOX = "outbox"
    LOG = "log"
    INIT_JOB_FIELDS = "initial_running_job_fields"
    VALUE_CACHE = "classad_value_cache"


class JobStatus:
    """possible statuses of a condor job (at any time) and their Condor integer codes"""
    IDLE, RUNNING, REMOVED, COMPLETED, HELD, TRANSFERRING_OUTPUT = range(1, 7)



# TODO: overhaul aggregation ops vs type. Some should be type specific?
class ConfigFields:
    """labels of the fields in the configuration file"""
    BIN_DURATION = "bin duration"
    OUTBOX_DATA_LIMIT = "outbox data limit"

METRICS = "metrics"
class MetricsFields:
    """labels of the fields common to all metric types"""
    DESCRIPTION = "description"
    DATABASE_NAME = "database name"
    MEASUREMENT_NAME = "measurement name"
    GROUP_FIELDS = "group by ClassAd fields"

    JOB_STATUS = "job status"
    class JobStatuses:
        """labels of the possible statuses of a job, which may be filtered out of a metric's gaze"""
        IDLE = "IDLE"
        RUNNING = "RUNNING"
        REMOVED = "REMOVED"
        COMPLETED = "COMPLETED"
        HELD = "HELD"
        TRANSFERRING_OUTPUT = "TRANSFERRING OUTPUT"

        RAN_THEN_REMOVED = "RAN THEN REMOVED"
        IDLE_THEN_REMOVED = "IDLE THEN REMOVED"
        RUNNING_OR_RAN = "RUNNING OR RAN"

    AGGREGATE_OP = "aggregation operation"
    class AggregationOps:
        """labels of the operations of aggregation"""
        SUM = "SUM"
        LAST = "LAST"
        FIRST = "FIRST"
        AVERAGE = "AVERAGE"

    METRIC_TYPE = "metric type"
    class MetricTypes:
        """labels of the types a metric can be"""
        RAW = "RAW"
        COUNTER = "COUNTER"
        DIFFERENCE = "DIFFERENCE"

class RawMetricFields:
    VALUE_CLASSAD_FIELD = "value ClassAd field"

class CounterMetricFields:
    pass

class DifferenceMetricFields:
    VALUE_CLASSAD_FIELD = "value ClassAd field"


def get_last_bin_time():
    """returns the time (seconds since epoch) of the end of the final bin in the previous push."""
    f = open(Filenames.LAST_BIN_TIME, 'r')
    t = f.read()
    f.close()
    return long(t)


def write_last_bin_time(t):
    """writes the time (seconds since epoch) to the last bin time cache"""
    f = open(Filenames.LAST_BIN_TIME, 'w')
    f.write(t)
    f.close()


def get_config():
    """returns a dict of config settings."""
    f = open(Filenames.CONFIG, 'r')
    d = json.load(f)
    f.close()
    return d


def get_outbox():
    """returns the contents of the outbox."""
    f = open(Filenames.OUTBOX, 'r')
    d = json.load(f)
    f.close()
    return d


def add_to_outbox(jobs):
    """adds a list of failed (to push) jobs to the outbox"""
    f = open(Filenames.OUTBOX, 'r+')
    j = json.load(f)
    f.seek(0)
    f.truncate()
    json.dump(j + jobs, f)
    f.close()


def add_to_log(message):
    """appends a string message to the log"""
    f = open(Filenames.LOG, "a")
    f.write("ERROR at %s:\n%s\n_____________" % (time.ctime(), message))
    f.close()


# debug
def spoof_config_metrics():
    conf = {
        ConfigFields.BIN_DURATION: 1,
        ConfigFields.OUTBOX_DATA_LIMIT: 1000,
        METRICS: [
            {
                MetricsFields.DATABASE_NAME: "RunningJobs",
                MetricsFields.MEASUREMENT_NAME: "num_jobs",
                MetricsFields.METRIC_TYPE: MetricsFields.MetricTypes.COUNTER,
                MetricsFields.GROUP_FIELDS: ["Owner"],
                MetricsFields.AGGREGATE_OP: MetricsFields.AggregationOps.LAST,
                MetricsFields.JOB_STATUS: [MetricsFields.JobStatuses.RUNNING_OR_RAN],
                MetricsFields.DESCRIPTION: ("The number of running jobs (by each owner) " +
                                                         "at the end of each bin")
            },

            {
                MetricsFields.DATABASE_NAME: "RunningJobs",
                MetricsFields.MEASUREMENT_NAME: "cpu_time",
                MetricsFields.METRIC_TYPE: MetricsFields.MetricTypes.DIFFERENCE,
                DifferenceMetricFields.VALUE_CLASSAD_FIELD: "RemoteUserCpu",
                MetricsFields.GROUP_FIELDS: ["Owner", "MATCH_EXP_JOB_Site"],
                MetricsFields.AGGREGATE_OP: MetricsFields.AggregationOps.SUM,
                MetricsFields.JOB_STATUS: [MetricsFields.JobStatuses.RUNNING_OR_RAN],
                MetricsFields.DESCRIPTION: "The CPU time used by each owner on each job site per bin."
            }
        ]
    }

    f = open(Filenames.CONFIG, "w")
    json.dump(conf, f, indent=4, sort_keys=False)
    f.close()


def get_relevant_jobs_for_metrics(metrics):



    # figure out what JobStatus we are after
    for metric in metrics:
        pass


    # figure out what fields we want

















'''
def get_fields_from_ad(ad, fields):
    """
    returns a dict of field to the value in the passed ad, for all fields in field.
    If a field is not in fields, false is returned and an error is logged.

    arguments:
        ad      -- the classad from which to extract the designated fields
        fields  -- the fields to collect from the ad
    returns:
        {}      -- a dictionary of the selected fields from ad if all fields are in ad
        False   -- if any field in fields is not in the ad. In this case, an error is logged.
    """
    job = {}
    for field in fields:
        if field not in ad:
            add_to_log('Requested field "%s" not in ad:\n%s' % (field, str(ad)))
            return False
        job[field] = ad[field]
    return job


def get_job_fields_running_since(t, fields):
    """
    returns a list of classad fields for all jobs which have either started since t, stopped since t (or both)
     or were running before t and have not yet t. Simply, it is all jobs which at any time, were running
    between t and now. This is exactly all jobs which are either currently running or have finished since t.
    Note idle jobs are not returned, nor are jobs which were terminated when idle (never ever ran).
    Jobs which ran and then were terminated before completion are returned as if completed when terminated.
    If a field in fields is not in any ad for a job since t, that particular job is dropped (and an error is logged)

    arguments:
        t       -- time in nanoseconds since epoch, after which jobs having run are sought
        fields  -- list of classad fields to return for each job
    returns:
        [ {}, {}, ...]  -- an array of dictionaries of classad keys to their job values, specified in fields
    """
    jobs = []

    # grab currently running jobs (which aren't idle)
    const = "JobStatus =!= 1"                                  # TODO: we may want idle
    fd = os.popen("condor_q -l -constraint '%s' " % const)
    ads = classad.parseOldAds(fd)

    # record required fields    (py 2.7 one liner: {f:ad[f] for f in fields})
    for ad in ads:
        job = get_fields_from_ad(ad, fields)
        if job:
            jobs.append(job)

    # grab jobs which have ended since t, and weren't terminated when idle
    const = "EnteredCurrentStatus > %d && LastJobStatus =!= 1" % t             # TODO: we may want idle
    fd = os.popen("condor_history -l -constraint '%s' " % const)
    ads = classad.parseOldAds(fd)

    # record required fields
    for ad in ads:
        job = get_fields_from_ad(ad, fields)
        if job:
            jobs.append(job)

    return jobs

'''



'''
#check_and_fix_files()

# TODO push outbox

# TODO get server time. How should I do it?
# maybe the Condor python bindings will make this more elegant?


config = get_config()
bin_duration = config[ConfigFields.BIN_DURATION]
initial_time = get_last_bin_time()
current_time = int(time.time())                    # TODO: use server time instead




# read the custom metrics from the config file
metrics = config[ConfigFields.METRICS]
for metric in metrics:
    print metric




# determine all required condor fields



# get the required fields of all relevant jobs



# decide on time bins



# iterate bins and populate custom metrics
'''