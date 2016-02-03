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

import htcondor
import urllib2
import time
import json


PRINT_DEBUGS = True


class Filenames:
    """the filenames of all auxillary files used by the main Daemon"""
    CONFIG = "config.json"
    LAST_BIN_TIME = "last_bin_time"
    OUTBOX = "outbox.json"
    LOG = "log.json"
    INIT_JOB_FIELDS = "initial_running_job_vals.json"
    VALUE_CACHE = "prev_value_cache.json"


class Labels:
    """the placeholder or defaulted strings to display to the user"""

    # the placeholder for a metric group val to use as an influxDB tag, if a job didn't have a value
    METRIC_GROUP_UNKNOWN_VALUE = "unknown"


class InfluxPatterns:
    """string patterns used by InfluxDB in its HTTP responses"""
    INEXISTANT_DATABASE = "database not found"


class CondorDudValues:
    """the values of Condor fields when Condor reports them incorrectly"""

    JOB_SITE_WHEN_ON_BRICK = ["Unknown", "$$(GLIDEIN_Site:Unknown)", "$$(JOB_Site:unknown)"]


def DEBUG_PRINT(msg, obj=''):
    """prints a debug message and object, only if the debug print flag is true"""
    if PRINT_DEBUGS:
        print msg, obj


class CondorOperators:
    """
    list of operator symbols recognised by Condor for query constraints
    """
    OR          = "||"
    AND         = "&&"
    EQUALS      = "=?="
    NOT_EQUALS  = "=!="
    LESS_THAN   = "<"
    GREATER_THAN= ">"


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
    ENQUEUE_TIME = "QDate"
    JOB_SITE = "MATCH_EXP_JOB_Site"
    SUBMIT_SITE = "SUBMIT_SITE"

    #TODO: the Glide_In_Job_Site field must default to submit for local jobs

    #the fields which are (attemptedly) kept for every job, despite those specifically needed by metrics
    REQUIRED = set([JOB_ID,
                    JOB_START_DATE,
                    JOB_STATUS,
                    LAST_JOB_STATUS,
                    SERVER_TIME,
                    ENTERED_STATUS_TIME,
                    ENQUEUE_TIME,
                    JOB_SITE,
                    SUBMIT_SITE])


class JobStatus:
    """possible statuses of a condor job (at any time) and their Condor integer codes"""
    IDLE, RUNNING, REMOVED, COMPLETED, HELD, TRANSFERRING_OUTPUT = range(1, 7)

    ONGOING = [IDLE, RUNNING, HELD, TRANSFERRING_OUTPUT]
    DONE    = [REMOVED, COMPLETED]


# TODO: overhaul aggregation ops vs type. Some should be type specific?
class ConfigFields:
    """labels of the fields in the configuration file"""
    BIN_DURATION = "bin duration"
    OUTBOX_DATA_LIMIT = "outbox data limit"
    DATABASE_DOMAIN = "database domain"
    METRICS = "metrics"


class MetricsFields:
    """labels of the fields common to all metric types"""
    DESCRIPTION = "description"
    DATABASE_NAME = "database name"
    MEASUREMENT_NAME = "measurement name"
    GROUP_FIELDS = "group by ClassAd fields"

    JOB_STATUS = "job statuses"
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

        ALL = "ALL"

    AGGREGATE_OP = "aggregation operation"
    METRIC_TYPE = "metric type"
    class MetricTypes:
        """labels of the types a metric can be"""
        RAW = "RAW"
        COUNTER = "COUNTER"
        CHANGE = "CHANGE"

class RawMetricFields:
    VALUE_CLASSAD_FIELD = "value ClassAd field"

    class AggregationOps:
        SUM = "SUM"
        AVERAGE = "AVERAGE"
        WEIGHTED_AVERAGE = "WEIGHTED AVERAGE"

class ChangeMetricFields:
    VALUE_CLASSAD_FIELD = "value ClassAd field"

    class AggregationOps:
        SUM = "SUM"
        AVERAGE = "AVERAGE"
        WEIGHTED_AVERAGE = "WEIGHTED AVERAGE"

class CounterMetricFields:

    class AggregationOps:
        ALL = "ALL"
        INITIAL = "INITIAL"
        FINAL = "FINAL"
        WEIGHTED_AVERAGE = "WEIGHTED AVERAGE"


def json_load_as_ascii(file_handle):
    return convert_to_ascii(
        json.load(file_handle, object_hook=convert_to_ascii),
        ignore_dicts=True)


def convert_to_ascii(data, ignore_dicts = False):
    if isinstance(data, unicode):
        return data.encode('utf-8')
    if isinstance(data, list):
        return [convert_to_ascii(item, ignore_dicts=True) for item in data ]
    if isinstance(data, dict) and not ignore_dicts:
        return dict([
            (convert_to_ascii(key, ignore_dicts=True), convert_to_ascii(value, ignore_dicts=True))
            for key, value in data.iteritems()])
    return data


def load_last_bin_time():
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


def load_prev_val_cache():
    """
    loads the cache containing previous job Classad values

    returns:
        {}  -  the cache of values:{GlobalJobId: {field: [val, time], ...}, ... }
    """
    f = open(Filenames.VALUE_CACHE, 'r')
    j = json_load_as_ascii(f)
    f.close()
    return j


def load_job_init_vals():
    """returns a dictionary of the initial values of job classad fields set when a job first runs"""
    f = open(Filenames.INIT_JOB_FIELDS, 'r')
    j = json_load_as_ascii(f)
    f.close()
    return j


def load_config():
    """returns a dict of config settings."""
    f = open(Filenames.CONFIG, 'r')
    j = json_load_as_ascii(f)
    f.close()
    return j


def load_outbox():
    """returns the contents of the outbox."""
    f = open(Filenames.OUTBOX, 'r')
    j = json_load_as_ascii(f)
    f.close()
    return j


def add_to_outbox(jobs):
    """adds a list of failed (to push) jobs to the outbox"""
    f = open(Filenames.OUTBOX, 'r+')
    j = json_load_as_ascii(f)
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
        ConfigFields.BIN_DURATION: 100,
        ConfigFields.OUTBOX_DATA_LIMIT: 1000,
        ConfigFields.DATABASE_DOMAIN: "http://test-003.t2.ucsd.edu:8086",
        ConfigFields.METRICS: [
            # {
            #     MetricsFields.DATABASE_NAME: "RunningJobs",
            #     MetricsFields.MEASUREMENT_NAME: "num_jobs",
            #     MetricsFields.METRIC_TYPE: MetricsFields.MetricTypes.COUNTER,
            #     MetricsFields.GROUP_FIELDS: ["Owner"],
            #     MetricsFields.AGGREGATE_OP: MetricsFields.AggregationOps.LAST,
            #     MetricsFields.JOB_STATUS: [MetricsFields.JobStatuses.RUNNING_OR_RAN],
            #     MetricsFields.DESCRIPTION: ("The number of running jobs (by each owner) " +
            #                                              "at the end of each bin")
            # },
            #
             {
                 MetricsFields.DATABASE_NAME: "MetricTest",
                 MetricsFields.MEASUREMENT_NAME: "cpu_time",
                 MetricsFields.METRIC_TYPE: MetricsFields.MetricTypes.CHANGE,
                 ChangeMetricFields.VALUE_CLASSAD_FIELD: "RemoteUserCpu",
                 MetricsFields.GROUP_FIELDS: ["Owner", "MATCH_EXP_JOB_Site"],
                 MetricsFields.AGGREGATE_OP: ChangeMetricFields.AggregationOps.SUM,
                 MetricsFields.JOB_STATUS: [MetricsFields.JobStatuses.RUNNING_OR_RAN],
                 MetricsFields.DESCRIPTION: "The CPU time used by each owner on each job site per bin."
             },
            # {
            #     MetricsFields.DATABASE_NAME: "MetricTest",
            #     MetricsFields.MEASUREMENT_NAME: "num_idle",
            #     MetricsFields.METRIC_TYPE: MetricsFields.MetricTypes.COUNTER,
            #     MetricsFields.GROUP_FIELDS: ["Owner"],
            #     MetricsFields.AGGREGATE_OP: CounterMetricFields.AggregationOps.FINAL,
            #     MetricsFields.JOB_STATUS: [MetricsFields.JobStatuses.IDLE,
            #                                MetricsFields.JobStatuses.IDLE_THEN_REMOVED],
            #     MetricsFields.DESCRIPTION: "Number of idle jobs per user at the end of each time bin, which remain running now"
            # },
            # {
            #     MetricsFields.DATABASE_NAME: "MetricTest",
            #     MetricsFields.MEASUREMENT_NAME: "num_total_jobs",
            #     MetricsFields.METRIC_TYPE: MetricsFields.MetricTypes.COUNTER,
            #     MetricsFields.GROUP_FIELDS: ["SUBMIT_SITE"],
            #     MetricsFields.AGGREGATE_OP: CounterMetricFields.AggregationOps.ALL,
            #     MetricsFields.JOB_STATUS: []
            # }
        ]
    }

    f = open(Filenames.CONFIG, "w")
    json.dump(conf, f, indent=4, sort_keys=False)
    f.close()


'''
def get_running_condor_job_ads(constraint):
    """
    returns classads of all currently running jobs which satisfy the constraint
s
    arguments:
        constraint   --  a condor formatted constraint, restricting which jobs are returned by the condor binaries
    """
    cmd = "condor_q -l -const '%s' " % constraint
    DEBUG_PRINT("Calling a Condor binary: ", cmd)
    #ads = classad.parseOldAds(os.popen(cmd))
    ads = classad.parseAds(os.popen(cmd).read())             # TODO: can't iterate this shit!
    return ads
'''


'''
def get_old_condor_job_ads_since(constraint, since_time):
    """
    returns classads of all jobs in condor history which satisfy the constraint, AND entered their current state
    since since_time

    arguments:
        since_time   --  the time (seconds since epoch), after which to grab jobs satisfying the constraint
        constraint   --  a condor formatted constraint, restricting which jobs are returned by the condor binaries
    """
    limit = SpecialClassAds.ENTERED_STATUS_TIME + ' ' + CondorOperators.GREATER_THAN + ' ' + str(since_time)
    cmd = "condor_history -l -const '(%s %s %s)' " % (constraint, CondorOperators.AND, limit)
    DEBUG_PRINT("Calling a Condor binary: ", cmd)
    #ads = classad.parseOldAds(os.popen(cmd))
    ads = classad.parseAds(os.popen(cmd).read())            # TODO: can't iterate this shit!
    return ads
'''


# unneeded since integrating condor bindings
'''
def get_stripped_classad(classad, fields):
    """
    returns a dict of only the classad fields in the passed list fields (with their classad values).
    If a field in fields isn't in the classad, it is skipped. I.e. the returned structure does not necessarily
    contain every field in fields, but it is gauranteed not to contain any field not in fields.
    However, if the job is currently idle and has never run, it has no LastJobStatus: this is
    manually added to be None (is referred to in other code but the value should never used).
    Also sets the current time of the script to be the classads server time, in ContextData
    """
    if SpecialClassAds.SERVER_TIME in classad:
        ContextData.current_time = classad[SpecialClassAds.SERVER_TIME]

    stripped = {}
    for field in fields:
        # for some reason, some classad fields are strings and hate unicode
        field = str(field)
        if field in classad:
            stripped[field] = classad[field]

    # add a dud LastJobStatus to idle jobs which never ran
    if SpecialClassAds.LAST_JOB_STATUS not in stripped:
        stripped[SpecialClassAds.LAST_JOB_STATUS] = None

    return stripped
'''


def get_relevant_jobs_and_fields_for_metrics(metrics, since_time):
    """
    Collect only relevant job ClassAds from condor. A job is deemed relevant if
    it is in a state (JobStatus) as required by a metric and if it entered its
    current state since the given time. A ClassAd in a job is deemed relevant if
    required by any metric (includes our list of required ads for every job).

    arguments:
        metrics    -- a list of metrics (in config file format)
        since+time -- the earliest time (seconds since epoch) of job entering state
    returns
        [{},...]   -- a list of dicts, representing jobs, populated with required ads
    """
    "figure out what JobStatus and ClassAd fields we are after"
    required_status = {
        MetricsFields.JobStatuses.IDLE:         False,
        MetricsFields.JobStatuses.RUNNING:      False,
        MetricsFields.JobStatuses.REMOVED:      False,
        MetricsFields.JobStatuses.COMPLETED:    False,
        MetricsFields.JobStatuses.HELD:         False,
        MetricsFields.JobStatuses.ALL:          False,
        MetricsFields.JobStatuses.RAN_THEN_REMOVED:     False,
        MetricsFields.JobStatuses.IDLE_THEN_REMOVED:    False,
        MetricsFields.JobStatuses.RUNNING_OR_RAN:       False,
        MetricsFields.JobStatuses.TRANSFERRING_OUTPUT:  False
    }
    required_fields = set().union(SpecialClassAds.REQUIRED)
    for metric in metrics:
        # mark that a JobStatus is needed
        for status in metric[MetricsFields.JOB_STATUS]:
            required_status[status] = True

        # mark that a ClassAd field is needed (groups and main val)
        for field in metric[MetricsFields.GROUP_FIELDS]:
            required_fields.add(field)
        if metric[MetricsFields.METRIC_TYPE] == MetricsFields.MetricTypes.RAW:
            required_fields.add(metric[RawMetricFields.VALUE_CLASSAD_FIELD])
        if metric[MetricsFields.METRIC_TYPE] == MetricsFields.MetricTypes.CHANGE:
            required_fields.add(metric[ChangeMetricFields.VALUE_CLASSAD_FIELD])

    "build condor query JobStatus constraints"
    # constraints which are to be OR'd
    consts = []

    # whether or not a call to each binary is required
    condor_q, condor_history = False, False

    # convenience variables
    STATUS = SpecialClassAds.JOB_STATUS
    LAST_STATUS = SpecialClassAds.LAST_JOB_STATUS
    EQUALS = ' ' + CondorOperators.EQUALS + ' '
    AND = ' ' + CondorOperators.AND + ' '
    OR = ' ' + CondorOperators.OR + ' '

    # build constraint constituents based on required statuses (some are compound)
    if required_status[MetricsFields.JobStatuses.ALL]:
        consts.append("true")
        condor_q = True
        condor_history = True
    if required_status[MetricsFields.JobStatuses.IDLE]:
        consts.append(STATUS + EQUALS + str(JobStatus.IDLE))
        condor_q = True
    if required_status[MetricsFields.JobStatuses.RUNNING]:
        consts.append(STATUS + EQUALS + str(JobStatus.RUNNING))
        condor_q = True
    if required_status[MetricsFields.JobStatuses.REMOVED]:
        consts.append(STATUS + EQUALS + str(JobStatus.REMOVED))
        condor_history = True
    if required_status[MetricsFields.JobStatuses.COMPLETED]:
        consts.append(STATUS + EQUALS + str(JobStatus.COMPLETED))
        condor_history = True
    if required_status[MetricsFields.JobStatuses.HELD]:
        consts.append(STATUS + EQUALS + str(JobStatus.HELD))
        condor_q = True
    if required_status[MetricsFields.JobStatuses.TRANSFERRING_OUTPUT]:
        consts.append(STATUS + EQUALS + str(JobStatus.TRANSFERRING_OUTPUT))
        condor_q = True
    if required_status[MetricsFields.JobStatuses.RAN_THEN_REMOVED]:
        consts.append(
                 STATUS + EQUALS + str(JobStatus.REMOVED) + AND +
                 LAST_STATUS + EQUALS + str(JobStatus.RUNNING))
        condor_history = True
    if required_status[MetricsFields.JobStatuses.IDLE_THEN_REMOVED]:
        consts.append(
                STATUS + EQUALS + str(JobStatus.REMOVED) + AND +
                LAST_STATUS + EQUALS + str(JobStatus.IDLE))
        condor_history = True
    if required_status[MetricsFields.JobStatuses.RUNNING_OR_RAN]:
        consts.append(
                STATUS + EQUALS + str(JobStatus.RUNNING) + OR +
                STATUS + EQUALS + str(JobStatus.COMPLETED) + OR +
                '(' + STATUS + EQUALS + str(JobStatus.REMOVED) + AND +
                 LAST_STATUS + EQUALS + str(JobStatus.RUNNING) + ')')
        condor_q = True
        condor_history = True

    # merge the individual constraints into a big OR statement
    constraint = 'true'
    if len(consts) > 0:
        constraint = '(' + (')' + OR + '(').join(consts) + ')'

    # prepare the condor python bindings
    schedd = htcondor.Schedd()

    "collect the relevant jobs and fields from condor"
    # jobs are formatted as dicts of required_fields to their condor values
    jobs = []
    if condor_q:
        jobs += schedd.query(constraint, list(required_fields))
    if condor_history:
        limit = SpecialClassAds.ENTERED_STATUS_TIME + ' ' + CondorOperators.GREATER_THAN + ' ' + str(since_time)
        constraint = "(%s %s %s)" % (constraint, CondorOperators.AND, limit)
        olds = schedd.history(constraint, list(required_fields), 10000000)     # TODO: magic number
        for ad in olds:
            jobs.append(ad)
    return jobs


def spoof_val_cache():
    """for debug. Spoofs previous RemoteUserCPU value in cache"""
    f = open(Filenames.VALUE_CACHE, 'w')
    # {GlobalJobId: { classad field: [val, timestamp], ...}, ... }
    j = {
        "uclhctest.t2.ucsd.edu#265.49#1452298787": {
            "RemoteUserCpu": [89568.0, 1454011005]
        }
    }
    json.dump(j, f)
    f.close()


def get_prev_value_from_cache(jobID, field, init_time, context):
    """
    get the last known value (and time thereof) of the passed field of the job
    with JobID from the previous value cache. If not in the cache, assumes
    the fields usual initial value (as stored in the initial job values file)
    and adds it, for the passed init_time (initial time of the job) to the cache,
    then returns that.
    Note that if the previous val must be assumed as initial but the field's
    initial value is not saved in the initial value file, an error will cause
    the Daemon to quit.

    arguments:
        jobID  --  the global ID of the job for which to find prev val of field
        field  --  the condor field name for which to find the prev val of
        init_time  --  the 'start' time of this job, only used if the job was
                       not in the cache and so must be added there
        context    --  the contextual data object, used for handles to
                       the previous value cache.

    returns:
        [val, time] -- the previous value and its corresponding time of the job
    """
    # if val was cached, return it and its cache time
    if (jobID in context.prev_value_cache) and (field in context.prev_value_cache[jobID]):
        return context.prev_value_cache[jobID][field] # [val, time]

    # otherwise, we must assume the previous val as initial at job start
    # if it's not in the init value list, report error and exit
    print "field", field
    print "init vals:", context.job_init_vals
    if field not in context.job_init_vals:
        print ("ERROR! The change in field %s was requested, which requires "+
               "previous value caching. The requested field does NOT appear in "+
               "the initial value cache (%s). Please add it and its initial value! Exiting"
               ) % (field, Filenames.INIT_JOB_FIELDS)
        exit()

    # grab the default init value
    init = context.job_init_vals[field]

    # add it to the previous value cache
    if jobID not in context.prev_value_cache:
        context.prev_value_cache[jobID] = {}
    context.prev_value_cache[jobID][field] = [init, init_time]

    return [init, init_time]


def get_duration_of_job_within_bin(job, t0, t1):
    """
    calculations the duration of a job (in some active state) within the bin from t0 to t1,
    and returns also the start time of the job and its end time, defaulted to t1 if still running.

    arguments:
        job -- a tripped job structure
        t0  -- the start time of the bin (inclusive), seconds since epoch
        t1  -- the end time of the bin (inclusive), seconds since epoch

    returns
        [int, int, int] -- the duration of the job within the bin. Minimum 0, maximum t1 - t0
                        -- the start time of the job
                        -- the end time of the job (t1 if still running)
    """

    # start and end of the jobs current state (end defaults to t1 if longer)
    start, end = None, None

    # active states start at EnteredCurrentStatus and 'end' at t1
    if job[SpecialClassAds.JOB_STATUS] in JobStatus.ONGOING:
        start = job[SpecialClassAds.ENTERED_STATUS_TIME]
        end   = t1

    # done states end at EnteredCurrentStatus
    if job[SpecialClassAds.JOB_STATUS] in JobStatus.DONE:
        end = job[SpecialClassAds.ENTERED_STATUS_TIME]

        # jobs which died idle 'started' at their enqueue time
        if job[SpecialClassAds.LAST_JOB_STATUS] == JobStatus.IDLE:
            start = job[SpecialClassAds.ENQUEUE_TIME]
        else:
            start = job[SpecialClassAds.JOB_START_DATE]

    # job is entirely outside bin
    if (end <= t0) or (start >= t1):
        return [0, start, end]

    # the time for which the job runs within the bin
    time_in_bin = min(end, t1) - max(t0, start)

    return (time_in_bin, start, end)


def get_change_in_val_over_bin(job, field, t0, t1, context):
    """
    calculates the change in a job field over/within the bin [t0, t1]. Does this by consulting
    the previous value cache (possibly creating a new entry if the job is unfamiliar,
    using default initial values). Returns also the duration of the job within the bin (<= t1-t0)

    arguments:
        job    -- a stripped job structure which MUST contain field
        field  -- the field in job for which to calculate the change in value of
        t0     -- the start time of the bin (inclusive), seconds since epoch
        t1     -- the end time of the bin (inclusive), seconds since epoch
        context -- the contextual variables object (for cache handles)

    returns
        [float, int] -- the change in value of the field over the bin (or within), and its duration in bin.
                        The duration of the job is less than or equal to t1 - t0.
    """
    time_in_bin, start, end = get_duration_of_job_within_bin(job, t0, t1)

    # the prev val of the field and that time the job had it
    prev_val, prev_time = get_prev_value_from_cache(job[SpecialClassAds.JOB_ID], field, start, context)

    # change in val per unit time, incurred since prev
    val_change_rate = (job[field] - prev_val)/float(context.current_time - prev_time)

    # change in val over bin
    val_change_in_bin = time_in_bin * val_change_rate

    return [val_change_in_bin, time_in_bin]



def is_job_status_in_metric_statuses(job_status, prev_job_status, metric_statuses):
    """
    determines whether a job of current and previous status should be included in a metric,
    given the user declared list of job statuses for the metric

    arguments:
        job_status      --  the current status of the job (Condor ID: 1-7)
        prev_job_status --  the job's previous status (Condor LastJobStatus. Condor ID 1-7)
        metric_statuses --  a list of MetricFields.JobStatuses as declared in the metric

    returns:
        True or False
    """
    # no specified statuses means it wants to se all jobs
    if len(metric_statuses) == 0:
        return True

    for req_stat in metric_statuses:
        if req_stat == MetricsFields.JobStatuses.ALL:
            return True
        if (req_stat == MetricsFields.JobStatuses.IDLE) and (job_status == JobStatus.IDLE):
            return True
        if (req_stat == MetricsFields.JobStatuses.RUNNING) and (job_status == JobStatus.RUNNING):
            return True
        if (req_stat == MetricsFields.JobStatuses.REMOVED) and (job_status == JobStatus.REMOVED):
            return True
        if (req_stat == MetricsFields.JobStatuses.COMPLETED) and (job_status == JobStatus.COMPLETED):
            return True
        if (req_stat == MetricsFields.JobStatuses.HELD) and (job_status == JobStatus.HELD):
            return True
        if (req_stat == MetricsFields.JobStatuses.TRANSFERRING_OUTPUT) and (job_status == JobStatus.TRANSFERRING_OUTPUT):
            return True

        if (req_stat == MetricsFields.JobStatuses.RAN_THEN_REMOVED) and (job_status == JobStatus.REMOVED):
            if prev_job_status == JobStatus.RUNNING:
                return True
        if (req_stat == MetricsFields.JobStatuses.IDLE_THEN_REMOVED) and (job_status == JobStatus.REMOVED):
            if prev_job_status == JobStatus.IDLE:
                return True
        if req_stat == MetricsFields.JobStatuses.RUNNING_OR_RAN:
            if job_status == JobStatus.RUNNING:
                return True
            if job_status == JobStatus.COMPLETED:
                return True
            if (job_status == JobStatus.REMOVED) and (prev_job_status == JobStatus.RUNNING):
                return True
    return False


def get_influx_DB_write_string_from_metric_data(metric, metric_vals_at_bins, bin_times):
    """
    formats metric data (values of metric at every bin time) into a Influx DB HTTP Push string. These
    are database specific and can be merged with other Influx pushes to the same database

    arguments:
        metric              -- the metric specification object
        metric_vals_at_bins -- the values (split into groups) of the metric at each bin time
        bin_times           -- an array of the starting time of each bin, coordinated with vals

    returns:
        a string of all metric data formatted to the Influx DB HTTP Push standard.
    """

    # vals_at_bins = [  [ (val, groups), (val, groups), ...], ... ] where groups = {'Owner':'trjones',...}

    measurement = metric[MetricsFields.MEASUREMENT_NAME]
    metric_string = ""
    for i in range(len(metric_vals_at_bins)):
        for pair in metric_vals_at_bins[i]:
            val    = pair[0]
            groups = pair[1]
            tag_segment = ','.join([label + '=' + groups[label] for label in groups])
            line = measurement + "," + tag_segment + " value=" + str(val) + " " + str(bin_times[i])
            metric_string += line + "\n"
    return metric_string[:-1]  # remove trailing newline


def create_influx_database(db_name, domain):
    """attempts to create a new database at the specified domain. If successful returns true, else false"""

    # prepare the URL
    if domain[-1] != "/":
        domain += "/"
    url = domain + "query?q=CREATE%20DATABASE%20"+db_name

    req = urllib2.Request(url)
    try:
        urllib2.urlopen(req)
        return True
    except:
        return False


def push_metric_data_to_influx_db(db_metrics, domain):
    """
    pushes raw formatted metric data (orgnasied by destination database name) to influx DB's at domain.
    records any data which failed to be pushed and returns. Creates a new influxDB if the database didn't
    already exist at the domain

    arguments:
        db_metrics -- a dict of database name to a string of raw data to push (InfluxDB HTTP push format)
        domain     -- the internet domain of the destination databases

    returns:
        {db: data, ...} -- a dict of database name to raw string data which failed to be pushed
    """
    # prepare the URL
    if domain[-1] != "/":
        domain += "/"

    # prepare a list of failed metrics
    failed = {}  # {db name: "data", ...}

    # try to push relevant data to each database, recording failures
    for db_name in db_metrics:
        url = domain + "write?db=" + db_name
        req = urllib2.Request(url, db_metrics[db_name])
        try:
            urllib2.urlopen(req)
        except urllib2.HTTPError, err:
            if err.code == 404:
                msg = err.read()
                if InfluxPatterns.INEXISTANT_DATABASE in msg:

                    # try create the database
                    if create_influx_database(db_name, domain):

                        # if created database, try to push again
                        try:
                            urllib2.urlopen(req)
                        except:
                            print "Created new database %s but failed to push to it:" % db_name
                            print db_metrics[db_name]
                            failed[db_name] = db_metrics[db_name]
                    else:
                        print (("Failed to create new database %s at %s. " % (db_name, domain)) +
                                "The follow data failed to be pushed:\n" + db_metrics[db_name])
                        failed[db_name] = db_metrics[db_name]

                else:
                    print "We failed (404) trying to push the follow metric data to database %s at %s:" % (db_name, domain)
                    print db_metrics[db_name]
                    failed[db_name] = db_metrics[db_name]
            else:
                print "We failed (%s) trying to push the follow metric data to database %s at %s:" % (
                        str(err.code), db_name, domain)
                print db_metrics[db_name]
                failed[db_name] = db_metrics[db_name]

    return failed


class ContextData:
    def __init__(self):
        """Grabs the required contextual data from files (and some calculations)"""
        # load files
        self.last_bin_time = load_last_bin_time()
        self.prev_value_cache   = load_prev_val_cache()
        self.job_init_vals = load_job_init_vals()
        config             = load_config()
        self.bin_duration  = config[ConfigFields.BIN_DURATION]
        self.database_domain = config[ConfigFields.DATABASE_DOMAIN]
        self.metrics       = config[ConfigFields.METRICS]
        self.current_time  = int(time.time())  # may be overwritten by class stripper to ServerTime

    def update_current_time_to_server_time(self, jobs):
        """updates the current_time field to use the server time as reported by a job. Not gauranteed to change"""
        for job in jobs:
            if SpecialClassAds.SERVER_TIME in job:
                self.current_time = job[SpecialClassAds.SERVER_TIME]
                break



# spoofing
spoof_config_metrics()
spoof_val_cache()




# get contextual values
context = ContextData()

# get needed fields of needed jobs
jobs = get_relevant_jobs_and_fields_for_metrics(context.metrics, context.last_bin_time)
context.update_current_time_to_server_time(jobs)

# get the times of each bin (inclusive of start, excludes end time of final bin)
bin_times = range(context.last_bin_time, context.current_time, context.bin_duration)[:-1]
DEBUG_PRINT("initial bin time is %d of duration %d" % (context.last_bin_time, context.bin_duration))

"for each metric, work out its value(s) at every time bin"

metric_data = {}   # { Database name: string (grows), ... }

for metric in context.metrics:

    vals_at_bins = [] #     [ [separated by dif tags, ..], ...]

    metric_type = metric[MetricsFields.METRIC_TYPE]
    metric_op   = metric[MetricsFields.AGGREGATE_OP]

    DEBUG_PRINT("processing metric: ", metric)

    for bin_start in bin_times:


        vals_for_bin = {}  #{groupscode: {'groups':{'Owner':'trjones', ...}, 'jobs': [jobs, ...]}, ..}
                           #group code is string concat of group vals
                           #each groupscode has a 'groups' dict and a 'jobs' list
                           #each job is {'value': val, 'duration': dur}

        "for each bin, look at every job"


        for job in jobs:

            #TODO: actually irrelevanr jobs should be skipped here!!!

            # skip the job if it doesn't satisfy the metrics status requirements
            if not is_job_status_in_metric_statuses(
                    job[SpecialClassAds.JOB_STATUS],
                    job[SpecialClassAds.LAST_JOB_STATUS],
                    metric[MetricsFields.JOB_STATUS]):
                continue


            "process different metric types; each must yield a value and duration variable"

            value, duration = None, None

            if metric_type == MetricsFields.MetricTypes.CHANGE:

                # condor field of the value we want to find the change in
                val_field = metric[ChangeMetricFields.VALUE_CLASSAD_FIELD]

                # skip the job if it doesn't contain the required (but not group) fields and report error
                if val_field not in job:

                    #TODO: this isn't actually an error. Not all jobs match your thing
                    #TODO: you should have an IF to look only at your specified jobs!!!!!!!!!
                    #TODO: (which is required for total validity, not just to avoid ins)

                    print "ERROR! The following job:\n#####"
                    print job
                    print ("#####\ndid not contain the field %s as required by the following metric:" %
                            metric[ChangeMetricFields.VALUE_CLASSAD_FIELD])
                    print metric
                    continue

                # TODO: you might also skip with further constraints here

                # get the change in the desired val
                value, duration = get_change_in_val_over_bin(
                        job, metric[ChangeMetricFields.VALUE_CLASSAD_FIELD],
                        bin_start, bin_start + context.bin_duration,
                        context)

            if metric_type == MetricsFields.MetricTypes.RAW:

                # condor field of the value we want
                val_field = metric[RawMetricFields.VALUE_CLASSAD_FIELD]

                # skip the job if it doesn't contain the required (but not group) fields and report error
                if val_field not in job:
                    print "ERROR! The following job:\n#####"
                    print job
                    print ("#####\ndid not contain the field %s as required by the following metric:" %
                            metric[ChangeMetricFields.VALUE_CLASSAD_FIELD])
                    print metric
                    continue

                value = job[val_field]
                duration = get_duration_of_job_within_bin(job, bin_start, bin_start + context.bin_duration)[0]

            if metric_type == MetricsFields.MetricTypes.COUNTER:

                duration, start, end = get_duration_of_job_within_bin(job, bin_start, bin_start + context.bin_duration)
                value = 1 if (duration > 0) else 0

                # for the counter type, we might actually skip this job depending on op type
                if metric_op == CounterMetricFields.AggregationOps.INITIAL:
                    if start > bin_start:
                        value = 0
                        # TODO put `continue` here if you don't want 0 val on grafana graph
                if metric_op == CounterMetricFields.AggregationOps.FINAL:
                    if end < bin_start + context.bin_duration:
                        value = 0


            # associate the job value with the metric's specified grouping
            job_val = {'value':value, 'duration':duration}
            groups = {}
            for group in metric[MetricsFields.GROUP_FIELDS]:
                if group in job:
                    groups[group] = job[group]
                    # special case: when jobs run on the brick, their job site gets stuffed. repair
                    if group == SpecialClassAds.JOB_SITE:
                        if job[group] in CondorDudValues.JOB_SITE_WHEN_ON_BRICK:
                            groups[group] = job[SpecialClassAds.SUBMIT_SITE]
                else:
                    groups[group] = Labels.METRIC_GROUP_UNKNOWN_VALUE
            group_code = ','.join([str(groups[key]) for key in groups])

            if group_code in vals_for_bin:
                vals_for_bin[group_code]['jobs'].append(job_val)
            else:
                vals_for_bin[group_code] = {'jobs':[job_val], 'groups':groups}



        "collate job data for this time bin"
        # vals_for_bin =   {groupscode: {'groups':{'Owner':'trjones', ...}, 'jobs': [jobs, ...]}, ..}
                           #group code is string concat of group vals
                           #each groupscode has a 'groups' dict and a 'jobs' list
                           #each job is {'value': val, 'duration': dur}

        results_for_bin = []   # [(val, groups), ...] where groups = {'Owner':'trjones',...}

        # summing all jobs (or pre-organised subset, if COUNTER and FIRST | LAST) per group
        if (metric_op == RawMetricFields.AggregationOps.SUM or
            metric_op == ChangeMetricFields.AggregationOps.SUM or
            metric_op == CounterMetricFields.AggregationOps.ALL or
            metric_op == CounterMetricFields.AggregationOps.INITIAL or
            metric_op == CounterMetricFields.AggregationOps.FINAL):

            for group_code in vals_for_bin:
                group_jobs   = vals_for_bin[group_code]['jobs']
                group_groups = vals_for_bin[group_code]['groups']
                result = sum([group_job['value'] for group_job in group_jobs])
                results_for_bin.append((result, group_groups))

        if (metric_op == RawMetricFields.AggregationOps.WEIGHTED_AVERAGE or
            metric_op == ChangeMetricFields.AggregationOps.WEIGHTED_AVERAGE):

            for group_code in vals_for_bin:
                group_jobs = vals_for_bin[group_code]['jobs']
                group_groups = vals_for_bin[group_code]['groups']

                total_duration = 0
                total_weighted = 0
                for group_job in group_jobs:
                    total_weighted += group_job['value'] * group_job['duration']
                    total_duration += group_job['duration']
                result = total_weighted / total_duration
                results_for_bin.append((result, group_groups))

        if metric_op == CounterMetricFields.AggregationOps.WEIGHTED_AVERAGE:

            for group_code in vals_for_bin:
                group_jobs = vals_for_bin[group_code]['jobs']
                group_groups = vals_for_bin[group_code]['groups']

                total_weighted = 0
                for group_job in group_jobs:
                    total_weighted += group_job['value'] * group_job['duration']
                result = total_weighted / context.bin_duration
                results_for_bin.append((result, group_groups))

        if (metric_op == RawMetricFields.AggregationOps.AVERAGE or
            metric_op == ChangeMetricFields.AggregationOps.AVERAGE):

            for group_code in vals_for_bin:
                group_jobs = vals_for_bin[group_code]['jobs']
                group_groups = vals_for_bin[group_code]['groups']

                total_unweighted = 0
                total_jobs = 0
                for group_job in group_jobs:
                    total_unweighted += group_job['value']
                    total_jobs += 1
                result = total_unweighted / total_jobs
                results_for_bin.append((result, group_groups))


        "submit metrics for this bin"
        vals_at_bins.append(results_for_bin)


    "submit all info for this metric"
    # vals_at_bins = [  [ (val, groups), (val, groups), ...], ... ] where groups = {'Owner':'trjones',...}

    metric_string = get_influx_DB_write_string_from_metric_data(metric, vals_at_bins, bin_times)
    if metric[MetricsFields.DATABASE_NAME] in metric_data:
        metric_data[metric[MetricsFields.DATABASE_NAME]] += "\n" + metric_string
    else:
        metric_data[metric[MetricsFields.DATABASE_NAME]] = metric_string

    DEBUG_PRINT("Metric yielded an influxDB push:\n", metric_string)


failed = push_metric_data_to_influx_db(metric_data, context.database_domain)

print "This shit failed to be pushed:"
print failed



# TODO: update the outbox


# TODO: update the prev_val_cache




# TODO: write out prev_val_cache
#






