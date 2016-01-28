

import sys; sys.dont_write_bytecode = True

import json

import Daemon


value_cache = False


def load_val_cache():
    """
    loads the cache containing previous job Classad values

    returns:
        {}  -  the cache of values:{GlobalJobId: {field: [val, time], ...}, ... }
    """
    f = open(Daemon.Filenames.VALUE_CACHE, 'r')
    j = json.load(f)
    f.close()
    return j


def spoof_val_cache():
    """for debug. Spoofs previous RemoteUserCPU value in cache"""
    f = open(Daemon.Filenames.VALUE_CACHE, 'w')
    # {GlobalJobId: { classad field: [val, timestamp], ...}, ... }
    j = {
        "uclhctest.t2.ucsd.edu#265.49#1452298787": {
            "RemoteUserCpu": [89568.0, 1454011005]
        }
    }
    json.dump(j, f)
    f.close()


def get_val_and_time_from_cache(field, jobID):
    """
    get a [val, time] for a particular classAd field from a job in cache
    If the cache is not yet loaded, loads the cache without issue.
    If the job isn't in the cache or doesn't have the field, returns False

    returns
        [value, time]   --  previously cached field value of job and its time
        False           --  job or field aren't cached
    """
    global value_cache
    if not value_cache:
        value_cache = load_val_cache()

    if (jobID not in value_cache) or (field not in value_cache[jobID]):
        return False


def add_val_to_cache(field, value, time, jobID):
    """
    adds a [val, time] to the cache under jobID: field. Cache must be loaded.
    Appends to existing jobs in cache. Overwrites existing same field.

    arguments:
        field  --  the ClassAd field name corresponding to the value
        value  --  the value of the ClassAd field to store
        time   --  the time at which the value applies
        jobID  --  the global ID of the job to which this value applies
    """
    if jobID not in value_cache:
        value_cache[jobID] = {field: [value, time]}
    else:
        value_cache[jobID][field] = [value, time]


def get_val_change_over_bin(ad, field, t0, t1):
    """
    interpolates the change in a job's ClassAd field's value over the time bin t0 to t1.
    The job run time can be misaligned from the time bin (zero returned for disjoint).
    Uses the value_cache: if not present in cache, adds and assumes default value
    as specified in the initial_running_jobs_fields.

    arguments:
        ad    --  the classad of the job
        field --  the classad field of which to measure the change in [t0,t1]
        t0    --  the initial time (seconds since epoch) of the time bin
        t1    --  the final time (seconds since epoch) of the time bin
    """
    jobID = ad[Daemon.SpecialClassAds.JOB_ID]
    oldval = get_val_and_time_from_cache(field, jobID)
    if not oldval:



# spoof the cache (DEBUG) TODO: remove
spoof_val_cache()



