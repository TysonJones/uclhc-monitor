#!/usr/bin/env python

# Author:       Tyson Jones, January 2016 (MURPA student of Prof Frank Wuerthwein, UCSD).
#               Feel free to contact me at  tjon14@student.monash.edu

# Purpose:      To be periodically run (by CRON) on each submit site.
#               Queries condor_q and condor_history (since last arbitrary run time), collates
#               any (custom formatted) job/classad data into time bins (default, 6 mins) and
#               pushes these to the front-end (influxDB).

import classad
import time
import json
import os


PRINT_DEBUGS = True


class Filenames:
    CONFIG = "config"
    LAST_BIN_TIME = "last_bin_time"
    OUTBOX = "outbox"
    LOG = "log"


class JobStatus:
    """possible statuses of a condor job (at any time) and their integer codes"""
    IDLE, RUNNING, REMOVED, COMPLETED, HELD, TRANSFERRING_OUTPUT = range(1, 7)


class ConfigFields:
    """labels of the fields in the configuration file"""
    BIN_DURATION = "bin duration"

    class MetricsFields:
        """labels of the fields common to all metric types"""
        DATABASE_NAME = "database name"
        MEASUREMENT_NAME = "measurement name"
        VALUE_CLASSAD_FIELD = "value ClassAd field"
        AGGREGATION_FIELD = "aggregate by ClassAd field"

    RAW_VALUE_METRICS = "raw value metrics"
    class RawMetricsFields:
        """labels of the fields specific to raw value metrics"""
        pass

    DIF_VALUE_METRICS = "difference value metrics"
    class DifMetricsFields:
        """labels of the fields specific to difference value metrics"""
        pass

    TAGS = "tags"
    class TagFields:
        """labels of the fields in the list of tags"""
        NAME = "tag name"
        CLASSAD_FIELD = "value ClassAd field"


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
    json.dump(j + jobs)
    f.close()


def add_to_log(message):
    """appends a string message to the log"""
    f = open(Filenames.LOG, "a")
    f.write("ERROR at %s:\n%s\n_____________" % (time.ctime(), message))
    f.close()


def create_default


def check_and_fix_files():

    #TODO do this

    pass


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
    const = "JobStatus =!= 1"
    fd = os.popen("condor_q -l -constraint '%s' " % const)
    ads = classad.parseOldAds(fd)

    # record required fields    (py 2.7 one liner: {f:ad[f] for f in fields})
    for ad in ads:
        job = get_fields_from_ad(ad, fields)
        if job:
            jobs.append(job)

    # grab jobs which have ended since t, and weren't terminated when idle
    const = "EnteredCurrentStatus > %d && LastJobStatus =!= 1" % t
    fd = os.popen("condor_history -l -constraint '%s' " % const)
    ads = classad.parseOldAds(fd)

    # record required fields
    for ad in ads:
        job = get_fields_from_ad(ad, fields)
        if job:
            jobs.append(job)

    return jobs


check_and_fix_files()


# TODO push outbox

# TODO get server time. How should I do it?
# maybe the Condor python bindings will make this more elegant?

bin_duration = get_config()[ConfigFields.BIN_DURATION]
initial_time = get_last_bin_time()
