 Setup Guide
======================

A guide for setting up **InfluxDB**, **Grafana**, **Telegraf** and the **Condor Python Daemon** in RHL 6.

-----------------------------------

InfluxDB
----------
**InfluxDB** is a time-series database and server with much greater flexibility (and documentation) than alternatives such as RRDTool or Graphite. It will store **Telegraf** metrics, the default **Condor** metrics, our custom **Condor** metrics and **XRootD** metrics in the **front-end**.

#### <i class="icon-download"></i> Download
```
wget https://s3.amazonaws.com/influxdb/influxdb-0.9.6.1-1.x86_64.rpm

sudo yum localinstall influxdb-0.9.6.1-1.x86_64.rpm
```

#### <i class="icon-play"></i> Start


``` 
sudo service influxdb start
```
The InfluxDB server uses by default port `8086`.

Check the Influx server is responding by attempting to access `your-domain:8086/write`. You should receive a `404 page not found` in plaintext.

#### <i class="icon-cog"></i> Create a Database

The InfluxSQL command to create a database is
```
CREATE DATABASE databasename
```
This can be run on the server (by entering the influx shell via `influx`), or submitted by HTTP to `your-domain:8086/query` with parameter `q`:

```
your-domain:8086/query?q=CREATE%20DATABASE%20databasename
```

You must make a database with the name expected by the custom Condor metrics daemon. As of 1/25/16, I expect this to be `custom_metrics`. Telegraf will make its own database called `telegraf`.

Reading and writing data through HTTP is discussed [here](https://docs.influxdata.com/influxdb/v0.9/guides/writing_data/)


#### <i class="icon-cog"></i> Configure Condor

TODO

#### <i class="icon-cog"></i> Setup Round Robin

TODO

---------------------------------------

Grafana
----------
**Grafana** is a visualisation framework and server which integrates seamlessly with **InfluxDB**. It will visually display the **Telegraf**, **Condor** and **XRootD** metrics.

#### <i class="icon-download"></i> Download
```
sudo yum install https://grafanarel.s3.amazonaws.com/builds/grafana-2.6.0-1.x86_64.rpm
```

#### <i class="icon-play"></i> Start
```
sudo service grafana-server start
```
The Grafana server uses by default port `3000`.

Check the Grafana server is working by visiting the page `your-domain:3000`. You may need to make an account with Grafana.

#### <i class="icon-cog"></i> Create Condor Metric Graphs

TODO

----------------------------------------

Telegraf
----------
**Telegraf** is a local metric collector which integrates seamlessly with **InfluxDB** and **Grafana**. It will run on each brick and submit to the front-end.

#### <i class="icon-download"></i> Download

```
wget http://get.influxdb.org/telegraf/telegraf-0.10.0-1.x86_64.rpm

sudo yum localinstall telegraf-0.10.0-1.x86_64.rpm
```

#### <i class="icon-cog"></i> Configure

```
cd /etc/telegraf

telegraf -sample-config -input-filter cpu:mem:net:disk:diskio -output-filter influxdb > telegraf.conf
```
Then, open `telegraf.conf` and navigate to the `OUTPUTS` section. 

Replace
``` 
urls = ["http://localhost:8086"]
```
with
``` 
urls = ["your-domain:8086"]
```
where `your-domain` is the domain of the front-end (where the Influx Database is stored).


TODO: specify the metrics we actually want.

#### <i class="icon-play"></i> Start
```
sudo service telegraf start
```

By default, **Telegraf** will collect metrics every 10s. Check that the (newly created) `telegraf` database has populated the `cpu` measurement with time data.

```
your-domain:8086/query?db=telegraf&q=SELECT%20*%20FROM%20cpu
```


#### <i class="icon-cog"></i> Create Grafana Graphs

TODO

----------------------------

Python Daemon
-------------------
The Python Daemon will push custom metrics (which the Condor collector cannot handle) from Condor to the InfluxDB. The Daemon must run on each brick and be scheduled by a CRON job.


#### <i class="icon-cog"></i> Install Condor Bindings
Ensure Python 2.7 is installed.
```
sudo yum install condor-python
```
You may need to first add the repo (see [here](https://research.cs.wisc.edu/htcondor/yum/)) via
```
sudo yum-config-manager --add-repo http://research.cs.wisc.edu/htcondor/yum/repo.d/htcondor-stable-rhel6.repo
```


If you don't have `yum-config-manager`, install it by:
```
sudo apt-get install yum-utils
```


Test the bindings.
```
python
>>> import htcondor
>>> import classad
```



#### <i class="icon-cog"></i> Place Daemon and Caches

TODO

#### <i class="icon-cog"></i> Schedule CRON Job

TODO

#### <i class="icon-cog"></i> Create Grafana Graphs

TODO


#### <i class="icon-cog"></i> Create Custom Metrics

Custom metrics are specified in the `config` file in the `metrics` list as a **JSON** dictionary. They specify where the metrics are sent and how, within each time bin (which divides runtime from previous execution) the metric is aggregated.

 Each metric must have the following keys:

| Key			| Value		| Example 	|
| ---------- | ----------| ------------- |
|`database name` | The name of the influx database to which to push the metric. | `"RunningJobs"`|
|`measurement name`| The name of the measurement as stored in the database. Must adhere to InfluxDB measurement name syntax.|`"num_jobs"`|
|`metric type`| One of `RAW`, `COUNTER` or `DIFFERENCE`. Specifies the numerical nature of the metric. | `"DIFFERENCE"`|
|`value ClassAd field`| The ClassAd field to use as numerical data. Only required if metrc type is `RAW` or `DIFFERENCE`.| `"RemoteUserCpu"`
|`aggregation operation`|One of `SUM`, `LAST`, `FIRST`, `AVERAGE`. Specifies how to aggregate job values within each time bin.|`"SUM"`|
|`group by ClassAd fields`| A list of the ClassAd fields by which to group aggregated data. Metric data with any differing field values are not aggregated and sent to the database individually | `["Owner", "SUBMIT_SITE"]`
|`constraint` [NOT USED]|[NOT USED] Constrains the considered jobs in each time bin. Syntax: TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO |`"JobStatus =?= 1"`|
|`description`| A description (for humans) of the metric|`"The CPU time used by each owner on each job site per bin"`|
|`job status`|A list of job statuses. Only jobs of a status in the list are included in the metric. Job statuses are `IDLE`, `RUNNING`, `REMOVED`, `COMPLETED`, `HELD`, `TRANSFERRING_OUTPUT`, `RAN_THEN_REMOVED`, `IDLE_THEN_REMOVED`, and `RUNNING OR RAN`.|`["RUNNING", "COMPLETED", "RAN_THEN_REMOVED"]` would match all jobs which are running or ended while running.|






| Metric Type | Explanation | Example |
|-------------| ------------| --------|
| `RAW`         | Grabs a single (numerical) value from each job's ClassAd with no operations. Requires that the **value ClassAd field** is also specified. |   |
| `COUNTER` | Counts each job. I.e. increments a value for each job. | Get the number of idle jobs.
| `DIFFERENCE` | Measures the difference in a single (numerical) ClassAd value since previously considered time. Used for measuring rates. Requires that the **value Classad field** is also specified.| Get the rate of CPU time.


|Aggregation Operation|Explanation|Example|
|----|-------|------|
| `SUM` | Sums all values with the same group values in the time bin | Use with `DIFFERENCE`, get the total rate of CPU time of an owner|
| `FIRST` | Uses only the first value (the earliest job) in the time bin| |
| `LAST` | Uses only the last value (the latest job) in the time bin| Use with `COUNTER`, get the final (per time bin) number of running jobs|
|`AVERAGE` | Averages (time weighted) the values in the time bin | |

|Job Status| Explanation|
|----------|------------|
|`IDLE`| Job is currently not running (in the GlideIn queue)|
|`RUNNING`|Job is currently running somewhere|
|`REMOVED`|The is not running; it was removed via `condor_rm`. This is equivalent to `["RAN THEN REMOVED", "IDLE THEN REMOVED"]`.|
|`COMPLETED`|Job is not currently running; it already ran and then finished (was not removed).|
|`HELD`||
|`TRANSFERRING_OUTPUT`||
|`RAN_THEN_REMOVED`|Job was running when it was removed via `condor_rm`. This is like `REMOVED`, but doesn't contain all the jobs which never ever ran (which lack many ClassAd fields)|
|`IDLE_THEN_REMOVED`|Job was idle when it was removed via `condor_rm`.|
|`RUNNING_OR_RAN`|The job is currently running, or was ever ran before completion or removal. Equivalent to `["RUNNING", "COMPLETED", "RAN_THEN_REMOVED"]`

>**Note**:

>  When getting information about idle jobs, only jobs that are currently idle (`IDLE`) or were removed whilst idle (`IDLE_THEN_REMOVED`) can be used in metrics. When a job leaves its idle state (without being removed. I.e. it ran), time information about its idleness is lost. Thus metrics reporting idle job information require the Daemon to run frequently (to catch currently idle jobs) to be reliable.


--------------------------------------------

####Examples

Uses the database `RunningJobs` with a measurement called `num_jobs`, which gives the number of jobs (not idle) being run by each owner (at the end of each time bin), on each of their job sites.

```
{
    "database name":           "RunningJobs",
    "measurement name":        "num_jobs",
    "metric type":             "COUNTER",
    "aggregation operation":   "LAST",
    "group by ClassAd fields": ["Owner", "MATCH_EXP_JOB_SITE"],
    "job status":              ["RUNNING", "COMPLETED", "RAN THEN REMOVED"],
    "description":             "The number of running jobs (by each owner on each site) at the end of each bin"
}
```

Uses the database `Utilisation` with a measurement called `cpu_time` which gives the CPU time (in each bin) used by each user for all their running and previously run (not currently idle) jobs combined.

```
{
    "database name":           "Utilisation",
    "measurement name":        "cpu_time",
    "metric type":             "DIFFERENCE",
    "value ClassAd field":     "RemoteUserCpu",
    "aggregation operation":   "SUM",
    "group by ClassAd fields": ["Owner"],
    "job status":              ["RUNNING", "COMPLETED", "RAN_THEN_REMOVED"],
    "description":             "The CPU time used by each owner on each job site per bin"
}
```

Uses the database `Dormant` with a measurement called `idle_jobs` which gives the number of idle jobs at each submit site; i.e. the number of unique jobs which were idle at any point within the time bin.
```
{
    "database name":           "Dormant",
    "measurement name":        "idle_jobs",
    "metric type":             "COUNTER",
    "aggregation operation":   "SUM",
    "group by ClassAd fields": ["Owner"],
    "job status":              ["IDLE"],
    "description":             "The CPU time used by each owner on each job site per bin"
}
```