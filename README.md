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
Tell Telegraf to send `cpu`, `memory`, `network,` `disk` and `disk io` metrics.
```
cd /etc/telegraf

telegraf -sample-config -input-filter cpu:mem:net:disk:diskio -output-filter influxdb > telegraf.conf
```
Then, open (the newly created) `telegraf.conf` and navigate to the `OUTPUTS` section.

Replace
```
urls = ["http://localhost:8086"]
```
with
```
urls = ["your-domain:8086"]
```
where `your-domain` is the domain of the front-end (where the Influx Database is stored).


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

Before they can be used in graphs, any new databases created/used by metrics in the Daemon must be registered with Grafana as a `Data Source`.

In the Grafana side bar, select `Data Sources`, and at the top click `Add New`.
Give the database a new name for Grafana (visible when selecting a data source for a graph), and a
**Type** of
```
InfluxDB 0.9.x
```
The **Url** should be
```
your-domain:8086
```
with **access** via `proxy`.

The **Database** field should have the name of the database as defined in your custom metrics. The **User** and **Password** should be that which you've logged into Grafana with.


TODO


#### <i class="icon-cog"></i> Create Custom Metrics

Custom metrics are specified in the `config` file in the `metrics` list as a **JSON** dictionary. They specify where the metrics are sent and how, **within each time bin** (which divides runtime from previous execution) the metric is calculated. For the best understanding, please see the visual explanations at the bottom of the document.

 Each metric must have the following keys:

| Key			| Value		| Example 	|
| ---------- | ----------| ------------- |
|`database name` | The name of the influx database to which to push the metric. | `"RunningJobs"`|
|`measurement name`| The name of the measurement as stored in the database. Must adhere to InfluxDB measurement name syntax.|`"num_jobs"`|
|`metric type`| One of `RAW`, `COUNTER` or `CHANGE` (explained below). Specifies the numerical nature of the metric, or how each job contributes to a metric. | `"CHANGE"`|
|`value ClassAd field`| The ClassAd field to use as numerical data. Only required if metrc type is `RAW` or `CHANGE`.| `"RemoteUserCpu"`
|`aggregation operation`|Available operations are specific to the `metric type` (see below) and specify how the contributions of each job to a metric are aggregated.|`"SUM"`|
|`group by ClassAd fields`| A list of the ClassAd fields by which to group aggregated data. Metric data with any differing field values are not aggregated and sent to the database individually. If **n** fields are specified, each time datum will have **n** qualifiers. | `["Owner", "SUBMIT_SITE"]`|
|`description`| A description (for humans) of the metric|`"The CPU time used by each owner on each job site per bin"`|
|`job statuses`|A list of job statuses. Only jobs of a status in the list are included in each bin calculation, and thus the metric. Job statuses are `IDLE`, `RUNNING`, `REMOVED`, `COMPLETED`, `HELD`, `TRANSFERRING_OUTPUT`, `RAN_THEN_REMOVED`, `IDLE_THEN_REMOVED`, and `RUNNING OR RAN`.|`["RUNNING", "COMPLETED", "RAN_THEN_REMOVED"]` would match all jobs which are running or ended while running (equivalent to `RUNNING_OR_RAN`).|






| Metric Types | Explanation | Example |
|-------------| ------------| --------|
| `RAW`         | Contributes a numerical value straight from each job's ClassAd to the bin. Requires that the **value ClassAd field** is also specified. |   |
| `CHANGE` | Contributes each job's change in a numerical ClassAd value since the time of its previous known value (linearly interpolated across each bin) to the bin. Requires that the **value Classad field** is also specified.| Get the rate of CPU time.|
| `COUNTER` | Counts the number of relevant jobs in the bin. I.e. each job contributes 1 to a growing increment for the bin.| Get the number of idle jobs.|


|`RAW` and `CHANGE` Aggregation Operations|Explanation|Example|
|----|-------|------|
| `SUM` | Sums the raw values or changes thereof of each job in the bin. $$ \sum_i \text{job}_i $$ | Use with `CHANGE`, get the total rate of CPU time of an owner|
| `AVERAGE` | Averages the raw values or changes thereof of each job in the bin, by the number of jobs. $$\frac{\sum_i \text{job}_i}{\#\text{jobs}}$$|
| `WEIGHTED AVERAGE` | Averages the raw values of changes thereof of each job, weighted by the duration of that job within the bin, over time. That is, jobs which span the entire bin or most of it more strongly affect the average than jobs running only shortly within the bin. $$ \frac{ \sum_i \text{job}_i \Delta t_i}{ \sum_i \Delta t_i}$$|

| `COUNTER` Aggregation Operations | Explanation | Example|
|--------------------------------|-------------|--------|
| `ALL` | Every job within the bin contributes to the counter. $$ \#\text{jobs}$$||
| `INITIAL` | Count only jobs present at the start of the bin||
|`FINAL` | Count only jobs present at the conclusion of the bin||
|`WEIGHTED AVERAGE`| Find the weighted (by the duration of each job) average of the number of concurrent jobs at any time across the bin. This is equivalent to the hypothetical number of jobs (non-integer) which ran for the full length of the bin, in relation to the duration. $$ \frac{ \sum_i \Delta t_i}{\Delta t_\text{bin}}$$|

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
    "aggregation operation":   "FINAL",
    "group by ClassAd fields": ["Owner", "MATCH_EXP_JOB_SITE"],
    "job statuses":              ["RUNNING OR RAN"],
    "description":             "The number of running jobs (by each owner on each site) at the end of each bin"
}
```

Uses the database `Utilisation` with a measurement called `cpu_time` which gives the CPU time (in each bin) used by each user for all their running and previously run (not currently idle) jobs combined (note that the `job statuses` is entirely equivalent to above).

```
{
    "database name":           "Utilisation",
    "measurement name":        "cpu_time",
    "metric type":             "CHANGE",
    "value ClassAd field":     "RemoteUserCpu",
    "aggregation operation":   "SUM",
    "group by ClassAd fields": ["Owner"],
    "job statuses":            ["RUNNING", "COMPLETED", "RAN_THEN_REMOVED"],
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
    "job statuses":            ["IDLE"],
    "description":             "The CPU time used by each owner on each job site per bin"
}
```

