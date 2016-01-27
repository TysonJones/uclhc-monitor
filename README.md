UCLHC Monitor
=====================
Employs Telegraf, python daemons, InfluxDB and Grafana to collect and display usage metrics of the UCLHC Condor and XRootD systems.




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
