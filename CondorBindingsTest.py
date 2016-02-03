import htcondor
import classad

schedd = htcondor.Schedd()

# current jobs
active = schedd.query('Owner =?= "tysonjones"', ["Owner", "JobStatus", "JobStartDate"])
print "active jobs"
print active

# old jobs
old = schedd.history('EnteredCurrentStatus > 1454388000 && Owner =?= "tysonjones"',
                     ["Owner", "JobStatus", "JobStartDate", "EnteredCurrentStatus"],
                     10000000)
print "old jobs"
for job in old:
    print

print "done"