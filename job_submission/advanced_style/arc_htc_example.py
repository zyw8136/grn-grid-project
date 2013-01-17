#! /usr/bin/env python
#
#   HTC_ARC_example01.py.
#   Code example used in various 'ARC fro developers' lectures
#
#   Copyright (C) 2011, 2012 GC3, University of Zurich
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#   This script was gladly provided by Sergio Maffioletti!

import sys
import os
import time
from optparse import OptionParser


import arc

def main():
    """
    The script is a simple demonstration of how ARC python client libraries
    could be used to write HTC control scripts.
    From a list of .inp files, the script will submit one job per file
    executing a simple grep searching for a defined pattern.
    After submission of all jobs, the script will monitor their status
    and retrieve the output of each of them
    selecting those in 'FINISHED' state. As part of the post-processing
    step, the script will search through each job's stdout for a
    validation pattern.
    """

    # 0) parse command line arguments
    parser = OptionParser(usage="%prog INPUT_FOLDER")

    parser.add_option("-r", "--resource", dest="resource", help="select destination resource", default="aio.grid.zoo")

    (options, args) = parser.parse_args(sys.argv[1:])

    if len(args) < 1 or not os.path.isdir(args[0]):
        raise Exception("Wrong number of arguments: this commands expects "
                        "at exactly 1 argument: Input folder to be scanned")

    input_folder = args[0]

    # import User configuration parameters
    _usercfg = arc.UserConfig("", "")

    host_endpoint = options.resource

    sys.stdout.write("Adding Computational service '%s' " % host_endpoint)


    # create arc.Endpoint
    # arc.Endpoint(host_endpoint, arc.Endpoint.COMPUTINGINFO, "org.nordugrid.ldapng")

    # Create an instance of the ComputingServiceRetriever
    # it uses plugins to reach the endpoint according to the exposed interface
    # plugins avaialble for ARC0, ARC1, CreamCE, Unicore(?)
    retriever = arc.ComputingServiceRetriever(_usercfg, [arc.Endpoint(host_endpoint, arc.Endpoint.COMPUTINGINFO)])

    # wait is necessary as the retriever contacts all the endpoints using separated threads
    retriever.wait()

    # Fetch ExecutionTarges corresponding to the endpoints
    # One ExecutionTarget per interfaces/queue
    targets = retriever.GetExecutionTargets()    

    print("'%s' returned '%d' valid ExecutionTargets" % (host_endpoint, len(targets)))

    # Create an instance of Jobsupervisor
    # this is the entity we will use to monitor anc act upon the submitted jobs
    # it takes an initial list of jobs. In our case empty
    _jobsupervisor = arc.JobSupervisor(_usercfg)

    # Prepare Job description

    # base xrsl
    xrsl = "+"
    xrsl_template = "& (executable='/bin/grep')(arguments='-i' 'egicf2012' 'inputfile')(join='yes')(stdout='egicf2012.log')(count='1')(gmlog='.arc')"

    jobdesclang = "nordugrid:xrsl"
                
    jobdescription_list = []

    print("Creating JobDescription: ")
    # iterate over input folder and create one jobdescription per file
    for filename in os.listdir(input_folder):
        xrsl = "%s (%s (InputFiles=('inputfile' '%s')) (jobname='%s'))" % (xrsl, xrsl_template, os.path.abspath(os.path.join(input_folder,filename)), filename)

    jds = arc.JobDescriptionList()
    if not arc.JobDescription_Parse(xrsl, jds, jobdesclang):
      sys.stdout.write("[%s:failed] " % filename)
      raise Exception("Failed creating JobDescription with xrls '%s'" % xrsl)
    sys.stdout.write("[%s:ok] " % filename) 

    j = arc.Job()  
    job_list = {}

    broker = arc.Broker(_usercfg)

    sys.stdout.write("Submitting jobs: ")
    for jd in jds:

      broker.set(jd)

      for t in targets:
	if broker.match(t):
	    if not t.Submit(_usercfg, jd, j):
	        sys.stdout.write("[failed]")
		continue
	    sys.stdout.write("[ok]")
            _jobsupervisor.AddJob(j)
            job_list[j.JobID.str()] = j
	    break
        else:
            sys.stdout.write("resource rejected\n")

      # if not arc.Job.WriteJobsToFile(_usercfg.JobListFile(),[j]):
      #   sys.stdout.write("Warning: Failed updating '%s' with job '%s'" % (_usercfg.JobListFile(),j.JobID.str()))

######## End of job submission part ######################

    sys.stdout.write("Start Monitoring... \n")

######## Start of job monitoring part ####################

##### monitor job status
    failed_states = [arc.JobState.DELETED, arc.JobState.KILLED, arc.JobState.FAILED, ]
    terminal_states = [arc.JobState.FINISHED, ] + failed_states
    running_states = [arc.JobState.ACCEPTED, arc.JobState.FINISHING, arc.JobState.HOLD, arc.JobState.PREPARING, arc.JobState.OTHER, arc.JobState.QUEUING, arc.JobState.RUNNING, arc.JobState.SUBMITTING, arc.JobState.UNDEFINED]

    unknown_list = {}

    remaining = len(job_list)
    total = remaining

    jobs_ok = 0
    jobs_failed = 0

    success = 0
    found_file_name = ""
    succeed_id = None

    successfull_jobs = []
    
    # Main monitoring loop
    # This is an oversimplified version of a generic monitoring loop:
    # 1. Update job status by querying remote resources
    # 2. Select those job with "FINISHED" status
    # 3. Only 1 job is supposed to finish correctly: Pattern found
    # 4. Compute statistics
    while remaining > 0:
        # update jobs
      
        failed_ids = []
        joblist = []

        _jobsupervisor.ClearSelection()
        _jobsupervisor.Update()
        jobs = _jobsupervisor.GetAllJobs()
        # total = len(jobs)

        arc_sl = arc.StringList()
      
        _jobsupervisor.SelectByStatus(["Finished"])
        if not _jobsupervisor.Retrieve("results", True, True, arc_sl):
            failed_ids = _jobsupervisor.GetIDsNotProcessed()

        succeed_id = [ ids.str() for ids in _jobsupervisor.GetIDsProcessed()]

        if succeed_id:
            successfull_jobs.extend([ job for job in jobs if job.JobID.str() in succeed_id ])

           # for job in jobs:
           #      if job.JobID.str() in succeed_id:
           #          # XXX: TODO Retrieve result of FINISHED job
           #          found_file_name = job.Name
           #          success = 1

        if not _jobsupervisor.Clean():
            failed_ids += [ids for ids in [npid for npid in _jobsupervisor.GetIDsNotProcessed()] if not ids in failed_ids]
        
        processed = len(_jobsupervisor.GetIDsProcessed())

        _jobsupervisor.ClearSelection()
        _jobsupervisor.SelectByStatus(["Failed","Killed","Deleted"])
        if not _jobsupervisor.Clean():
            failed_ids += [ids for ids in [npid for npid in _jobsupervisor.GetIDsNotProcessed()] if not ids in failed_ids]

        processed += len(_jobsupervisor.GetIDsProcessed())
      
        remaining = remaining - processed - len(failed_ids)

        print("""
        Remaining: %d
        Success: %d
        Failed:	%d
        """ % (remaining, success, len(failed_ids)))
        # wait....
        time.sleep(10)

    if len(successfull_jobs) > 0:
        for id in successfull_jobs:
            print "Pattern found in file %s" % job_list[id].Name
        return 0
    else:
        print "No pattern found"
        return 1
   
# run script
if __name__ == '__main__':
    sys.exit(main())
