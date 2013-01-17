#! /usr/bin/env python
import arc
import sys
import os
import random

def example():
    # Creating a UserConfig object with the user's proxy
    # and the path of the trusted CA certificates
    uc = arc.UserConfig()
    uc.JobListFile("/home/aln/.arc/jobs.xml")
    uc.ProxyPath("/tmp/x509up_u%s" % os.getuid())
    uc.CACertificatesDirectory("/etc/grid-security/certificates")

    # Creating an endpoint for a Computing Element
    endpoint = arc.Endpoint("arc.imbg.org.ua", arc.Endpoint.COMPUTINGINFO)

    retriever = arc.ComputingServiceRetriever(uc, [endpoint])
    retriever.wait()
    targets = retriever.GetExecutionTargets()    

    targets = list(targets)
    random.shuffle(targets)

    # Create a JobDescription
    jobdesc = arc.JobDescription()
    jobdesc.Application.Executable.Path = "/bin/hostname"
    jobdesc.Application.Output = "stdout.txt"

    # create an empty job object which will contain our submitted job
    job = arc.Job()
    success = False;
    # Submit job directly to the execution targets, without a broker
    for target in targets:
        print "Trying to submit to", target.ComputingEndpoint.URLString, "(%s)" % target.ComputingEndpoint.InterfaceName, "...",
        sys.stdout.flush()
        success = target.Submit(uc, jobdesc, job)
        if success:
            print "succeeded!"
            break
        else:
            print "failed!"
    if success:
        print "Job was submitted:"
        job.SaveToStream(arc.CPyOstream(sys.stdout), False)
        job.WriteJobsToFile("/home/aln/.arc/jobs.xml", [job])
    else:
        print "Job submission failed"
    
# wait for all the background threads to finish before we destroy the objects they may use
import atexit
@atexit.register
def wait_exit():
    arc.ThreadInitializer().waitExit()

#arc.Logger.getRootLogger().addDestination(arc.LogStream(sys.stderr))
#arc.Logger.getRootLogger().setThreshold(arc.DEBUG)

# run the example
example()
