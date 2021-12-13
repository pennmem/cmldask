from dask_jobqueue import SGECluster
from dask.distributed import Client
import pandas as pd
import os
import pwd
import traceback


def new_dask_client(job_name, memory_per_job, max_n_jobs=100, threads_per_job=1,
                    processes_per_job=1, queue='RAM.q', walltime="1500000",
                    local_directory=None,
                    log_directory=None,
                    scheduler_options={}, **kwargs):
    """
    Returns a new dask client instance with associated dashboard for monitoring jobs
    The default method assumes a very basic use case - that is, embarassingly parallel tasks -
    in which the user simply wants to speed up independent computation by running them in parallel.
    There is just one single-threaded process per job.
    The thread/process parameters are configurable, but will lead to more complicated parallel executions
    which might be hard to track. Workers might share memory instead of being independent.
    Leave the defaults unless you have some specific reason to believe another
    configuration will help with your use case.
    
    Parameters
    ----------
    job_name : gives every job this client runs a shared name. You can subsequently add prefixes to this
                name for individual jobs.
    memory_per_job : a string specifying how much memory each job, 
    
    
    
    """
    dashboard_port = get_unique_port()
    print(f"Unique port for {os.environ['USER']} is {dashboard_port}")
    scheduler_options.update({'dashboard_address': f':{dashboard_port}'})
    print(scheduler_options)
    # create cluster instance
    cluster = SGECluster(
        cores=threads_per_job,
        processes=processes_per_job,
        memory=memory_per_job,
        queue=queue,
        walltime=walltime,
        job_name=job_name,
        local_directory=local_directory or os.environ['HOME'] + "/dask-worker-space/",
        log_directory=log_directory or os.environ['HOME'],
        scheduler_options=scheduler_options,
        **kwargs
    )
    # create client / scheduling interface
    client = Client(cluster)
    cluster.adapt(minimum=0, maximum=max_n_jobs)
    # Print dashboard instructions
    forwarding_path = ":".join(["8000"] + client.dashboard_link[7:-7].split(":"))
    print("To view the dashboard, run:",
          f"\n`ssh -fN {os.environ['USER']}@rhino2.pysch.upenn.edu -L {forwarding_path}`",
          "in your local computer's terminal (NOT rhino) \nand then navigate to localhost:8000 in your browser")
    return client


def get_unique_port():
    """
    Generate user-specific port based on user id
    """
    username = os.environ['USER']
    userid = pwd.getpwnam(username).pw_uid
    return 50000 + userid


def get_exceptions(futures, params):
    exceptions = pd.DataFrame(
        columns=["index", "param", "exception", "traceback_obj"])
    for i, (param, future) in enumerate(zip(params, futures)):
        if future.status == 'error':
            exceptions = exceptions.append(
                {"index": i,
                 "param": param,
                 "exception": repr(future.exception()),
                 "traceback_obj": future.traceback()},
                ignore_index=True)
    exceptions['index'] = exceptions['index'].astype(int)
    exceptions.set_index('index', inplace=True)
    return exceptions

def print_traceback(error_df, index):
    traceback.print_tb(error_df.loc[index, "traceback_obj"])
