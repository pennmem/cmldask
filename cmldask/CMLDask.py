from dask_jobqueue import SGECluster, SLURMCluster
from dask.distributed import Client
from dask.distributed import Future
from typing import Iterable
import pandas as pd
import os
import pwd
import traceback
import pickle


def new_dask_client_sge(
    job_name,
    memory_per_job,
    max_n_jobs=100,
    threads_per_job=1,
    processes_per_job=1,
    adapt=True,
    queue="RAM.q",
    walltime="1500000",
    local_directory=None,
    log_directory=None,
    scheduler_options={},
    **kwargs,
):
    """
    Returns a new dask client instance with associated dashboard for
    monitoring jobs. The default method assumes a very basic use case - that
    is, embarassingly parallel tasks - in which the user simply wants to speed
    up independent computations by running them in parallel. There is just one
    single-threaded process per job. The thread/process parameters are
    configurable, but will lead to more complicated parallel executions
    which might be hard to track. Workers might share memory instead of
    being independent.
    Leave the defaults unless you have some specific reason to believe another
    configuration will help with your use case.

    Parameters
    ----------
    job_name : gives every job this client runs a shared name. You can
        subsequently add prefixes to this name for individual jobs.
    memory_per_job : a string specifying how much memory each job needs.
        Ex: "1GB" for 50 jobs would request 1GB of memory for each job,
        for a total of 50 GB. If any one job exceeds 1GB, you will get a
        memory error.
    max_n_jobs : maximum number of parallel processes (assuming
        processes_per_job is 1). Common courtesy dictates that you avoid
        running more than 100 jobs and hogging cluster resources (default: 100)
    threads_per_job : number of threads used per job (default: 1)
    processes_per_job : number of processes the threads/cpu are split among.
    adapt : boolean determining whether to use adaptive or manual scaling.
    queue : Sun Grid Engine queue to use (default: "RAM.q")
    walltime : timeout for cluster job, after which job is killed. Seconds,
        or HH:MM:SS (default : 17 days)
    local_directory : directory for dask worker space, used internally
    log_directory : a directory to dump worker log outputs. Dumped to home
        directory by default.
    scheduler_options : dict of arguments to pass to Dask scheduler directly.
        See docs for more
        info: https://docs.dask.org/en/latest/how-to/deploy-dask/python-advanced.html#distributed.Scheduler
    **kwargs

    Returns
    -------
    client : instance of dask.Client

    """
#     check conforms to cluster limits
    assert threads_per_job <= 40, "Rhino only has 40 CPU per node"
#     assert memory_per_job <= "128GB", "Rhino only has 128 GB RAM per node"
    
    dashboard_port = get_unique_port()
    print(f"Unique port for {os.environ['USER']} is {dashboard_port}")
    scheduler_options.update({"dashboard_address": f":{dashboard_port}"})
    print(scheduler_options)
    
    # create cluster instance
    cluster = SGECluster(
        cores=threads_per_job,
        processes=processes_per_job,
        memory=memory_per_job,
        queue=queue,
        walltime=walltime,
        job_name=job_name,
        local_directory=local_directory or os.environ["HOME"] + "/dask-worker-space/",
        log_directory=log_directory or os.environ["HOME"],
        scheduler_options=scheduler_options,
        **kwargs,
    )
    # create client / scheduling interface
    client = Client(cluster)
    # Print dashboard instructions
    forwarding_path = ":".join(["7000"] + client.dashboard_link[7:-7].split(":"))
    print(
        "To view the dashboard, run:",
        f"\n`ssh -fN {os.environ['USER']}@rhino2.psych.upenn.edu -L {forwarding_path}`",
        "in your local computer's terminal (NOT rhino) \nand then navigate to localhost:7000 in your browser",
    )
    if adapt:
        cluster.adapt(minimum=0, maximum=max_n_jobs)
    else:
        cluster.scale(max_n_jobs)
        print("You've chosen to scale your cluster manually.",
        "This means workers will continue to run until you manually shut them down.",
        "Remember to run `client.shutdown` after you're done computing and no longer need to reserve resources.")

    return client


def new_dask_client_slurm(
    job_name,
    memory_per_job,
    max_n_jobs=100,
    threads_per_job=1,
    processes_per_job=1,
    adapt=True,
    queue="RAM",
    walltime="1500000",
    local_directory=None,
    log_directory=None,
    scheduler_options={},
    **kwargs,
):
    """
    Returns a new dask client instance with associated dashboard for
    monitoring jobs. The default method assumes a very basic use case - that
    is, embarassingly parallel tasks - in which the user simply wants to speed
    up independent computations by running them in parallel. There is just one
    single-threaded process per job. The thread/process parameters are
    configurable, but will lead to more complicated parallel executions
    which might be hard to track. Workers might share memory instead of
    being independent.
    Leave the defaults unless you have some specific reason to believe another
    configuration will help with your use case.

    Parameters
    ----------
    job_name : gives every job this client runs a shared name. You can
        subsequentlyadd prefixes to this name for individual jobs.
    memory_per_job : a string specifying how much memory each job needs.
        Ex: "1GB" for 50 jobs would request 1GB of memory for each job,
        for a total of 50 GB. If any one job exceeds 1GB, you will get a
        memory error.
    max_n_jobs : maximum number of parallel processes (assuming
        processes_per_job is 1). Common courtesy dictates that you avoid
        running more than 100 jobs and hogging cluster resources (default: 100)
    threads_per_job : number of threads used per job (default: 1)
    adapt : boolean determining whether to use adaptive or manual scaling.
    queue : SLURM queue to use (default: "RAM")
    walltime : timeout for cluster job, after which job is killed. Seconds,
        or HH:MM:SS (default : 17 days)
    local_directory : directory for dask worker space, used internally
    log_directory : a directory to dump worker log outputs. Dumped to home
        directory by default.
    scheduler_options : dict of arguments to pass to Dask scheduler directly.
        See docs for more
        info: https://docs.dask.org/en/latest/how-to/deploy-dask/python-advanced.html#distributed.Scheduler
    **kwargs

    Returns
    -------
    client : instance of dask.Client

    """
    # check conforms to cluster limits
    assert threads_per_job <= 40, "Rhino only has 40 CPU per node"
#     assert memory_per_job <= "128GB", "Rhino only has 128 GB RAM per node"
    
    dashboard_port = get_unique_port()
    print(f"Unique port for {os.environ['USER']} is {dashboard_port}")
    scheduler_options.update({"dashboard_address": f":{dashboard_port}"})
    print(scheduler_options)
    # create cluster instance
    cluster = SLURMCluster(
        cores=threads_per_job,
        processes=processes_per_job,
        memory=memory_per_job,
        queue=queue,
        walltime=walltime,
        job_name=job_name,
        local_directory=local_directory or os.environ["HOME"] + "/dask-worker-space/",
        log_directory=log_directory or os.environ["HOME"],
        scheduler_options=scheduler_options,
        **kwargs,
    )
    # create client / scheduling interface
    client = Client(cluster)
    # Print dashboard instructions
    forwarding_path = ":".join(["8000"] + client.dashboard_link[7:-7].split(":"))
    print(
        "To view the dashboard, run:",
        f"\n`ssh -fN {os.environ['USER']}@rhino2.psych.upenn.edu -L {forwarding_path}`",
        "in your local computer's terminal (NOT rhino) \nand then navigate to localhost:8000 in your browser",
    )
    if adapt:
        cluster.adapt(minimum=0, maximum=max_n_jobs)
    else:
        cluster.scale(max_n_jobs)
        print("You've chosen to scale your cluster manually.",
        "This means workers will continue to run until you manually shut them down.",
        "Remember to run `client.shutdown` after you're done computing and no longer need to reserve resources.")

    return client


def get_unique_port():
    """
    Generate user-specific port based on user id
    """
    username = os.environ["USER"]
    userid = pwd.getpwnam(username).pw_uid
    return 50000 + userid


def get_exceptions(futures: Iterable[Future], params: Iterable):
    exceptions = []
    for i, (param, future) in enumerate(zip(params, futures)):
        if future.status == "error":
            exceptions.append(pd.Series(
                {
                    "param": param,
                    "exception": repr(future.exception()),
                    "traceback_obj": future.traceback(),
                }
            ))
    if not len(exceptions):
        raise Exception("None of the given futures resulted in exceptions")
    exceptions = pd.concat(exceptions, axis=1).T
    exceptions.set_index("param", inplace=True)
    return exceptions


def print_traceback(error_df, index):
    traceback.print_tb(error_df.loc[index, "traceback_obj"])


def filter_futures(futures: Iterable[Future], status: Iterable[str] = ["finished"], return_mask=False):
    """
    Helper function to filter futures objects. By default returns only
    futures with status "finished". These can be gathered from distributed
    memory without error. If return_mask, returns (filtered_futures, mask) where mask is a list of booleans 
    signifying which of the futures had the status in question.
    """
    filtered = [f for f in futures if f.status in status]
    if return_mask:
        mask = [f.status in status for f in futures]
        return filtered, mask
    else: return filtered


class Settings:
    """
    settings = Settings()
    settings.somelist = [1, 2, 3]
    settings.importantstring = 'saveme'
    settings.Save()
    settings = Settings.Load()
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.__dict__[k] = v

    def Save(self, filename="settings.pkl"):
        with open(filename, "wb") as fw:
            fw.write(pickle.dumps(self))

    def Load(filename="settings.pkl"):
        return pickle.load(open(filename, "rb"))

    def __repr__(self):
        return (
            "Settings("
            + ", ".join(str(k) + "=" + repr(v) for k, v in self.__dict__.items())
            + ")"
        )

    def __str__(self):
        return "\n".join(str(k) + ": " + str(v) for k, v in self.__dict__.items())
