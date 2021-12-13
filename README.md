# CMLDask
Utility/Wrapper for interacting with the Computational Memory Lab's computing resources using the Dask distributed computing framework

## Installation
Clone this repository:

`git clone git@github.com:pennmem/CMLDask.git` (Using SSH)

Install using pip:

`pip install -e CMLDask -r CMLDask/requirements.txt`

## Usage
See included notebooks for more detailed instructions and examples, but in short:
1) Initialize a new client with dask: `client = CMLDask.new_dask_client("job_name", "1GB")`
2) *Optional*: Open dashboard using instructions printed upon running (1)
3) Define some function `func` that takes an argument `arg` (or many arguments) and returns a result.
4) Define an iterable of arguments for `func` that you want to compute in parallel: `arguments`
5) Use the client to map these arguments to the function on different parallel workers: `futures = client.map(func, arguments)`. Dask's Futures API launches parallel jobs and computes results immediately, but delays collecting the results from distributed memory.
6) Gather results into memory of your current process: `result = client.gather(futures)`

## Dask Documentation
Refer to the [Dask Futures](https://docs.dask.org/en/stable/futures.html#) API for great documentation on the preferred approach for parallel computing. 

Some cases might warrant using [Dask Delayed](https://docs.dask.org/en/stable/delayed.html), which evaluates functions lazily.

Dask clients, once initialized in your kernel, will also automatically parallelize operations on a [Dask DataFrame](https://docs.dask.org/en/stable/dataframe.html) or a [Dask Array](https://docs.dask.org/en/stable/array.html). It is straightforward to convert between standard pandas, numpy, and xarray objects and these dask objects for distributed computing. As long as your data object fits easily in memory, though, it's unlikely that you will stand to benefit much from using these implementations (there is significant overhead).
