# CMLDask
Utility/Wrapper for interacting with the Computational Memory Lab's computing resources using the Dask distributed computing framework

## Installation
Clone this repository:
`git clone git@github.com:pennmem/CMLDask.git` (Using SSH)

Install using pip
`pip install -e CMLDask -r CMLDask/requirements.txt`

## Usage
See included notebooks for more detailed instructions and examples, but in short:
1) Initialize a new client with dask: `client = CMLDask.new_dask_client("job_name", "1GB")`
2) *Optional*: Open dashboard using instructions printed upon running (1)
3) Define some function `func` that takes an argument `arg` (or many arguments) and returns a result.
4) Define an iterable of arguments for `func` that you want to compute in parallel: `arguments`
5) Use the client to map these arguments to the function on different parallel workers: `futures = client.map(func, arguments)`. Dask's Futures API launches parallel jobs and computes results immediately, but delays collecting the results from distributed memory.
6) Gather results into memory of your current process: `result = client.gather(futures)`
