from dask.distributed import Client, progress, wait
import pytest
import numpy as np
import cmldask.CMLDask as da

client = Client()

def test_local_client():
    futures = client.map(lambda x: x**2, [x for x in range(5)])
    results = client.gather(futures)
    assert np.array_equal([0, 1, 4, 9, 16], results)


def test_exceptions():
    def raise_odd(x):
        assert x%2==0
    futures = client.map(lambda x: raise_odd(x), [x for x in range(5)])
    wait(futures)
    errors = da.get_exceptions(futures, np.arange(5)) 
    pass
    
