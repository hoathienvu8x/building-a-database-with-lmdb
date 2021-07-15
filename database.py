import os
from operator import itemgetter

import lmdb
from models import Sample
from typing import List, Iterable, Set, Dict

Ki = 1024
Mi = 1024 * Ki
Gi = 1024 * Mi
ENV = lmdb.Environment(
    "example.lmdb",
    map_size=Gi,
    subdir=True,
    readonly=False,
    metasync=True,
    sync=True,
    map_async=False,
    mode=493,
    create=True,
    readahead=True,
    writemap=False,
    meminit=True,
    max_readers=126,
    max_dbs=2,
    max_spare_txns=1,
    lock=True,
)


def write_sample(ident: str, sample: float, timestamp: float) -> Sample:
    ident_encoded = ident.encode()
    input_sample = Sample(ident=ident_encoded, sample=sample, timestamp=timestamp)
    global ENV, KEYS_BY_IDENTITY, KEYS_BY_TIMESTAMP
    with ENV.begin(write=True) as txn:
        txn.put(ident_encoded, bytes(input_sample))
    return input_sample


def get_samples_by_ident(ident: str) -> List[Sample]:
    global ENV
    with ENV.begin() as txn:
        return Sample.from_buffer_copy(txn.get(ident.encode()))


def drop_all_data():
    global ENV, KEYS_BY_IDENTITY, KEYS_BY_TIMESTAMP
    ENV.close()
    for f in os.listdir("example.lmdb"):
        os.remove(os.path.join("example.lmdb", f))
    os.rmdir("example.lmdb")
    ENV = lmdb.Environment(
        "example.lmdb",
        map_size=Gi,
        subdir=True,
        readonly=False,
        metasync=True,
        sync=True,
        map_async=False,
        mode=493,
        create=True,
        readahead=True,
        writemap=False,
        meminit=True,
        max_readers=126,
        max_dbs=2,
        max_spare_txns=1,
        lock=True,
    )
