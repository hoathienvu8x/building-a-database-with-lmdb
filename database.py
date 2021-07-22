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
KEYS_BY_IDENTITY = ENV.open_db("idenity_index".encode(), dupsort=True)
KEYS_BY_TIMESTAMP = ENV.open_db("timestamp_index".encode(), dupsort=True)


def write_sample(input_sample: Sample) -> None:
    global ENV, KEYS_BY_IDENTITY, KEYS_BY_TIMESTAMP
    with ENV.begin(write=True) as txn:
        key = (
            input_sample.ident
            + b"/"
            + int(input_sample.timestamp * 10).to_bytes(8, byteorder="big")
        )
        txn.put(key, bytes(input_sample))
        txn.put(input_sample.ident, key, db=KEYS_BY_IDENTITY)
        txn.put(
            int(input_sample.timestamp).to_bytes(8, byteorder="big"),
            key,
            db=KEYS_BY_TIMESTAMP,
        )


def get_samples_by_ident(ident: str) -> List[Sample]:
    samples: List[Sample] = list()
    global ENV
    with ENV.begin() as txn:
        with txn.cursor(KEYS_BY_IDENTITY) as cursor:
            cursor.set_key(ident.encode())
            for key in cursor.iternext_dup():
                samples.append(Sample.from_buffer_copy(txn.get(key)))
    return samples


def get_samples_by_timestamp(timestamp: float) -> List[Sample]:
    samples: List[Sample] = []
    global ENV
    with ENV.begin() as txn:
        with txn.cursor(KEYS_BY_TIMESTAMP) as cursor:
            cursor.set_key(int(timestamp).to_bytes(8, byteorder="big"))
            for key in cursor.iternext_dup():
                samples.append(Sample.from_buffer_copy(txn.get(key)))
    return samples


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
    KEYS_BY_IDENTITY = ENV.open_db("idenity_index".encode(), dupsort=True)
    KEYS_BY_TIMESTAMP = ENV.open_db("timestamp_index".encode(), dupsort=True)
