import lmdb
from models import Sample
from typing import List


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
    writemap=True,
    meminit=True,
    max_readers=126,
    max_dbs=2,
    max_spare_txns=1,
    lock=True,
)
KEYS_BY_IDENTITY = ENV.open_db("idenity_index".encode(), dupsort=True)
KEYS_BY_TIMESTAMP = ENV.open_db("timestamp_index".encode(), dupsort=True)


def write_sample(ident: str, sample: float, timestamp: float) -> Sample:
    input_sample = Sample(ident=ident.encode(), sample=sample, timestamp=timestamp)
    global ENV
    with ENV.begin(write=True) as txn:
        key = f"{input_sample.ident.decode()}-{input_sample.timestamp}".encode()
        txn.put(key, bytes(input_sample))
        txn.put(input_sample.ident, key, db=KEYS_BY_IDENTITY)
        txn.put(f"{int(input_sample.timestamp)}".encode(), key, db=KEYS_BY_TIMESTAMP)
    return input_sample


def get_samples_by_ident(ident: str) -> List[Sample]:
    samples: List[Sample] = []
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
            cursor.set_key(str(int(timestamp)).encode())
            for key in cursor.iternext_dup():
                samples.append(Sample.from_buffer_copy(txn.get(key)))
    return samples
