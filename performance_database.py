from collections import defaultdict
from time import perf_counter
from uuid import uuid4
from random import random, uniform
from functools import wraps

import database


def clear_db(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        database.drop_all_data()
        return func(*args, **kwargs)

    return wrapper


def timethis(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = perf_counter()
        func(*args, **kwargs)
        end = perf_counter()
        return end - start

    return wrapper


def prep_2(func):
    n = 1000

    @wraps(func)
    def wrapper(*args, **kwargs):
        outputs = defaultdict(dict)
        samples = []
        timestamp = uniform(0, 100000000)
        for i in range(n):
            ident = str(uuid4())
            for j in range(n):
                sample = random()
                s = database.Sample(ident.encode(), sample, timestamp + j)
                outputs[ident][timestamp + j] = s
                samples.append(s)
        database.bulk_write(samples)
        return func(*args, **kwargs, outputs=outputs)

    return wrapper


def prep_3(func):
    n = 1000

    @wraps(func)
    def wrapper(*args, **kwargs):
        timestamp = uniform(0, 100000000)
        samples = []
        for i in range(n):
            for j in range(n):
                ident = str(uuid4())
                sample = random()
                s = database.Sample(ident.encode(), sample, timestamp + i)
                samples.append(s)
        database.bulk_write(samples)
        return func(*args, **kwargs, n=n, timestamp=timestamp)

    return wrapper


class DatabasePerformance:
    def run(self):
        write = self.write_samples()
        print(f"Write: {write}s")
        bulk = self.bulk_write(100)
        print(f"Bulk: {bulk}s")
        read_by_ident = self.read_by_ident()
        print(f"Read-by-ident: {read_by_ident}s")
        read_by_timestamp = self.read_by_timestamp()
        print(f"Read-by-timestamp: {read_by_timestamp}s")

    @staticmethod
    def generate_samples(n):
        samples = []
        for i in range(n):
            ident = str(uuid4())
            for j in range(n):
                sample = random()
                timestamp = uniform(0, 100000000)
                s = database.Sample(ident.encode(), sample, timestamp)
                samples.append(s)
        return samples

    @clear_db
    @timethis
    def write_samples(self):
        for sample in self.generate_samples(100):
            database.write_sample(
                sample.ident.decode(), sample.sample, sample.timestamp
            )

    @clear_db
    @timethis
    def bulk_write(self, n):
        samples = self.generate_samples(n)
        database.bulk_write(samples)

    @clear_db
    @prep_2
    @timethis
    def read_by_ident(self, outputs):
        for ident in outputs.keys():
            database.get_samples_by_ident(ident)

    @clear_db
    @prep_3
    @timethis
    def read_by_timestamp(self, n, timestamp):
        for i in range(n):
            database.get_samples_by_timestamp(timestamp + i)


if __name__ == "__main__":
    DatabasePerformance().run()
