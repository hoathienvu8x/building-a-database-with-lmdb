import unittest
from collections import defaultdict
from uuid import uuid4
from random import random, uniform

import database


class DatabaseTestCase(unittest.TestCase):
    def test_write_sample(self):
        ident = str(uuid4())
        sample = random()
        timestamp = uniform(0, 100000000)
        output = database.write_sample(ident, sample, timestamp)
        self.assertEqual(output.ident, ident.encode())
        self.assertEqual(output.timestamp, timestamp)
        self.assertEqual(output.sample, sample)

    def test_read_by_ident(self):
        outputs = defaultdict(dict)
        for i in range(3):
            ident = str(uuid4())
            for j in range(5000):
                sample = random()
                timestamp = uniform(0, 100000000)
                output = database.write_sample(ident, sample, timestamp)
                outputs[ident][timestamp] = output
        for ident in outputs.keys():
            for sample in database.get_samples_by_ident(ident):
                output = outputs[ident][sample.timestamp]
                self.assertEqual(output.ident, sample.ident)
                self.assertEqual(output.timestamp, sample.timestamp)
                self.assertEqual(output.sample, sample.sample)

    def test_read_by_timestamp(self):
        timestamp = uniform(0, 100000000)
        outputs = defaultdict(dict)
        for i in range(3):
            for j in range(5000):
                ident = str(uuid4())
                sample = random()
                output = database.write_sample(ident, sample, timestamp + i)
                outputs[timestamp + i][ident.encode()] = output
        for i in range(3):
            samples = database.get_samples_by_timestamp(timestamp + i)
            self.assertEqual(len(samples), len(outputs[timestamp + i]))
            for sample in samples:
                self.assertEqual(
                    outputs[timestamp + i][sample.ident].ident, sample.ident
                )
                self.assertEqual(
                    outputs[timestamp + i][sample.ident].timestamp, sample.timestamp
                )
                self.assertEqual(
                    outputs[timestamp + i][sample.ident].sample, sample.sample
                )


if __name__ == "__main__":
    unittest.main()
