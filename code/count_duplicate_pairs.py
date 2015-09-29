from __future__ import division

import random
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol
    
class DuplicatePairCounter(MRJob):
    """
    A script to count the number of pairs of duplicate sentences.
    """

    INPUT_PROTOCOL = JSONProtocol

    def get_num_pairs_of_duplicates(self, count, sentence):
        count = int(count)
        yield 'sum', count * (count - 1) // 2

    def sum_pairs(self, _, num_pairs):
        yield 'Total number of pairs', sum(num_pairs)


    def steps(self):
        return [MRStep(mapper=self.get_num_pairs_of_duplicates,
                       reducer=self.sum_pairs)]


if __name__=="__main__":
    DuplicatePairCounter().run()