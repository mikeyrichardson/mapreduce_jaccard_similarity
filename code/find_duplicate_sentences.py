from __future__ import division

import random
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol
    
class DuplicateSentenceFinder(MRJob):
    """
    A script to find and count duplicate sentences.
    """

    def extract_sentences(self, _, line):
        words = line.split()
        sentence = ' '.join(words[1:])
        yield sentence, 1

    def remove_and_count_duplicates(self, sentence, counts):
        yield sum(counts), sentence

    def steps(self):
        return [MRStep(mapper=self.extract_sentences,
                       reducer=self.remove_and_count_duplicates)]


if __name__=="__main__":
    DuplicateSentenceFinder().run()