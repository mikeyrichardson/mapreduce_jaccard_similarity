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

    def hash_lines(self, _, line):
        words = line.split()
        sentence = ' '.join(words[1:])
        yield (hash(sentence), sentence[:5]), sentence

    def remove_and_count_duplicates(self, hash, sentences):
        sentence = None
        sentence_count = 0
        for s in sentences:
            if not sentence:
                sentence = s
            sentence_count += 1
        yield sentence_count, sentence

    def steps(self):
        return [MRStep(mapper=self.hash_lines,
                       reducer=self.remove_and_count_duplicates)]


if __name__=="__main__":
    DuplicateSentenceFinder().run()