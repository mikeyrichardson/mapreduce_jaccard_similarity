from __future__ import division

import random
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol

class EditDistanceOneSentenceFinder(MRJob):
    """
    A script to find similar sentences using jaccard similarity
    and locality sensitive hashing.
    """
    INPUT_PROTOCOL = JSONProtocol

    def hash_half_lines(self, count, sentence):
        words = sentence.split()
        mid = len(words) // 2
        first_half = ' '.join(words[:mid])
        second_half = ' '.join(words[mid:])
        # yield (first_half, 1), (count, sentence)
        # yield (second_half, 2), (count, sentence)
        yield (hash(first_half), 1), (count, sentence)
        yield (hash(second_half), 2), (count, sentence)
        if len(words) % 2 == 1:
            first_half_plus_mid = ' '.join(words[:mid+1])
            second_half_plus_mid = ' '.join(words[mid+1:])
            # yield (first_half_plus_mid, 1), (count, sentence)
            # yield (second_half_plus_mid, 2), (count, sentence)
            yield (hash(first_half_plus_mid), 1), (count, sentence)
            yield (hash(second_half_plus_mid), 2), (count, sentence)

    def count_similar_pairs(self, hash, values):
        values = list(values)
        if len(values) < 2:
            return
        for i, (c1, s1) in enumerate(values):
            for c2, s2 in values[i+1:]:
                if are_similar(s1, s2):
                    # Each sentence represents a set
                    # of duplicate sentences. The duplicate pairs were
                    # counted earlier (in remove_and_count_duplicates)
                    num_pairs = c1 * c2
                    if are_double_counted_pair(s1,s2):
                        yield 'sum', num_pairs / 2
                    else:
                        yield 'sum', num_pairs
    def sum_counts(self, _, counts):
        yield None, sum(counts)

    def steps(self):
        return [MRStep(mapper=self.hash_half_lines,
                       reducer=self.count_similar_pairs),
                MRStep(reducer=self.sum_counts)]


def are_similar(sentence1, sentence2):
    """
    Return true if two sentences have edit distance
    at most 1.
    """
    sentence1 = sentence1.split()
    sentence2 = sentence2.split()
    len1 = len(sentence1)
    len2 = len(sentence2)
    if (abs(len1 - len2) > 1):
        return False
    min_len = min(len1, len2)
    max_len = max(len1, len2)
    i = 0
    while i < min_len:
        if sentence1[i] != sentence2[i]:
            break
        i += 1        
    if i == min_len:
        return True
    j = 1
    while j <= min_len:
        if sentence1[-j] != sentence2[-j]:
            break
        j += 1
    if i + j == max_len:
        return True
    else:
        return False

def are_double_counted_pair(sentence1,sentence2):
    s1 = sentence1.split()
    s2 = sentence2.split()
    l1 = len(s1)
    l2 = len(s2)
    if l1 % 2 == 0 and l2 % 2 == 0:
        return False
    if (l1 + l2) % 2 == 0:
        # the sentences are both of same odd length
        # and will always be double counted
        return True
    # the sentences are different lengths and will be double
    # counted if the deletion occurs in the middle
    # and both ends of the sentence match
    max_len = max(l1, l2)
    mid = max_len // 2
    return s1[:mid] == s2[:mid] and s1[-mid:] == s2[-mid:]

if __name__=="__main__":
    EditDistanceOneSentenceFinder().run()