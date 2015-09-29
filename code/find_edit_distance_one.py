from __future__ import division

import random
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONProtocol

class EditDistanceOneSentenceFinder(MRJob):
    """
    A script to find and count number of pairs of sentences that
    have an edit distance of 1 word. This script assumes the sentences
    have been preprocessed to count and remove duplicate sentences.
    """
    INPUT_PROTOCOL = JSONProtocol

    def emit_half_lines(self, count, sentence):
        words = sentence.split()
        mid = len(words) // 2
        first_half = ' '.join(words[:mid])
        second_half = ' '.join(words[mid:])
        yield (first_half, 1), (count, second_half)
        yield (second_half, 2), (count, first_half)
        if len(words) % 2 == 1:
            first_half_plus_mid = ' '.join(words[:mid+1])
            second_half_plus_mid = ' '.join(words[mid+1:])
            yield (first_half_plus_mid, 1), (count, second_half_plus_mid)
            yield (second_half_plus_mid, 2), (count, first_half_plus_mid)

    def count_similar_pairs(self, key, values):
        half, part = key
        if part == 1:
            sentences = [(count, half + ' ' + half2) for (count, half2) in values]
        else:
            sentences = [(count, half1 + ' ' + half) for (count, half1) in values]
        for i, (c1, s1) in enumerate(sentences):
            for c2, s2 in sentences[i+1:]:
                if are_similar(s1, s2):
                    num_pairs = c1 * c2
                    if are_double_counted_pair(s1,s2):
                        yield 'sum', num_pairs / 2
                    else:
                        yield 'sum', num_pairs

    def sum_counts(self, _, counts):
        yield "Total number of edit distance one pairs", sum(counts)

    def steps(self):
        return [MRStep(mapper=self.emit_half_lines,
                       reducer=self.count_similar_pairs),
                MRStep(reducer=self.sum_counts)]


def are_similar(sentence1, sentence2):
    """
    Return True if two sentences have edit distance
    at most 1.
    """
    words1 = sentence1.split()
    words2 = sentence2.split()
    len1 = len(words1)
    len2 = len(words2)
    length_difference = abs(len1 - len2)
    if (length_difference > 1):
        return False
    if length_difference == 0:
        diff_count = 0
        for w1, w2 in zip(words1, words2):
            if w1 != w2:
                diff_count += 1
                if diff_count > 1:
                    return False
        return True

    if len1 > len2:
        words_longer = words1
        words_shorter = words2
    else:
        words_longer = words2
        words_shorter = words1
    i = 0
    while i < len(words_shorter):
        if words1[i] != words2[i]:
            break
        i += 1        
    return words_shorter[i:] == words_longer[i+1:]

def are_double_counted_pair(sentence1,sentence2):
    s1 = sentence1.split()
    s2 = sentence2.split()
    l1 = len(s1)
    l2 = len(s2)
    if l1 % 2 == 0 and l2 % 2 == 0:
        return False
    if (l1 + l2) % 2 == 0:
        # the sentences both have odd length
        # and will always be double counted
        return True
    # the sentences are different lengths and will be double
    # counted if the deletion occurs in the middle
    # and both ends of the sentence are the same
    max_len = max(l1, l2)
    mid = max_len // 2
    return s1[:mid] == s2[:mid] and s1[-mid:] == s2[-mid:]

if __name__=="__main__":
    EditDistanceOneSentenceFinder().run()