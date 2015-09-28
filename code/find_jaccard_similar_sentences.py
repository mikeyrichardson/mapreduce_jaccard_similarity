from __future__ import division

import random
import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawProtocol

class SimilarSentenceFinder(MRJob):
    """
    A script to find similar sentences using jaccard similarity
    and locality sensitive hashing.
    """
    OUTPUT_PROTOCOL = RawProtocol

    def __init__(self, num_minhash_functions=100, num_bands=10, 
                 shingle_length=5, seed=None, **kwargs):
        super(SimilarSentenceFinder, self).__init__(**kwargs)
        assert num_minhash_functions % num_bands == 0, \
            "num_minhash_functions must be divisible by num_bands"
        assert shingle_length > 0, "shingle_length must be greater than 0"
        self.num_minhash_functions = num_minhash_functions
        self.num_bands = num_bands
        self.shingle_length = shingle_length
        if seed:
            self.seed = seed
        else:
            self.seed = random.random()

    def get_minhash_signature(self, shingles):
        """
        Convert a list of shingles to a minhash signature.
        """
        random.seed(self.seed)
        signature = []
        for _ in xrange(self.num_minhash_functions):
            minhash = sys.maxint
            random_int = random.randint(0, 0xffffffff)
            for shingle in shingles:
                current_hashed_shingle = \
                    (hash(shingle) & 0xffffffff) ^ random_int
                if current_hashed_shingle < minhash:
                    minhash = current_hashed_shingle
            signature.append(minhash)
        return signature

    def create_lsh(self, _, line):
        """
        For each sentence create shingles consisting of 
        self.shingle_length words, generate a minhash signature,
        split the signature into bands, and then
        emit the band number and band contents to use as a 
        locality sensitive hash (along with the id of the sentence).
        """
        words = line.split()
        id = words[0]
        shingles = []
        for i in xrange(1, len(words) - self.shingle_length):
            shingle = ' '.join(words[i:i + self.shingle_length])
            shingles.append(shingle)
        minhash_signature = self.get_minhash_signature(shingles)
        num_rows_per_band = len(minhash_signature) // self.num_bands
        for band in xrange(self.num_bands):
            start = band * num_rows_per_band
            end = (band + 1) * num_rows_per_band
            band_contents = ' '.join(minhash_signature[start:end])
            band_hash = hash(band_contents) & 0xffffffff
            yield (band, band_hash), id

    def find_similar_pairs(self, lsh, ids):
        """
        If the lsh corresponds to more than one id, 
        emit all possible pairs of those ids.
        """
        id_list = list(ids)
        if len(id_list) < 2:
            return
        for id1 in id_list:
            for id2 in id_list:
                if id1 < id2:
                    yield (id1, id2), 1

    def remove_duplicates(self, ids, values):
        yield str(ids[0]), str(ids[1])

    def steps(self):
        return [MRStep(mapper=self.create_lsh,
                       reducer=self.find_similar_pairs),
                MRStep(reducer=self.remove_duplicates)]


if __name__=="__main__":
    SimilarSentenceFinder().run()

