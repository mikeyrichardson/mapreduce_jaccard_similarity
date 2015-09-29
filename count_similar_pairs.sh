#!/bin/bash

python ./code/find_duplicate_sentences.py ./input/sentences.txt > ./input/dups.txt
python ./code/count_duplicate_pairs.py ./input/dups.txt > ./results/dup_pair_count.txt
python ./code/find_edit_distance_one.py ./input/dups.txt > ./results/sim_pair_count.txt
