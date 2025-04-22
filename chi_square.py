#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.protocol
import mrjob
import logging
from timeit import default_timer as timer
import itertools
import re
import json
from collections import defaultdict
import heapq
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChiSquareJob(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol
    
    def configure_args(self):
        super(ChiSquareJob, self).configure_args()
        self.add_file_arg("--stopwords", help="path to stopwords file")
    
    def mapper_init(self):
        self.stopwords = set()
        with open(self.options.stopwords, "r") as f:
            self.stopwords = set(line.strip() for line in f)
    
    def mapper(self, _, line):
        json_dict = json.loads(line)
        review_text = json_dict["reviewText"]
        category = json_dict["category"]
        
        # tokenization, case folding, stopword removal
        terms = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'~#@&*%€$§\/]+', review_text)
        terms = set(terms) - self.stopwords
        terms = [t.lower() for t in terms if len(t) > 1]
        
        for term in terms:
            yield (term, category), 1
        
        # each line corresponds to exactly one category
        yield (None, category), 1
    
    def combiner(self, key, counts):
        yield None, (key, sum(counts))
    
    def reducer(self, _, key_count):
        N = 0
        cat_count = defaultdict(int)
        term_count = defaultdict(int)
        term_cat_count = defaultdict(int)
        
        for key, count in key_count:
            term, cat = key
            if term is None:
                N += count
                cat_count[cat] = count
            else:
                term_count[term] += count
                term_cat_count[(term, cat)] = count
        
        # 1. calculate chi2 of all terms for each category
        # can't merge with previous loop because we need to calculate N first
        chi2_cat_term = {}
        for (term, cat) in term_cat_count:
            A = term_cat_count[(term, cat)]
            B = term_count[term] - A
            C = cat_count[cat] - A
            D = N - A - B - C
            
            # Avoid division by zero
            denominator = (A + B) * (A + C) * (B + D) * (C + D)
            if denominator == 0:
                chi2 = 0
            else:
                chi2 = (N * (A * D - B * C) ** 2) / denominator
                
            if cat not in chi2_cat_term:
                chi2_cat_term[cat] = {}
            chi2_cat_term[cat][term] = chi2
        
        # 2. sort chi2 keys alphabetically
        chi2_cat_term = dict(sorted(chi2_cat_term.items(), key=lambda x: x[0]))
        
        # 3. sort chi2 values and get top 75
        for cat, terms in chi2_cat_term.items():
            chi2_cat_term[cat] = dict(heapq.nlargest(75, terms.items(), key=lambda x: x[1]))
            if not chi2_cat_term[cat]:
                del chi2_cat_term[cat]
        
        # 4. yield results
        # <category name> term1:chi2 term2:chi2 ... term75:chi2
        for cat, terms in chi2_cat_term.items():
            yield None, str(cat) + " " + " ".join(f"{term}:{chi2}" for term, chi2 in terms.items())
        
        # 5. all terms space-separated and ordered alphabetically
        all_terms = set()
        for terms_dict in chi2_cat_term.values():
            all_terms.update(terms_dict.keys())
        
        yield None, " ".join(sorted(all_terms))
    
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer,
            ),
        ]

if __name__ == "__main__":
    t1 = timer()
    ChiSquareJob.run()
    t2 = timer()
    runtime = t2 - t1
    logger.info(f"time: {runtime:.2f}s")
