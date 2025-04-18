from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import math

class ChiSquareCalculator(MRJob):
    def configure_args(self):
        super(ChiSquareCalculator, self).configure_args()
        self.add_file_arg('--counts', help='Path to the counts file from Job 1')
    
    def mapper_init(self):
        # Load counts from previous job
        self.total_docs = 0
        self.category_docs = {}
        self.term_totals = {}
        
        with open(self.options.counts, 'r') as f:
            for line in f:
                key, value = line.strip().split('\t')
                value = int(value)
                
                if key == "TOTAL_DOCS":
                    self.total_docs = value
                elif key.startswith("CAT_DOCS|"):
                    category = key.split("|")[1]
                    self.category_docs[category] = value
                elif key.startswith("TERM_TOTAL|"):
                    term = key.split("|")[1]
                    self.term_totals[term] = value
    
    def mapper(self, key, value):
        # We only process term-category counts here
        if key.startswith("TERM|"):
            _, term, category = key.split("|")
            count = int(value)
            
            # Verify we have all the data we need
            if term in self.term_totals and category in self.category_docs:
                # A = count (documents with term in this category)
                A = count
                
                # B = documents with term NOT in this category
                B = self.term_totals[term] - A
                
                # C = documents in this category WITHOUT this term
                C = self.category_docs[category] - A
                
                # D = documents NOT in this category and WITHOUT this term
                D = self.total_docs - A - B - C
                
                # Calculate chi-square
                chi_square = self.calculate_chi_square(A, B, C, D)
                
                # Emit (category, term) -> chi_square
                yield f"{category}|{term}", chi_square
    
    def calculate_chi_square(self, A, B, C, D):
        N = A + B + C + D
        
        # Avoid division by zero
        if N == 0 or (A+B) == 0 or (A+C) == 0 or (B+D) == 0 or (C+D) == 0:
            return 0
            
        numerator = N * ((A * D - B * C) ** 2)
        denominator = (A + B) * (A + C) * (B + D) * (C + D)
        
        return numerator / denominator
    
    def reducer(self, key, values):
        # Just pass through the calculated chi-square values
        # In case we got multiple values for the same key (shouldn't happen), take the sum
        category, term = key.split("|")
        chi_square = sum(values)
        yield f"{category}|{term}", chi_square

if __name__ == '__main__':
    ChiSquareCalculator.run()
