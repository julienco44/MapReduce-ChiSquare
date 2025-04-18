from mrjob.job import MRJob
from mrjob.step import MRStep

class TopTermsSelector(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_reformat_for_sorting,
                reducer=self.reducer_collect_top_terms
            ),
            MRStep(
                mapper=self.mapper_prepare_output,
                reducer=self.reducer_format_output
            )
        ]
    
    def mapper_reformat_for_sorting(self, key, value):
        # Input: "{category}|{term}" -> chi_square
        category, term = key.split("|")
        chi_square = float(value)
        
        # Emit a composite key for secondary sort:
        # (category, -chi_square) as the key, and term as the value
        # Using negative chi_square to sort in descending order
        yield (category, -chi_square), term
    
    def reducer_collect_top_terms(self, composite_key, terms):
        category, neg_chi_square = composite_key
        chi_square = -neg_chi_square  # Convert back to positive
        
        # Keep track of how many terms we've seen for this category
        count = 0
        
        # Because of the secondary sort, terms are already sorted by chi-square in descending order
        for term in terms:
            # Only take the top 75 terms
            if count < 75:
                # Emit for final output formatting
                yield category, (term, chi_square)
                # Also emit for the merged dictionary
                yield "ALL_TERMS", term
            else:
                # We've already collected 75 terms, so we can stop
                break
            count += 1
    
    def mapper_prepare_output(self, key, value):
        # Just pass through
        yield key, value
    
    def reducer_format_output(self, key, values):
        if key == "ALL_TERMS":
            # Collect all unique terms for the merged dictionary
            unique_terms = set(values)
            yield "MERGED_DICT", sorted(unique_terms)
        else:
            # This is a category
            # Collect the top terms with their scores
            category = key
            top_terms = list(values)
            yield category, top_terms

if __name__ == '__main__':
    TopTermsSelector.run()
