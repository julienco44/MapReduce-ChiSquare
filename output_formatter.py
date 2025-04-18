from mrjob.job import MRJob
from mrjob.step import MRStep

class OutputFormatter(MRJob):
    def mapper(self, key, value):
        if key == "MERGED_DICT":
            # Pass through terms for the merged dictionary
            yield "MERGED_DICT", value
        else:
            # This is a category with its top terms
            category = key
            top_terms = value
            
            # Format the output string
            formatted = f"{category} " + " ".join([f"{term}:{score}" for term, score in top_terms])
            
            # Emit with category as the key for sorting
            yield "CATEGORIES", (category, formatted)
    
    def reducer(self, key, values):
        if key == "MERGED_DICT":
            # Collect all unique terms
            unique_terms = set()
            for term_list in values:
                unique_terms.update(term_list)
            
            # Sort alphabetically and join
            merged_dict = " ".join(sorted(unique_terms))
            
            # Using a high ASCII value to ensure this comes after all categories
            yield "~MERGED_DICT", merged_dict
        else:  # key == "CATEGORIES"
            # Sort categories alphabetically
            sorted_categories = sorted(values, key=lambda x: x[0])
            
            # Emit each category's formatted string
            for _, formatted in sorted_categories:
                yield None, formatted

if __name__ == '__main__':
    OutputFormatter.run()
