from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import os

class DocumentAndTermCounter(MRJob):
    def configure_args(self):
        super(DocumentAndTermCounter, self).configure_args()
        self.add_file_arg('--stopwords', help='Path to stopwords file')
    
    def mapper_init(self):
        # Load stopwords from file
        self.stopwords = set()
        with open(self.options.stopwords, 'r') as f:
            for line in f:
                self.stopwords.add(line.strip().lower())
    
    def mapper(self, _, line):
        try:
            # Parse the JSON line
            review = json.loads(line)
            category = review['category']
            
            # Combine review text and summary for processing
            text = (review['reviewText'] + ' ' + review['summary']).lower()  # Case folding
            
            # Tokenization with specified delimiters
            delimiters = r'[\s\t\d\(\)\[\]\{\}\.\!\?\,\;\:\+\=\-\_\"\'`\~\#\@\&\*\%\€\$\§\\\/]+'
            tokens = re.split(delimiters, text)
            
            # Filter stopwords and single-character tokens
            filtered_tokens = [token for token in tokens if token and token not in self.stopwords and len(token) > 1]
            
            # Use set to count each term only once per document
            unique_tokens = set(filtered_tokens)
            
            # Emit total document count
            yield "TOTAL_DOCS", 1
            
            # Emit category document count
            yield f"CAT_DOCS|{category}", 1
            
            # Emit term counts per category (each term counts once per document)
            for token in unique_tokens:
                yield f"TERM|{token}|{category}", 1
                # Also track total documents with this term across all categories
                yield f"TERM_TOTAL|{token}", 1
                
        except json.JSONDecodeError:
            # Skip malformed JSON
            pass
    
    def combiner(self, key, values):
        # Sum counts locally before sending to reducer
        yield key, sum(values)
    
    def reducer(self, key, values):
        # Sum all counts for this key
        count = sum(values)
        yield key, count

if __name__ == '__main__':
    DocumentAndTermCounter.run()
