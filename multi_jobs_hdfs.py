#!/usr/bin/env python3
"""
Multi-job implementation of chi-square analysis for Hadoop
Modified to work with HDFS files
"""
import os
import json
import re
import heapq
import logging
import subprocess
from timeit import default_timer as timer
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_stopwords(filepath):
    """Load stopwords from local file"""
    with open(filepath, 'r') as f:
        return set(line.strip().lower() for line in f)

def read_hdfs_file(hdfs_path):
    """Read a file from HDFS using Hadoop command"""
    logger.info(f"Reading HDFS file: {hdfs_path}")
    cmd = ["hadoop", "fs", "-cat", hdfs_path]
    logger.info(f"Running command: {' '.join(cmd)}")
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    output, error = process.communicate()
    
    if process.returncode != 0:
        logger.error(f"Error reading HDFS file: {error}")
        raise Exception(f"Failed to read HDFS file: {error}")
    
    # Split the output into lines
    return output.splitlines()

def process_reviews(input_file, stopwords):
    """
    Job 1: Process reviews and count documents and terms
    Input file can be HDFS path
    Returns: document counts, term counts, and term-category counts
    """
    logger.info("Job 1: Processing reviews and counting documents and terms")
    
    total_docs = 0
    category_docs = defaultdict(int)  # Category -> doc count
    term_counts = defaultdict(int)    # Term -> doc count
    term_cat_counts = defaultdict(int)  # (Term, Category) -> doc count
    
    # Determine if the input is an HDFS path
    if input_file.startswith("hdfs://"):
        lines = read_hdfs_file(input_file)
    else:
        # Try local file
        try:
            with open(input_file, 'r') as f:
                lines = f.readlines()
        except FileNotFoundError:
            # Maybe it's an HDFS path without the 'hdfs://' prefix
            lines = read_hdfs_file(input_file)
    
    for line in lines:
        try:
            review = json.loads(line)
            category = review.get('category', '')
            text = review.get('reviewText', '')
            
            if not category or not text:
                continue
            
            # Tokenize and process text
            terms = re.split(r'[ \t\d()\[\]{}.!?,;:+=\-_"\'`~#@&*%€$§\/]+', text.lower())
            terms = [t for t in terms if t and t not in stopwords and len(t) > 1]
            
            # Count unique terms in this document
            unique_terms = set(terms)
            
            # Update counts
            total_docs += 1
            category_docs[category] += 1
            
            for term in unique_terms:
                term_counts[term] += 1
                term_cat_counts[(term, category)] += 1
            
            # Log progress for large datasets
            if total_docs % 10000 == 0:
                logger.info(f"Processed {total_docs} documents...")
                
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON in line: {line[:100]}...")
            continue
    
    logger.info(f"Processed {total_docs} documents across {len(category_docs)} categories")
    logger.info(f"Found {len(term_counts)} unique terms and {len(term_cat_counts)} term-category pairs")
    
    return total_docs, category_docs, term_counts, term_cat_counts

def calculate_chi_square(total_docs, category_docs, term_counts, term_cat_counts):
    """
    Job 2: Calculate chi-square values for each term-category pair
    Returns: Dictionary of {category -> {term -> chi_square}}
    """
    logger.info("Job 2: Calculating chi-square values")
    
    chi_square_values = defaultdict(dict)
    count = 0
    total_pairs = len(term_cat_counts)
    
    for (term, category), count_a in term_cat_counts.items():
        # A = docs in category with term
        a = count_a
        
        # B = docs not in category with term
        b = term_counts[term] - a
        
        # C = docs in category without term
        c = category_docs[category] - a
        
        # D = docs not in category without term
        d = total_docs - a - b - c
        
        # Calculate chi-square
        denominator = (a + b) * (a + c) * (b + d) * (c + d)
        if denominator == 0:
            chi_square = 0
        else:
            chi_square = (total_docs * ((a * d - b * c) ** 2)) / denominator
        
        chi_square_values[category][term] = chi_square
        
        # Log progress
        count += 1
        if count % 100000 == 0:
            logger.info(f"Calculated {count}/{total_pairs} chi-square values ({count/total_pairs*100:.1f}%)")
    
    logger.info(f"Calculated chi-square values for {len(term_cat_counts)} term-category pairs")
    return chi_square_values

def select_top_terms(chi_square_values, top_n=75):
    """
    Job 3: Select top N terms for each category based on chi-square values
    Returns: Dictionary of {category -> [(term, chi_square), ...]}
    """
    logger.info(f"Job 3: Selecting top {top_n} terms per category")
    
    top_terms = {}
    merged_dict = set()
    
    for category, terms in chi_square_values.items():
        # Get top N terms for this category
        category_top_terms = heapq.nlargest(top_n, terms.items(), key=lambda x: x[1])
        top_terms[category] = category_top_terms
        
        # Add terms to merged dictionary
        merged_dict.update(term for term, _ in category_top_terms)
    
    logger.info(f"Selected {sum(len(terms) for terms in top_terms.values())} top terms")
    logger.info(f"Merged dictionary has {len(merged_dict)} terms")
    
    return top_terms, sorted(merged_dict)

def format_output(top_terms, merged_dict):
    """
    Job 4: Format the final output
    Returns: List of output lines
    """
    logger.info("Job 4: Formatting output")
    
    output_lines = []
    
    # Add category lines in alphabetical order
    for category in sorted(top_terms.keys()):
        terms = top_terms[category]
        formatted = f"{category} " + " ".join([f"{term}:{chi_square}" for term, chi_square in terms])
        output_lines.append(formatted)
    
    # Add merged dictionary line
    output_lines.append(" ".join(merged_dict))
    
    return output_lines

def run_multi_job_pipeline(input_file, stopwords_file, output_file):
    """Run the entire multi-job pipeline and write output to file"""
    logger.info(f"Starting multi-job pipeline on {input_file}")
    
    try:
        # Load stopwords
        stopwords = load_stopwords(stopwords_file)
        logger.info(f"Loaded {len(stopwords)} stopwords")
        
        # Job 1: Process reviews and count documents
        total_docs, category_docs, term_counts, term_cat_counts = process_reviews(input_file, stopwords)
        
        # Job 2: Calculate chi-square values
        chi_square_values = calculate_chi_square(total_docs, category_docs, term_counts, term_cat_counts)
        
        # Job 3: Select top terms per category
        top_terms, merged_dict = select_top_terms(chi_square_values)
        
        # Job 4: Format output
        output_lines = format_output(top_terms, merged_dict)
        
        # Write output to file
        with open(output_file, 'w') as f:
            for line in output_lines:
                f.write(line + '\n')
        
        logger.info(f"Results written to {output_file}")
    
    except Exception as e:
        logger.error(f"Error in multi-job pipeline: {e}")
        raise

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <input_file> <stopwords_file> <output_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    stopwords_file = sys.argv[2]
    output_file = sys.argv[3]
    
    t1 = timer()
    run_multi_job_pipeline(input_file, stopwords_file, output_file)
    t2 = timer()
    runtime = t2 - t1
    logger.info(f"Multi-job pipeline runtime: {runtime:.2f}s")