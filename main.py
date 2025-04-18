#!/usr/bin/env python3
import os
import sys
import subprocess

def run_job(job_class, input_path, output_path, args=None):
    """Run a MapReduce job with the given parameters."""
    if args is None:
        args = []
    
    cmd = [
        'python3', 
        f'{job_class}.py', 
        '--runner=hadoop',
        f'--input={input_path}',
        f'--output={output_path}'
    ] + args
    
    print(f"Running command: {' '.join(cmd)}")
    subprocess.check_call(cmd)

def main():
    # Parse command line arguments
    if len(sys.argv) < 3:
        print("Usage: python main.py <input_path> <output_dir> <stopwords_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_dir = sys.argv[2]
    stopwords_path = sys.argv[3]
    
    # Create output directories
    os.makedirs(output_dir, exist_ok=True)
    
    # Define intermediate and final paths
    counts_output = os.path.join(output_dir, "1_counts")
    chisquare_output = os.path.join(output_dir, "2_chisquare")
    topterms_output = os.path.join(output_dir, "3_topterms")
    final_output = os.path.join(output_dir, "4_final")
    
    # 1. Run document and term counter
    run_job(
        "document_term_counter", 
        input_path, 
        counts_output, 
        [f"--stopwords={stopwords_path}"]
    )
    
    # 2. Run chi-square calculator
    run_job(
        "chisquare_calculator", 
        counts_output + "/part-*", 
        chisquare_output, 
        [f"--counts={counts_output}/part-00000"]
    )
    
    # 3. Run top terms selector
    run_job(
        "top_terms_selector", 
        chisquare_output + "/part-*", 
        topterms_output
    )
    
    # 4. Run output formatter
    run_job(
        "output_formatter", 
        topterms_output + "/part-*", 
        final_output
    )
    
    # 5. Combine parts into final output.txt
    os.system(f"hdfs dfs -getmerge {final_output}/part-* output.txt")
    print("Pipeline completed successfully. Results saved to output.txt")

if __name__ == "__main__":
    main()
