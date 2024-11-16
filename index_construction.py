import os
import json
from bs4 import BeautifulSoup
from collections import defaultdict
from multiprocessing import Pool, cpu_count
import math
import pickle

import globals
from tokenizer import tokenize

# Posting class definition


class Posting:
    def __init__(self, doc_id, tf, tf_idf):
        self.doc_id = doc_id
        self.tf = tf
        self.tf_idf = tf_idf

# Function to read JSON file


def read_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='ascii') as f:
            data = json.load(f)
        url = data.get('url')
        content = data.get('content')
        if not url or not content:
            return None, None
        return url, content
    except Exception:
        return None, None

# Function to extract text from HTML content


def extract_text_from_html(html_content):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        # Remove script and style elements
        for script_or_style in soup(['script', 'style']):
            script_or_style.decompose()
        text = soup.get_text(separator=' ')
        return text
    except Exception:
        return ''

# Function to calculate term frequencies


def calculate_term_frequencies(tokens):
    tf_dict = defaultdict(int)
    for token in tokens:
        tf_dict[token] += 1
    return tf_dict

# Function to process a single JSON file and return doc_id and term frequencies


def process_json_file(file_path):
    doc_id, html_content = read_json_file(file_path)
    if not html_content:
        return None
    text = extract_text_from_html(html_content)
    if not text.strip():
        return None
    tokens = tokenize(text)
    if not tokens:
        return None
    term_frequencies = calculate_term_frequencies(tokens)
    return (doc_id, term_frequencies)


resulting_pickle_file_name = 'inverted_index.pkl'
# Main execution block
if __name__ == '__main__':
    inverted_index = defaultdict(list)
    doc_freqs = defaultdict(int)  # Document frequencies
    doc_ids = set()  # Set of unique document IDs

    # Path to the directory you want to process
    directory_to_process = os.path.join('DEV')
    file_paths = []
    if os.path.exists(directory_to_process):
        for root, dirs, files in os.walk(directory_to_process):
            for filename in files:
                if filename.endswith('.json'):
                    file_path = os.path.join(root, filename)
                    file_paths.append(file_path)
    else:
        print(f"The directory {directory_to_process} does not exist.")
        exit(1)

    # Multiprocessing
    num_workers = cpu_count()
    with Pool(num_workers) as pool:
        results = pool.map(process_json_file, file_paths)

    # Collect term frequencies and document frequencies
    doc_term_freqs = {}  # {doc_id: {token: tf}}

    for result in results:
        if result is None:
            continue
        doc_id, term_frequencies = result
        doc_ids.add(doc_id)
        doc_term_freqs[doc_id] = term_frequencies
        for token in term_frequencies.keys():
            doc_freqs[token] += 1  # Increment document frequency for the token

    total_docs = len(doc_ids)  # Total number of documents

    # Calculate idf values
    idf_values = {}
    for token, df in doc_freqs.items():
        idf = math.log(total_docs / df)
        idf_values[token] = idf

    # Build the inverted index with tf-idf scores
    for doc_id, term_frequencies in doc_term_freqs.items():
        for token, tf in term_frequencies.items():
            idf = idf_values[token]
            tf_idf = tf * idf
            posting = Posting(doc_id, tf, tf_idf)
            inverted_index[token].append(posting)

    # Save the inverted index to disk
    with open(resulting_pickle_file_name, 'wb') as f:
        pickle.dump(inverted_index, f)

    # Get the size of the index file in KB
    index_size_kb = os.path.getsize(resulting_pickle_file_name) / 1024

    # Display the analytics
    print("\n=== Index Analytics ===")
    print(f"Number of indexed documents: {total_docs}")
    print(f"Number of unique tokens: {len(inverted_index)}")
    print(f"Total size of the index on disk: {index_size_kb:.2f} KB")
