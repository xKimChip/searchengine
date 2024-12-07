import os
import json
import math
import pickle
from bs4 import BeautifulSoup
from collections import defaultdict
from nltk.stem.porter import PorterStemmer
from tokenizer import tokenize
from multiprocessing import Pool, cpu_count

# CONFIGURATION
NUM_PROCESSES = cpu_count()
CHUNK_SIZE = 1000
PARTIAL_INDEX_SIZE = 50000

OUTPUT_DIR = 'index_files'
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

HTML_WEIGHT_MULTIPLIERS = {
    'title': 3,
    'h1': 2,
    'h2': 1.75,
    'h3': 1.5,
    'b': 1.25,
    'strong': 1.25,
    'a': 1.05,
    'i': 1.05,
    'em': 1.05,
    'h5': 1.05,
    'h6': 1.05,
}

stemmer = PorterStemmer()

class Posting:
    __slots__ = ['doc_id', 'tf']
    def __init__(self, doc_id, tf):
        self.doc_id = doc_id
        self.tf = tf

def calculate_term_frequencies(tokens):
    tf_dict = defaultdict(int)
    total_terms = len(tokens)
    for token in tokens:
        tf_dict[token] += 1
    for term in tf_dict:
        tf_dict[term] = tf_dict[term]
    return tf_dict

def apply_html_weight(soup, term_frequencies):
    # Apply HTML weights
    for tag_name, weight in HTML_WEIGHT_MULTIPLIERS.items():
        for tag in soup.find_all(tag_name):
            subtokens = tokenize(tag.get_text(separator=' ').lower())
            subtokens = [stemmer.stem(t) for t in subtokens if t.strip()]
            for t in subtokens:
                if t in term_frequencies:
                    term_frequencies[t] *= weight

def process_document(file_path, doc_id):
    try:
        with open(file_path, 'r', encoding='ascii', errors='replace') as f:
            data = json.load(f)
        url = data.get('url')
        content = data.get('content', '')
        if not url or not content:
            return None, None
    except:
        return None, None

    soup = BeautifulSoup(content, 'lxml')
    text = soup.get_text(separator=' ').lower()
    tokens = tokenize(text)
    tokens = [stemmer.stem(t) for t in tokens if t.strip()]

    if not tokens:
        return None, None

    term_frequencies = calculate_term_frequencies(tokens)
    apply_html_weight(soup, term_frequencies)
    return url, term_frequencies

def process_chunk(args):
    """Process a chunk of file paths in a separate process."""
    file_paths, start_doc_id = args
    iIndex = defaultdict(list)
    doc_id_map = []
    doc_id = start_doc_id

    for fp in file_paths:
        url, tf_dict = process_document(fp, doc_id)
        if not tf_dict:
            doc_id += 1
            continue
        doc_id_map.append(url)
        for term, tf in tf_dict.items():
            iIndex[term].append(Posting(doc_id, tf))
        doc_id += 1

    # Return the partial index and the doc_id_map for this chunk
    return iIndex, doc_id_map

def write_partial_index(iIndex, doc_id_map, partial_count):
    partial_filename = os.path.join(OUTPUT_DIR, f'partial_index_{partial_count}.pkl')
    with open(partial_filename, 'wb') as f:
        pickle.dump((iIndex, doc_id_map), f)

def merge_partial_indexes():
    # Merge all partial indexes
    partial_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith('partial_index_') and f.endswith('.pkl')]
    partial_files.sort()

    global_doc_id_map = []
    term_postings_map = defaultdict(list)

    for pfile in partial_files:
        full_path = os.path.join(OUTPUT_DIR, pfile)
        with open(full_path, 'rb') as pf:
            iIndex, local_doc_id_map = pickle.load(pf)
        start_id = len(global_doc_id_map)
        global_doc_id_map.extend(local_doc_id_map)
        # Append postings
        for term, postings in iIndex.items():
            term_postings_map[term].extend(postings)

    # Compute DF and IDF
    doc_count = len(global_doc_id_map)
    df_map = {term: len(set(p.doc_id for p in postings)) for term, postings in term_postings_map.items()}
    idf_map = {term: math.log(doc_count / df) for term, df in df_map.items()}

    final_index = {}
    for term, postings in term_postings_map.items():
        idf = idf_map[term]
        new_postings = []
        for p in postings:
            tf_idf = p.tf * idf
            new_postings.append((p.doc_id, tf_idf))
        new_postings.sort(key=lambda x: x[0])
        final_index[term] = new_postings

    return final_index, global_doc_id_map

def write_final_index(final_index, doc_id_map):
    postings_file = os.path.join(OUTPUT_DIR, 'inverted_index_postings.txt')
    dict_file = os.path.join(OUTPUT_DIR, 'inverted_index_dict.pkl')
    docmap_file = os.path.join(OUTPUT_DIR, 'doc_id_map.pkl')

    with open(docmap_file, 'wb') as f:
        pickle.dump(doc_id_map, f)

    terms = sorted(final_index.keys())

    with open(postings_file, 'wb') as pf:
        term_dict = {}
        for term in terms:
            start_offset = pf.tell()
            postings_str = " ".join(f"{doc_id}:{tf_idf:.6f}" for doc_id, tf_idf in final_index[term])
            postings_bytes = postings_str.encode('utf-8')
            length = len(postings_bytes)
            length_bytes = f"{length}\n".encode('utf-8')
            pf.write(length_bytes)
            pf.write(postings_bytes)
            pf.write(b"\n")
            term_dict[term] = start_offset

    with open(dict_file, 'wb') as f:
        pickle.dump(term_dict, f)

def main():
    directory_to_process = 'DEV'
    file_paths = []
    for root, dirs, files in os.walk(directory_to_process):
        for filename in files:
            if filename.endswith('.json'):
                file_paths.append(os.path.join(root, filename))

    file_paths.sort()

    # Split file_paths into chunks
    chunks = [file_paths[i:i+CHUNK_SIZE] for i in range(0, len(file_paths), CHUNK_SIZE)]

    # Maintain a running doc_id start for each chunk
    start_ids = []
    running_doc_id = 0
    for c in chunks:
        start_ids.append(running_doc_id)
        running_doc_id += len(c)

    # Prepare arguments for pool
    pool_args = [(c, sid) for c, sid in zip(chunks, start_ids)]

    iIndex_merged = defaultdict(list)
    doc_id_map = []
    partial_count = 0

    # Use multiprocessing pool to process chunks
    with Pool(NUM_PROCESSES) as pool:
        results = pool.map(process_chunk, pool_args)

    # Write each chunk as a separate partial index file
    for iIndex_chunk, doc_ids_chunk in results:
        write_partial_index(iIndex_chunk, doc_ids_chunk, partial_count)
        partial_count += 1

    # Merge all partial indexes
    final_index, global_doc_id_map = merge_partial_indexes()

    # Write final index
    write_final_index(final_index, global_doc_id_map)
    print("Indexing complete")

if __name__ == '__main__':
    main()