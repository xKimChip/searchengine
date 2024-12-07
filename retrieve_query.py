import pickle
import os
import sys
from nltk.stem.porter import PorterStemmer
import string
import struct

OUTPUT_DIR = 'index_files'
DOCMAP_FILE = os.path.join(OUTPUT_DIR, 'doc_id_map.pkl')

# Load doc_id_map
with open(DOCMAP_FILE, 'rb') as f:
    doc_id_map = pickle.load(f)

# Identify all dict files
range_files = {}
range_dicts = {}
index_file_handles = {}

for f in os.listdir(OUTPUT_DIR):
    if f.startswith('inverted_index_dict_') and f.endswith('.pkl'):
        full_path = os.path.join(OUTPUT_DIR, f)
        with open(full_path, 'rb') as df:
            d = pickle.load(df)
        # corresponding postings file
        part = f.replace('inverted_index_dict_', '').replace('.pkl','')
        if len(part) == 1 and part in string.ascii_lowercase:
            postings_file = os.path.join(OUTPUT_DIR, f'inverted_index_{part}.bin')
        else:
            postings_file = os.path.join(OUTPUT_DIR, 'inverted_index_others.bin')
        index_file_handles[postings_file] = open(postings_file, 'rb')
        range_dicts[postings_file] = d

def get_range_file(term):
    if not term:
        return os.path.join(OUTPUT_DIR, 'inverted_index_others.bin')
    first_char = term[0].lower()
    if first_char in string.ascii_lowercase:
        return os.path.join(OUTPUT_DIR, f'inverted_index_{first_char}.bin')
    else:
        return os.path.join(OUTPUT_DIR, 'inverted_index_others.bin')

stemmer = PorterStemmer()

def tokenize_query(query):
    tokens = query.lower().split()
    tokens = [stemmer.stem(t) for t in tokens]
    return tokens

def get_postings_for_term(term):
    postings_file = get_range_file(term)
    if postings_file not in range_dicts:
        return []
    tdict = range_dicts[postings_file]
    if term not in tdict:
        return []
    offset = tdict[term]
    f = index_file_handles[postings_file]
    f.seek(offset)
    # Read term info
    term_len_buf = f.read(2)
    if len(term_len_buf) < 2:
        return []
    term_len = struct.unpack('>H', term_len_buf)[0]
    f.seek(term_len, 1)  # skip term bytes
    pc_buf = f.read(4)
    if len(pc_buf) < 4:
        return []
    postings_count = struct.unpack('>I', pc_buf)[0]
    postings = []
    for _ in range(postings_count):
        doc_id_buf = f.read(4)
        tfidf_buf = f.read(8)
        if len(doc_id_buf) < 4 or len(tfidf_buf) < 8:
            break
        doc_id = struct.unpack('>i', doc_id_buf)[0]
        tfidf = struct.unpack('>d', tfidf_buf)[0]
        postings.append((doc_id, tfidf))
    return postings

def query_and(tokens):
    if not tokens:
        return []
    postings_lists = [get_postings_for_term(t) for t in tokens]
    if not all(postings_lists):
        return []

    dicts = []
    for pl in postings_lists:
        d = {p[0]: p[1] for p in pl}
        dicts.append(d)
    common_docs = set(dicts[0].keys())
    for d in dicts[1:]:
        common_docs.intersection_update(d.keys())

    results = []
    for doc_id in common_docs:
        score = sum(d[doc_id] for d in dicts)
        results.append((doc_id, score))

    results.sort(key=lambda x: x[1], reverse=True)
    return results

def main():
    while True:
        query = input("Enter query (type 'exit' to quit): ").strip()
        if query.lower() == 'exit':
            break
        tokens = tokenize_query(query)
        results = query_and(tokens)
        if not results:
            print("No results found.")
        else:
            for doc_id, score in results[:5]:
                print(f"{doc_id_map[doc_id]} (score: {score})")

if __name__ == "__main__":
    main()