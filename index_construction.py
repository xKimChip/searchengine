import os
import json
import math
import pickle
import struct
from bs4 import BeautifulSoup
from collections import defaultdict
from nltk.stem.porter import PorterStemmer
from tokenizer import tokenize
import heapq
import string
import time

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
stem_cache = {}

def stem_token(token):
    if token in stem_cache:
        return stem_cache[token]
    s = stemmer.stem(token)
    stem_cache[token] = s
    return s

def generate_ngrams(tokens, n):
    length = len(tokens)
    if length < n:
        return []
    return ['_'.join(tokens[i:i+n]) for i in range(length - n + 1)]

class Posting:
    __slots__ = ['doc_id', 'count']
    def __init__(self, doc_id, count):
        self.doc_id = doc_id
        self.count = count

def fast_tokenize_and_stem(text):
    raw_tokens = tokenize(text)
    return [stem_token(t) for t in raw_tokens if t.strip()]

def process_document(file_path, doc_id):
    try:
        with open(file_path, 'r', encoding='ascii', errors='replace') as f:
            data = json.load(f)
        url = data.get('url')
        content = data.get('content', '')
        if not url or not content:
            return None, None, None
    except:
        return None, None, None

    soup = BeautifulSoup(content, 'lxml')
    main_text = soup.get_text(separator=' ').lower()
    tokens = fast_tokenize_and_stem(main_text)
    if not tokens:
        return url, None, None

    # Collect weighted tokens for HTML tags
    weighted_tokens_counts = defaultdict(float)
    for tag_name, weight in HTML_WEIGHT_MULTIPLIERS.items():
        for tag in soup.find_all(tag_name):
            tag_text = tag.get_text(separator=' ').lower()
            tag_tokens = fast_tokenize_and_stem(tag_text)
            add_factor = weight - 1.0
            for tt in tag_tokens:
                weighted_tokens_counts[tt] += add_factor

    # Extract anchor texts for indexing into the target pages later
    # This returns a mapping of target_url -> list_of_anchor_tokens
    anchor_map_for_this_doc = defaultdict(list)
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        # anchor text
        anchor_text = a_tag.get_text(separator=' ').lower()
        anchor_tokens = fast_tokenize_and_stem(anchor_text)
        # We also can consider n-grams for anchor text if desired
        # It's optional, but let's do it for completeness
        if len(anchor_tokens) > 1:
            anchor_tokens += generate_ngrams(anchor_tokens, 2)
            if len(anchor_tokens) > 2:
                anchor_tokens += generate_ngrams(anchor_tokens, 3)
        
        # Record them for the target URL
        if anchor_tokens:
            anchor_map_for_this_doc[href].extend(anchor_tokens)

    # Add bigrams and trigrams to main tokens
    bigrams = generate_ngrams(tokens, 2) if len(tokens) > 1 else []
    trigrams = generate_ngrams(tokens, 3) if len(tokens) > 2 else []
    all_tokens = tokens + bigrams + trigrams

    freq = defaultdict(int)
    for t in all_tokens:
        freq[t] += 1

    # Apply weights to unigrams
    for t, add_factor in weighted_tokens_counts.items():
        if t in freq:
            weight = 1.0 + add_factor
            freq[t] = int(freq[t] * weight)

    return url, freq, anchor_map_for_this_doc

def write_partial_index(iIndex, doc_id_map, partial_count):
    partial_filename = os.path.join(OUTPUT_DIR, f'partial_index_{partial_count}.bin')
    with open(partial_filename, 'wb') as f:
        terms = list(iIndex.keys())
        terms.sort()
        for term in terms:
            postings = iIndex[term]
            term_encoded = term.encode('utf-8')
            term_len = len(term_encoded)
            postings_count = len(postings)
            f.write(struct.pack('>H', term_len))
            f.write(term_encoded)
            f.write(struct.pack('>I', postings_count))
            for p in postings:
                f.write(struct.pack('>i', p.doc_id))
                f.write(struct.pack('>d', float(p.count)))
    iIndex.clear()

class PartialIndexReader:
    def __init__(self, file):
        self.file = file

    def read_next_term(self):
        buf = self.file.read(2)
        if not buf or len(buf) < 2:
            return None
        term_len = struct.unpack('>H', buf)[0]
        term_bytes = self.file.read(term_len)
        if len(term_bytes) < term_len:
            return None
        term = term_bytes.decode('utf-8')
        pc_buf = self.file.read(4)
        if len(pc_buf) < 4:
            return None
        postings_count = struct.unpack('>I', pc_buf)[0]
        postings = []
        for _ in range(postings_count):
            doc_id_buf = self.file.read(4)
            count_buf = self.file.read(8)
            if len(doc_id_buf) < 4 or len(count_buf) < 8:
                return None
            doc_id = struct.unpack('>i', doc_id_buf)[0]
            c = struct.unpack('>d', count_buf)[0]
            postings.append((doc_id, c))
        return term, postings

def get_range_file(term):
    first_char = term[0].lower()
    if first_char in string.ascii_lowercase:
        return os.path.join(OUTPUT_DIR, f'inverted_index_{first_char}.bin')
    else:
        return os.path.join(OUTPUT_DIR, 'inverted_index_others.bin')

def get_range_dict_file(first_char):
    if len(first_char) == 1 and first_char in string.ascii_lowercase:
        return os.path.join(OUTPUT_DIR, f'inverted_index_dict_{first_char}.pkl')
    else:
        return os.path.join(OUTPUT_DIR, 'inverted_index_dict_others.pkl')

def merge_partial_indexes(doc_count):
    partial_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith('partial_index_') and f.endswith('.bin')]
    partial_files = [os.path.join(OUTPUT_DIR, pf) for pf in partial_files]
    partial_files.sort()

    file_objs = [open(pf, 'rb') for pf in partial_files]
    readers = [PartialIndexReader(f) for f in file_objs]

    heap = []
    for i, r in enumerate(readers):
        entry = r.read_next_term()
        if entry:
            term, postings = entry
            heapq.heappush(heap, (term, postings, i))

    range_dicts = defaultdict(dict)
    range_file_handles = {}

    def get_file_handle(term):
        rf = get_range_file(term)
        if rf not in range_file_handles:
            range_file_handles[rf] = open(rf, 'wb')
        return range_file_handles[rf]

    current_term = None
    merged_postings = []

    while heap:
        term, postings, rid = heapq.heappop(heap)
        if current_term is None:
            current_term = term
            merged_postings = postings
        elif term == current_term:
            merged_postings.extend(postings)
        else:
            # write out current_term
            df = len(set(p[0] for p in merged_postings))
            idf = math.log(doc_count / df)
            merged_postings.sort(key=lambda x: x[0])
            final_postings = [(doc_id, count * idf) for doc_id, count in merged_postings]

            out_f = get_file_handle(current_term)
            term_enc = current_term.encode('utf-8')
            out_f_start = out_f.tell()
            out_f.write(struct.pack('>H', len(term_enc)))
            out_f.write(term_enc)
            out_f.write(struct.pack('>I', len(final_postings)))
            for doc_id, score in final_postings:
                out_f.write(struct.pack('>i', doc_id))
                out_f.write(struct.pack('>d', score))

            range_file = get_range_file(current_term)
            range_dicts[range_file][current_term] = out_f_start

            current_term = term
            merged_postings = postings

        nxt = readers[rid].read_next_term()
        if nxt:
            heapq.heappush(heap, (nxt[0], nxt[1], rid))

    # last term
    if current_term is not None and merged_postings:
        df = len(set(p[0] for p in merged_postings))
        idf = math.log(doc_count / df)
        merged_postings.sort(key=lambda x: x[0])
        final_postings = [(doc_id, count * idf) for doc_id, count in merged_postings]

        out_f = get_file_handle(current_term)
        term_enc = current_term.encode('utf-8')
        out_f_start = out_f.tell()
        out_f.write(struct.pack('>H', len(term_enc)))
        out_f.write(term_enc)
        out_f.write(struct.pack('>I', len(final_postings)))
        for doc_id, score in final_postings:
            out_f.write(struct.pack('>i', doc_id))
            out_f.write(struct.pack('>d', score))

        range_file = get_range_file(current_term)
        range_dicts[range_file][current_term] = out_f_start

    for f in file_objs:
        f.close()

    for fh in range_file_handles.values():
        fh.close()

    # Write out dictionaries
    for rf, tdict in range_dicts.items():
        basename = os.path.basename(rf)
        part = basename.replace('inverted_index_', '').replace('.bin','')
        dict_file = get_range_dict_file(part)
        with open(dict_file, 'wb') as df:
            pickle.dump(tdict, df)

def main():
    start_time = time.time()
    
    directory_to_process = 'DEV'
    file_paths = []
    for root, dirs, files in os.walk(directory_to_process):
        for filename in files:
            if filename.endswith('.json'):
                file_paths.append(os.path.join(root, filename))

    iIndex = defaultdict(list)
    doc_id_map = []
    doc_id = 0

    # This will store anchor tokens for pages that are linked to, but we don't know their doc_id yet.
    # Structure: {target_url: [list of anchor tokens]}
    anchor_text_map = defaultdict(list)

    for fp in file_paths:
        url, freq, anchor_map_for_this_doc = process_document(fp, doc_id)
        if freq is None:
            continue
        doc_id_map.append(url)

        for term, count in freq.items():
            iIndex[term].append(Posting(doc_id, count))

        # Merge anchor_map_for_this_doc into global anchor_text_map
        # handle doc_id resolution later
        for target_url, tokens in anchor_map_for_this_doc.items():
            anchor_text_map[target_url].extend(tokens)

        doc_id += 1

        # Write partial index periodically if large
        if doc_id > 0 and doc_id % PARTIAL_INDEX_SIZE == 0:
            write_partial_index(iIndex, doc_id_map, doc_id // PARTIAL_INDEX_SIZE - 1)

    # Write last partial index if any
    if iIndex:
        write_partial_index(iIndex, doc_id_map, doc_id // PARTIAL_INDEX_SIZE)

    docmap_file = os.path.join(OUTPUT_DIR, 'doc_id_map.pkl')
    with open(docmap_file, 'wb') as f:
        pickle.dump(doc_id_map, f)

    doc_count = len(doc_id_map)

    # Now integrate anchor text:
    # For each target_url in anchor_text_map, find doc_id and add anchor tokens.
    url_to_id = {u: i for i, u in enumerate(doc_id_map)}
    # iIndex was cleared after writing partial indexes, we need to rebuild it for anchor terms
    # Or we can do anchor indexing in memory and write a separate partial index.
    anchor_iIndex = defaultdict(list)

    for target_url, tokens in anchor_text_map.items():
        if target_url in url_to_id:
            target_doc_id = url_to_id[target_url]
            freq = defaultdict(int)
            for t in tokens:
                freq[t] += 1
            for term, count in freq.items():
                anchor_iIndex[term].append(Posting(target_doc_id, count))

    # Write anchor partial index if it has data
    if anchor_iIndex:
        # We'll treat anchor_iIndex as another partial index and write it out.
        write_partial_index(anchor_iIndex, doc_id_map, doc_id // PARTIAL_INDEX_SIZE + 1)

    # Merge all partial indexes now (both original text and anchor text)
    merge_partial_indexes(doc_count)
    
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Indexing complete in {elapsed:.2f} seconds.")

if __name__ == '__main__':
    main()