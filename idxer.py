import os, sys, re
import json
from bs4 import BeautifulSoup
from collections import OrderedDict, defaultdict
from multiprocessing import Pool, cpu_count
import math
import pickle
from nltk import WordNetLemmatizer
from helpers import *
#import globals
from tokenizer import tokenize

MAX_INDEX_SIZE = 100000000 # Should be about 200MB
partialidx_count = 1

resulting_txt_IOI = 'index_index.txt'
resulting_pickle_IOI = 'index_index.pkl'

PICKLE = True


#Posting class definition
class Posting:
    def __init__(self, doc_id, tf, weight):
    #def __init__(self, doc_id: set, tf, tf_idf, weight):
        self.doc_id = doc_id
        self.tf = tf
        self.weight = weight
        self.tf_idf = 0

    def __repr__(self) -> str:
        return f'{self.doc_id} {self.tf} {self.weight} {self.tf_idf}'

    def __str__(self) -> str:
         return f'{self.doc_id} {self.tf} {self.weight} {self.tf_idf}'

    def __eq__(self, other):
        return self.doc_id == other.doc_id

    def __hash__(self):
        return hash(self.doc_id)
    
    def update_tfidf(self, tfidf_score):
        self.tf_idf = tfidf_score
        
    def calculate_tfidf(self, idf_score):
        tfidf_score = self.tf * idf_score
        self.update_tfidf(tfidf_score)


iIndex = defaultdict(list)
doc_id_map = defaultdict()
doc_count = 0

    
    
    
# Function to read JSON file
def read_json_file(file_path):
    global doc_count
    try:
        with open(file_path, 'r', encoding='ascii') as f:
            data = json.load(f)
        url = data.get('url')
        content = data.get('content')
        # For multithreading, might put this under the html check and add a lock
        doc_id = doc_count
        doc_count += 1
        
        #url = data.at_pointer(b'/url').decode()
        #content = data.at_pointer(b'/content').decode()

        
        if not url or not content:
            return None, None
        return url, content, doc_id
    except Exception:
        return None, None

# Function to extract text from HTML content




# Function to process a single JSON file and return doc_id and term frequencies

def process_json_file(file_path):
    
    doc_url, html_content, cur_doc_id = read_json_file(file_path)
    if not html_content:
        return None
    doc_id_map[cur_doc_id] = doc_url
    
    soup = BeautifulSoup(html_content, 'lxml')
    
    text = soup.get_text(separator=' ').lower()
    
    tokens = tokenize(text)
    if not tokens:
        return None
    
    term_frequencies = calculate_term_frequencies(tokens)
    
    term_weights = calculate_term_weights(soup, term_frequencies)
    
    for term in term_frequencies:
        tf = term_frequencies[term]
        weight = term_weights[term]
        new_post = Posting(cur_doc_id, tf, weight)
        iIndex[term].append(new_post)
    

    
    if (sys.getsizeof(iIndex) > MAX_INDEX_SIZE):
        write_partialidx()


        
# Save the inverted index to disk
def write_partialidx():
    global partialidx_count

    if PICKLE:
        resulting_pickle_file_name = f'results/inverted_index{partialidx_count}.pkl'

        
        with open(resulting_pickle_file_name, 'wb') as f:
            pickle.dump(iIndex, f)
    else:
        resulting_txt_file_name = f'results/inverted_index{partialidx_count}.txt'
        
        with open(resulting_txt_file_name, 'w') as f:
            for key, value in iIndex.items():
                f.write(f"{key}: {value}\n")
                
    iIndex.clear()
    partialidx_count += 1
            
            
def merge_partialidx(partialidx):
    filename = f'results/{partialidx}'
    
    


# Main execution block
if __name__ == '__main__':
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

    # # Multiprocessing
    # num_workers = cpu_count()
    # with Pool(num_workers) as pool:
    #     pool.map(process_json_file, file_paths)


    for file in file_paths:
        process_json_file(file)
    
    if not iIndex:
        #Write the remainder
        write_partialidx()
    
    print('All files processed, Commence merging process.')





    # # Multiprocessing
    # num_workers = cpu_count()
    # with Pool(num_workers) as pool:
    #     results = pool.map(process_json_file, file_paths)

    # Collect term frequencies and document frequencies
    #doc_term_freqs = {}  # {doc_id: {token: tf}}

    # doc_count = 0
    # for result in results:
    #     if result is None:
    #         continue
    #     doc_url, term_frequencies = result
    #     doc_id = doc_count
    #     doc_id_map[doc_id] = doc_url
    #     #doc_ids.add(doc_id)
    #     doc_term_freqs[doc_id] = term_frequencies
        
    #     for token in term_frequencies.keys():
    #         doc_freqs[token] += 1  # Increment document frequency for the token
        
    #     doc_count += 1

    #total_docs = len(doc_ids)  # Total number of documents

    # Calculate idf values
    # idf_values = {}
    # for token, df in doc_freqs.items():
    #     idf = math.log(doc_count / df)
    #     idf_values[token] = idf

    #Redo the inverted index to have all doc id's in the set of doc_ids
    
    
    
    # Build the inverted index with tf-idf scores
    # for doc_id, term_frequencies in doc_term_freqs.items():
    #     for token, tf in term_frequencies.items():
    #         idf = idf_values[token]
    #         tf_idf = tf * idf
    #         posting = Posting(doc_id, tf, tf_idf)
    #         # if the first char of the token changes, add new index with the position.
    #         if token not in inverted_index:
    #             new_posting_list = [posting]
    #             inverted_index[token] = new_posting_list
    #         else:
    #             inverted_index[token].append(posting) #possibly words for the ordered dict?
            #inverted_index[token].append(posting)
            
    #sort the inverted index
    # sorted_items = sorted(inverted_index.items(), key=lambda item: item[0])
    # sorted_inverted_index = dict(sorted_items)
    
    #Build the index of the index
    line_count = 0
    last_char = '\0'
    #ind_ind = defaultdict(char)
    # for token, value in inverted_index.items():
        
    #     if token[0] != last_char:
    #         last_char = token[0]
    #         ind_ind[last_char] = token
            
    # with open(resulting_index_of_index, 'w') as f:
    #     for key, value in ind_ind.items():
    #         f.write(f"{key}: {value}\n")
    

    # Save the inverted index to disk
    # with open(resulting_pickle_file_name, 'wb') as f:
    #     pickle.dump(inverted_index, f)

    # Save the inverted index to disk
    # with open(resulting_pickle_file_name, 'w') as f:
    #     for key, value in sorted_inverted_index.items():
    #         f.write(f"{key}: {value}\n")
        

    # Get the size of the index file in KB
    #index_size_kb = os.path.getsize(resulting_pickle_file_name) / 1024

    # Display the analytics
    print("\n=== Index Analytics ===")
    print(f"Number of indexed documents: {doc_count}")
    print(f"Number of unique tokens: {len(iIndex)}")
    #print(f"Total size of the index on disk: {index_size_kb:.2f} KB")
