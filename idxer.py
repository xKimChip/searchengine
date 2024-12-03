import os, sys, re
import json
from bs4 import BeautifulSoup
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
import math
import pickle
import shelve
from nltk import WordNetLemmatizer
from helpers import *
#import globals
from tokenizer import tokenize

#MAX_INDEX_SIZE = 20000000
MAX_INDEX_SIZE = 18000
partialidx_count = 1

resulting_txt_IOI = 'index_index.txt'
resulting_pickle_IOI = 'index_index.pkl'

PICKLE = False
MULTI_PROC = False


#Posting class definition
class Posting:
    def __init__(self, doc_id, tf, weight):
    #def __init__(self, doc_id: set, tf, tf_idf, weight):
        self.doc_id = doc_id
        self.tf = tf
        self.weight = weight
        self.tf_idf = 0
    
    def __del__(self):
        return

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
doc_id_map = list()
doc_count = 0

index_lock = Lock()
id_lock = Lock()

    
    
# Function to read JSON file
def read_json_file(file_path):
    #global doc_count
    try:
        with open(file_path, 'r', encoding='ascii') as f:
            data = json.load(f)
        url = data.get('url')
        content = data.get('content')
        # For multithreading, might put this under the html check and add a lock
        with id_lock:
            doc_id = len(doc_id_map)
            doc_id_map.append(url)

        
        if not url or not content:
            return None, None, None
        return url, content, doc_id
    except Exception:
        return None, None

# Function to extract text from HTML content




# Function to process a single JSON file and return doc_id and term frequencies

def process_json_file(file_path):
    global iIndex
    doc_url, html_content, cur_doc_id = read_json_file(file_path)
    if not html_content:
        return None
    
    
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
        with index_lock:
            iIndex[term].append(new_post)
            # if (sys.getsizeof(iIndex) > MAX_INDEX_SIZE):
            #     write_partialidx()


        
# Save the inverted index to disk
def write_partialidx():
    global partialidx_count
    global iIndex
    

    if PICKLE:
        resulting_pickle_file_name = f'results/inverted_index{partialidx_count}.pkl'

        
        with open(resulting_pickle_file_name, 'wb') as f:
            pickle.dump(sorted(iIndex), f)
    else:
        resulting_txt_file_name = f'results/inverted_index{partialidx_count}.txt'
        
        with open(resulting_txt_file_name, 'w') as f:
            for key, value in sorted(iIndex.items()):
                f.write(f"{key}: {value}\n")
                
    partialidx_count += 1
    iIndex.clear()
            
#May switch to shelve
def merge_partialidx(partialidx):
    filename = f'results/{partialidx}'
    with open(filename) as file:
        pidx_json = pickle.load(file)
        
    cur_char_file = '0.pkl'
    with open(f'results/{cur_char_file}') as file:
        load = pickle.load(file)
        
    for token in pidx_json.keys():
        char_file = f'{token[0]}.pkl'
        
        if index_char_file != char_file:
            
            index_char_file = char_file
            
            with open(f'results/{char_file}') as f:
                load = pickle.load(f)
    
    
    
def main():
    # Path to the directory you want to process
    #global partialidx_count
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

    if MULTI_PROC:
        print('THREADING TIME!')
        with ThreadPoolExecutor() as executor:
            executor.map(process_json_file, file_paths)
    else:  
        for file in file_paths:
            process_json_file(file)
            if len(doc_id_map) > MAX_INDEX_SIZE * partialidx_count:
                write_partialidx()
        write_partialidx()    
        with open('results/id_map.pkl', 'w+') as file:
            json.dump(doc_id_map, file)

        
    
    print('All files processed, Commence merging process.')

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
    


    # Display the analytics
    print("\n=== Index Analytics ===")
    print(f"Number of indexed documents: {len(doc_id_map)}")
    print(f"Number of unique tokens: {len(iIndex)}")
    #print(f"Total size of the index on disk: {index_size_kb:.2f} KB")

# Main execution block
if __name__ == '__main__':
    main()
