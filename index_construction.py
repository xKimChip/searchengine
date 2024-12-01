import os, re
import json
from bs4 import BeautifulSoup
from collections import OrderedDict, defaultdict
from multiprocessing import Pool, cpu_count
import math
import pickle
from nltk import WordNetLemmatizer

#import globals
from tokenizer import tokenize


HTML_WEIGHT_MULTIPLIER = {
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

global doc_count


# Posting class definition


class Posting:
    def __init__(self, doc_id, tf, tf_idf):
        self.doc_id = doc_id
        self.tf = tf
        self.tf_idf = tf_idf

    def __repr__(self) -> str:
        return f'{self.doc_id} {self.tf} {self.tf_idf}'

    def __str__(self) -> str:
        return f'{self.doc_id} {self.tf} {self.tf_idf}'

    def __eq__(self, other):
        return self.doc_id == other.doc_id

    def __hash__(self):
        return hash(self.doc_id)
# Function to read JSON file

def assign_importance_to_tokens(soup_text, term_frequencies_dict):
    lemma = WordNetLemmatizer()
    
    for tag in soup_text.find_all():
        tag_text = re.split("[^a-zA-Z']+", tag.get_text().lower())
        
        for word in tag_text:
            word = lemma.lemmatize(word.strip(" '"))

            #This should be adding an importance weight multiplier if the word shows up in any of the html_weighted categories.
            #May change to a multiplier for each category in the future, if the weights get too high.
            if word in term_frequencies_dict:
                term_frequencies_dict[word] *= HTML_WEIGHT_MULTIPLIER.get(tag.name, 1)
        

def read_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='ascii') as f:
            data = json.load(f)
        url = data.get('url')
        content = data.get('content')
        
        #url = data.at_pointer(b'/url').decode()
        #content = data.at_pointer(b'/content').decode()

        
        if not url or not content:
            return None, None
        return url, content
    except Exception:
        return None, None

# Function to extract text from HTML content


# def extract_text_from_html(html_content):
#     try:
#         soup = BeautifulSoup(html_content, 'html.parser')
#         # Remove script and style elements
#         for script_or_style in soup(['script', 'style']):
#             script_or_style.decompose()
#         text = soup.get_text(separator=' ').lower
#         return text
#     except Exception:
#         return ''

# Function to calculate term frequencies


def calculate_term_frequencies(tokens):
    tf_dict = defaultdict(int)
    total_terms = len(tokens)
    for token in tokens:
        tf_dict[token] += 1 / total_terms
    
    return tf_dict

# Function to process a single JSON file and return doc_id and term frequencies

def process_json_file(file_path):

    doc_url, html_content = read_json_file(file_path)
    if not html_content:
        return None
    
    
    #soup = extract_text_from_html(html_content)
    soup = BeautifulSoup(html_content, 'lxml')
    
    text = soup.get_text(separator=' ').lower()
    
    tokens = tokenize(text)
    if not tokens:
        return None
    term_frequencies = calculate_term_frequencies(tokens)
    
    #Assign a weight importance to each token
    #assign_importance_to_tokens(soup, term_frequencies)

    return (doc_url, term_frequencies)


resulting_pickle_file_name = 'inverted_index.txt'
resulting_index_of_index = 'index_index.txt'
# Main execution block
if __name__ == '__main__':
    inverted_index = defaultdict(list)
    doc_freqs = defaultdict(int)  # Document frequencies
    doc_ids = set()  # Set of unique document IDs
    doc_id_map = defaultdict()
    
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

    doc_count = 0
    for result in results:
        if result is None:
            continue
        doc_url, term_frequencies = result
        doc_id = doc_count
        doc_id_map[doc_id] = doc_url
        #doc_ids.add(doc_id)
        doc_term_freqs[doc_id] = term_frequencies
        
        for token in term_frequencies.keys():
            doc_freqs[token] += 1  # Increment document frequency for the token
        
        doc_count += 1

    #total_docs = len(doc_ids)  # Total number of documents

    # Calculate idf values
    idf_values = {}
    for token, df in doc_freqs.items():
        idf = math.log(doc_count / df)
        idf_values[token] = idf

    # Build the inverted index with tf-idf scores
    
    for doc_id, term_frequencies in doc_term_freqs.items():
        for token, tf in term_frequencies.items():
            idf = idf_values[token]
            tf_idf = tf * idf
            posting = Posting(doc_id, tf, tf_idf)
            # if the first char of the token changes, add new index with the position.
            inverted_index[token] = posting
    
    #Build the index of the index
    line_count = 0
    last_char = '\0'
    ind_ind = defaultdict(int)
    # for token, value in inverted_index.items():
        
    #     if token[0] != last_char:
    #         last_char = token[0]
    #         ind_ind[last_char] = line_count
            
    with open(resulting_index_of_index, 'w') as f:
        for key, value in ind_ind.items():
            f.write(f"{key}: {value}\n")
    

    # Save the inverted index to disk
    with open(resulting_pickle_file_name, 'w') as f:
        for key, value in inverted_index.items():
            f.write(f"{key}: doc_id= {value.doc_id}, tf= {value.tf}, tf-idf= {value.tf_idf}\n")
        

    # Get the size of the index file in KB
    index_size_kb = os.path.getsize(resulting_pickle_file_name) / 1024

    # Display the analytics
    print("\n=== Index Analytics ===")
    print(f"Number of indexed documents: {doc_count}")
    print(f"Number of unique tokens: {len(inverted_index)}")
    print(f"Total size of the index on disk: {index_size_kb:.2f} KB")
