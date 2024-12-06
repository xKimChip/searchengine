import pickle
import os

# CONFIGURATION
OUTPUT_DIR = 'index_files'
postings_file_path = os.path.join(OUTPUT_DIR, 'inverted_index_postings.txt')
dict_file_path = os.path.join(OUTPUT_DIR, 'inverted_index_dict.pkl')
docmap_file_path = os.path.join(OUTPUT_DIR, 'doc_id_map.pkl')
stop_list = set(
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your',
    'yours', 'yourself', 'yourselves', 'he', 'him',
    'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself',
    'they', 'them', 'their', 'theirs', 'themselves',
    'what', 'which', 'who', 'whom', 'this',
    'that', 'these', 'those', 'am', 'is',
    'are', 'was', 'were', 'be', 'been',
    'being', 'have'',', 'has', 'had', 'having',
    'do', 'does', 'did', 'doing', 'a',
    'an', 'the', 'and', 'but', 'if',
    'or', 'because', 'as', 'until', 'while',
    'of', 'at', 'by', 'for', 'with',
    'about', 'against', 'between', 'into', 'through',
    'during', 'before', 'after', 'above', 'below',
    'to', 'from', 'up', 'down', 'in',
    'out', 'on', 'off', 'over', 'under',
    'again', 'further', 'then', 'once', 'here',
    'there', 'when', 'where', 'why', 'how',
    'all', 'any', 'both', 'each', 'few',
    'more', 'most', 'other', 'some', 'such',
    'no', 'nor', 'not', 'only', 'own',
    'same', 'so', 'than', 'too', 'very',
    's', 't', 'can', 'will', 'just',
    'don', 'should', 'now'
)
# stop word list stolen blatantly from https://gist.github.com/sebleier/554280
# Load dictionary and doc_id_map
with open(dict_file_path, 'rb') as f:
    term_dict = pickle.load(f)

with open(docmap_file_path, 'rb') as f:
    doc_id_map = pickle.load(f)


def tokenize_query(query):
    from nltk.stem.porter import PorterStemmer
    stemmer = PorterStemmer()
    tokens = query.lower().split()
    tokens = [stemmer.stem(t) for t in tokens if t not in stop_list]
    return tokens


def get_postings_for_term(term):
    # Check if term in dictionary
    if term not in term_dict:
        return []

    offset = term_dict[term]

    # Seek in postings file
    with open(postings_file_path, 'rb') as pf:
        pf.seek(offset)
        # Read the length line
        length_line = pf.readline().decode('utf-8').strip()
        length = int(length_line)
        postings_data = pf.read(length)
        # postings_data is "doc_id:tfidf doc_id:tfidf ..."
        postings_str = postings_data.decode('utf-8').strip()
        postings_pairs = postings_str.split(" ")
        postings = []
        for pair in postings_pairs:
            doc_id_str, tfidf_str = pair.split(":")
            doc_id_int = int(doc_id_str)
            tfidf_float = float(tfidf_str)
            postings.append((doc_id_int, tfidf_float))
        return postings


def query_and(tokens):
    # Retrieve postings for each token and intersect
    if not tokens:
        return []
    postings_lists = [get_postings_for_term(t) for t in tokens]
    # Convert each to dict {doc_id:score} for intersection
    dicts = []
    for pl in postings_lists:
        d = {p[0]: p[1] for p in pl}
        dicts.append(d)

    # Intersect by keys
    # Start with first
    common_docs = set(dicts[0].keys())
    for d in dicts[1:]:
        common_docs.intersection_update(d.keys())

    # Combine scores from intersection (sum tfidf)
    results = []
    for doc_id in common_docs:
        score = sum(d[doc_id] for d in dicts)
        results.append((doc_id, score))

    # Sort by score descending
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
            # Print top 5 results
            for doc_id, score in results[:5]:
                print(f"{doc_id_map[doc_id]} (score: {score})")


if __name__ == "__main__":
    main()
