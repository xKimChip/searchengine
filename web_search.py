import pickle
import os
import struct
import string
from flask import Flask, request, render_template_string
from nltk.stem.porter import PorterStemmer

# Configuration
OUTPUT_DIR = 'index_files'
DOCMAP_FILE = os.path.join(OUTPUT_DIR, 'doc_id_map.pkl')

# Load doc_id_map
with open(DOCMAP_FILE, 'rb') as f:
    doc_id_map = pickle.load(f)

# Load all dictionaries and keep file handles open
range_dicts = {}
index_file_handles = {}

for f in os.listdir(OUTPUT_DIR):
    if f.startswith('inverted_index_dict_') and f.endswith('.pkl'):
        full_path = os.path.join(OUTPUT_DIR, f)
        with open(full_path, 'rb') as df:
            d = pickle.load(df)
        part = f.replace('inverted_index_dict_', '').replace('.pkl','')
        if len(part) == 1 and part in string.ascii_lowercase:
            postings_file = os.path.join(OUTPUT_DIR, f'inverted_index_{part}.bin')
        else:
            postings_file = os.path.join(OUTPUT_DIR, 'inverted_index_others.bin')

        if postings_file not in index_file_handles:
            index_file_handles[postings_file] = open(postings_file, 'rb')
        range_dicts[postings_file] = d

stemmer = PorterStemmer()

def tokenize_query(query):
    tokens = query.lower().split()
    tokens = [stemmer.stem(t) for t in tokens]
    return tokens

def generate_ngrams(tokens, n):
    return ['_'.join(tokens[i:i+n]) for i in range(len(tokens)-n+1)]

def get_range_file(term):
    if not term:
        return os.path.join(OUTPUT_DIR, 'inverted_index_others.bin')
    first_char = term[0].lower()
    if first_char in string.ascii_lowercase:
        return os.path.join(OUTPUT_DIR, f'inverted_index_{first_char}.bin')
    else:
        return os.path.join(OUTPUT_DIR, 'inverted_index_others.bin')

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
    term_len_buf = f.read(2)
    if len(term_len_buf) < 2:
        return []
    term_len = struct.unpack('>H', term_len_buf)[0]
    f.seek(term_len, 1)
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
    # In addition to single tokens, we incorporate 2-grams and 3-grams
    if not tokens:
        return []

    # Get single-term postings
    postings_lists = [get_postings_for_term(t) for t in tokens]

    # Generate and retrieve bigrams and trigrams from the query
    bigrams = generate_ngrams(tokens, 2)
    trigrams = generate_ngrams(tokens, 3)

    # Add their postings as well
    for bg in bigrams:
        postings_lists.append(get_postings_for_term(bg))
    for tg in trigrams:
        postings_lists.append(get_postings_for_term(tg))

    # Combine scores from all postings (single terms, bigrams, trigrams)
    # Instead of strict intersection, we union all doc_ids and sum their scores.
    score_map = {}
    for pl in postings_lists:
        for doc_id, score in pl:
            score_map[doc_id] = score_map.get(doc_id, 0) + score

    # Convert to a list of (doc_id, score) and sort
    results = list(score_map.items())
    results.sort(key=lambda x: x[1], reverse=True)
    return results

HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
    <title>My Animated Search Engine</title>
    <!-- Bootstrap CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <!-- Animate.css -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/4.1.1/animate.min.css"/>
    <style>
    body {
        background: #f2f2f2;
        font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
    }
    .hero {
        background: linear-gradient(rgba(0,0,0,0.4), rgba(0,0,0,0.4)),
                    url('https://i.kym-cdn.com/entries/icons/original/000/048/227/freaky_memes.jpg') center/cover no-repeat;
        color: white;
        padding: 100px 0;
        text-align: center;
    }
    .hero h1 {
        font-size: 4rem;
        font-weight: 700;
    }
    .search-box {
        max-width: 600px;
        margin: 30px auto;
    }
    .search-box input[type='text'] {
        border-radius: 30px;
        padding: 15px 20px;
        width: 80%;
        border: none;
        outline: none;
        margin-right: 10px;
    }
    .search-box input[type='submit'] {
        border-radius: 30px;
        padding: 10px 20px;
        border: none;
        background: #007bff;
        color: #fff;
        font-weight: 600;
    }
    .results {
        margin: 50px auto;
        max-width: 800px;
    }
    .result-item {
        background: #fff;
        border-radius: 8px;
        margin-bottom: 20px;
        padding: 20px;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }
    .result-item:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 20px rgba(0,0,0,0.1);
    }
    .no-results {
        text-align: center;
        font-size: 1.5rem;
        color: #555;
        margin-top: 50px;
    }
    </style>
</head>
<body>

<div class="hero animate__animated animate__fadeIn">
    <div class="container">
        <h1 class="animate__animated animate__fadeInDown">Walmart Google</h1>
        <p class="lead animate__animated animate__fadeInUp">Performs searchs n stuff idk</p>
        <div class="search-box animate__animated animate__zoomIn">
            <form method="GET" action="/">
                <input type="text" name="q" placeholder="Enter your query..." value="{{ query }}">
                <input type="submit" value="Search">
            </form>
        </div>
    </div>
</div>

<div class="container results animate__animated animate__fadeIn">
    {% if query and results %}
        <h2>Results for "{{ query }}"</h2>
        {% for url, score in results %}
        <div class="result-item">
            <h5><a href="{{ url }}" target="_blank" class="text-decoration-none">{{ url }}</a></h5>
            <p>Score: {{ score }}</p>
        </div>
        {% endfor %}
    {% elif query and query != "" %}
        <div class="no-results">No results found for "{{ query }}"</div>
    {% endif %}
</div>

<!-- Bootstrap Bundle JS -->
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

app = Flask(__name__)

@app.route("/", methods=["GET"])
def search_page():
    query = request.args.get('q', '')
    results_display = []
    if query.strip():
        tokens = tokenize_query(query)
        results = query_and(tokens)
        if results:
            for doc_id, score in results[:10]:
                url = doc_id_map[doc_id]
                results_display.append((url, score))
        else:
            results_display = []
    return render_template_string(HTML_TEMPLATE, query=query, results=results_display)

if __name__ == "__main__":
    # Run in debug mode for development
    app.run(debug=True)