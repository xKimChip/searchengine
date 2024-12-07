import os, re
from collections import OrderedDict, defaultdict
from nltk import WordNetLemmatizer



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

def calculate_term_frequencies(tokens):
    tf_dict = defaultdict(int)
    total_terms = len(tokens)
    for token in tokens:
        tf_dict[token] += 1
    
    return tf_dict
        


def calculate_term_weights(soup_text, term_frequencies_dict):
    lemma = WordNetLemmatizer()
    tw_dict = defaultdict(int)
    
    for tag in soup_text.find_all():
        # Regex to split on tokens pretty much.
        tag_text = re.split("[^a-zA-Z0-9']+", tag.get_text().lower())
        
        for word in tag_text:
            word = lemma.lemmatize(word.strip(" '"))

            #This should be adding an importance weight multiplier if the word shows up in any of the html_weighted categories.
            #May change to a multiplier for each category in the future, if the weights get too high.
            if word in term_frequencies_dict:
                tw_dict[word] += HTML_WEIGHT_MULTIPLIER.get(tag.name, 1)
                
    return tw_dict