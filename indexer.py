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

def tree(): return defaultdict(tree)
index = tree()


doc_count = 0
