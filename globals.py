from collections import defaultdict, deque
from typing import Any, Callable, TypeAlias
from DomainTrie import DomainTrie

allowed_domains = [
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
    "today.uci.edu"
]

URL_SIMILARITY_THRESHOLD: float = .85

unique_urls_trie: DomainTrie = DomainTrie()
unique_urls = set()
longest_page = {
    'url': '',
    'word_count': 0
}
word_frequencies = defaultdict(int)
subdomains = defaultdict(int)
MAX_TOKEN_LENGTH = 10000  # Set a reasonable maximum length

today_uci_edu_path = "/department/information_computer_sciences"

Token: TypeAlias = str
HASH: TypeAlias = int
Token_Tuple: TypeAlias = tuple[Token, Token, Token]
url_string: TypeAlias = str


def unique_urls_trie_insert(domain_to_insert: url_string) -> bool:
    # lock
    # return false if cannot be unlocked fast enough
    unique_urls_trie.insert(domain_to_insert)
    # unlock
    return True
# Define the tokenizer function
