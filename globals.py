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
DEFAULT_N_GRAM_SIZE = 3

today_uci_edu_path = "/department/information_computer_sciences"

Token: TypeAlias = str
HASH: TypeAlias = int
Token_Tuple: TypeAlias = tuple[Token, Token, Token]
url_string: TypeAlias = str
MAX_ALLOWED_SIMILARITY = .65

N_GRAM_HASHED_LIST: deque[set[HASH]] = deque()
N_GRAM_HASHED_LIST_MAX_SIZE = 100


def unique_urls_trie_insert(domain_to_insert: url_string) -> bool:
    # lock
    # return false if cannot be unlocked fast enough
    unique_urls_trie.insert(domain_to_insert)
    # unlock
    return True
# Define the tokenizer function


def read_n_gram_hash_list(operation: Callable[[deque[set[HASH]]], Any], *args) -> Any:
    # Access the global variable
    # lock
    global N_GRAM_HASHED_LIST
    # Perform the operation on the global data structure
    result: Any = operation(N_GRAM_HASHED_LIST, *args)
    # unlock
    return result


def add_to_n_gram_hashed_list(hash_to_add: set[HASH]) -> bool:
    if type(hash_to_add) != set():
        hash_to_add = set(hash_to_add)

    # lock
    if len(N_GRAM_HASHED_LIST) == N_GRAM_HASHED_LIST_MAX_SIZE:
        N_GRAM_HASHED_LIST.popleft()

    N_GRAM_HASHED_LIST.append(hash_to_add)

    # unlock
    return True
