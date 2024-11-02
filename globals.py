from collections import defaultdict, deque
from typing import Any, Callable #, TypeAlias
from DomainTrie import DomainTrie
from threading import Lock

allowed_domains = [
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
    "today.uci.edu"
]


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
# add type alias letter
Token = str
HASH = int
Token_Tuple = tuple[Token, Token, Token]
url_string = str

# for the read function, the first argument passed into action_to_take MUST be the global variable itself


def read_global_variable(global_to_read: Any, global_variable_lock: Lock, action_to_take: Callable[..., Any] = None, *args) -> Any:
    with global_variable_lock:
        print(f"\n\tAcessing global variable {global_to_read}\n\tRunning operation {action_to_take.__name__}({global_to_read, args})")
        if action_to_take == None:
            result = global_to_read
        elif len(args) == 0:
            result = action_to_take(global_to_read)
        else:
            result = action_to_take(global_to_read, *args)

    return result


def write_global_variable(global_to_read: Any, global_variable_lock: Lock, action_to_take: Callable[..., Any], *args) -> Any:
    with global_variable_lock:
        print(f"\n\tReading global variable {global_to_read}\n\tRunning operation {action_to_take.__name__}({global_to_read, args})")
        if len(args) == 0:
            result = action_to_take(global_to_read)
        else:
            result = action_to_take(global_to_read, *args)

        print(f'Successfully altered global variable')

    return result


def unique_urls_trie_insert(domain_to_insert: url_string) -> bool:
    # lock
    # return false if cannot be unlocked fast enough
    unique_urls_trie.insert(domain_to_insert)
    # unlock
    return True
# Define the tokenizer function
