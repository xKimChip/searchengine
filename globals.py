from collections import defaultdict, deque
from operator import __eq__
import random
from typing import Any, Callable  # , TypeAlias
from DomainTrie import DomainTrie
from threading import Lock
from test_suite import test_function

allowed_domains = [
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
    "today.uci.edu"
]


unique_urls_trie: DomainTrie = DomainTrie()
unique_urls = set()
unique_urls_lock = Lock()
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

lock_to_global_dict: dict[Lock, Any] = dict()

generic_global_var: list = list()
generic_global_var_lock = Lock()

# lock_to_global_dict[generic_global_var_lock] = generic_global_var

# def add_global_lock_variable_combo
# for the read function, the first argument passed into action_to_take MUST be the global variable itself


def read_global_variable(global_to_read: Any, global_variable_lock: Lock, action_to_take: Callable[..., Any] = None, *args) -> Any:
    with global_variable_lock:
        print(f"\n\tAcessing global variable {global_to_read}\n\tRunning operation {
              action_to_take.__name__}({global_to_read, args})")
        if action_to_take == None:
            result = global_to_read
        elif len(args) == 0:
            result = action_to_take(global_to_read)
        else:
            result = action_to_take(global_to_read, *args)

    return result


def read_global_variable_action_does_not_pass_global(global_to_read: Any, global_variable_lock: Lock, action_to_take: Callable[..., Any] = None, *args) -> Any:
    with global_variable_lock:
        # note that this print could be out of date if the global variable is passed by value and not by reference
        print(f"\n\tAcessing global variable {global_to_read}\n\tRunning operation {
              action_to_take.__name__}({global_to_read, args})")
        if action_to_take == None:
            result = global_to_read
        elif len(args) == 0:
            result = action_to_take()
        else:
            result = action_to_take(*args)

    return result


def write_global_variable(global_to_read: Any, global_variable_lock: Lock, action_to_take: Callable[..., Any], *args) -> Any:
    with global_variable_lock:
        # note that this print could be out of date if the global variable is passed by value and not by reference
        print(f"\n\tReading global variable {global_to_read}\n\tRunning operation {
              action_to_take.__name__}({global_to_read, args})")
        if len(args) == 0:
            result = action_to_take(global_to_read)
        else:
            result = action_to_take(global_to_read, *args)

        print(f'Successfully altered global variable')

    return result


def write_global_variable_action_does_not_pass_global(global_to_read: Any, global_variable_lock: Lock, action_to_take: Callable[..., Any], *args) -> Any:
    with global_variable_lock:
        print(f"\n\tReading global variable {global_to_read}\n\tRunning operation {
              action_to_take.__name__}({global_to_read, args})")
        if len(args) == 0:
            result = action_to_take()
        else:
            result = action_to_take(*args)

        print(f'Successfully altered global variable')

    return result


def unique_urls_trie_insert(domain_to_insert: url_string) -> bool:
    # lock
    # return false if cannot be unlocked fast enough
    unique_urls_trie.insert(domain_to_insert)
    # unlock
    return True
# Define the tokenizer function


def pollute_generic_global_var_with_test_data(num_generic_test_data: int = 100, max_range: int = 100):
    for i in range(num_generic_test_data):
        generic_global_var.append(random.randint(0, max_range))


def get_generic_data_sum() -> int:
    return sum(generic_global_var)


def test_generic_data_sum() -> int:
    test_function(get_generic_data_sum(), __eq__, read_global_variable_action_does_not_pass_global,
                  generic_global_var, generic_global_var_lock, get_generic_data_sum)

    test_function(None, __eq__, write_global_variable_action_does_not_pass_global,
                  generic_global_var, generic_global_var_lock, pollute_generic_global_var_with_test_data)

    test_function(get_generic_data_sum(), __eq__, read_global_variable_action_does_not_pass_global,
                  generic_global_var, generic_global_var_lock, get_generic_data_sum)


if __name__ == "__main__":
    print(f'Running {__file__.split("/")[-1]}')
    test_generic_data_sum()

    print(f'\n\n\n\nglobals = {globals()}')
