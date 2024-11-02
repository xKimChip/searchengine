from collections import deque
import random
from typing import Any, Callable
from globals import (Token, Token_Tuple, HASH)


MAX_ALLOWED_SIMILARITY = .65
N_GRAM_HASHED_LIST_MAX_SIZE = 100
DEFAULT_N_GRAM_SIZE = 3

N_GRAM_HASHED_LIST: deque[set[HASH]] = deque()


def n_gram(token_list: list[Token], n_grams: int = DEFAULT_N_GRAM_SIZE) -> list[Token_Tuple]:

    # determine what percentage of the document to select
    # set the value between 0 and 1
    AMOUNT_OF_LIST_TO_SELECT: float = 1

    tuple_list: list[Token_Tuple] = list()
    for i in range(0, len(token_list), n_grams):
        # curr_tuple: tuple[Token, Token, Token] = tuple()
        curr_list_of_elements: list[Token] = list()
        if (random.random() <= AMOUNT_OF_LIST_TO_SELECT):
            for j in range(i, min(i + n_grams, len(token_list))):
                curr_list_of_elements.append(token_list[j])

            resultant_n_tuple: Token_Tuple = tuple(curr_list_of_elements)
            print(f'Appending the tuple : {resultant_n_tuple}')
            tuple_list.append(resultant_n_tuple)

    return tuple_list


def create_list_of_n_gram_hashes(tuple_list: list[Token_Tuple]) -> list[HASH]:
    resultant_hash_list: list[HASH] = list()
    for token_tuple in tuple_list:
        resultant_hash_list.append(hash(token_tuple))

    return resultant_hash_list


def make_set_of_n_gram_hashes(tuple_list: list[Token_Tuple]) -> set[HASH]:
    return set(create_list_of_n_gram_hashes(tuple_list=tuple_list))


def get_similarity_score(n_gram_hash1: set[HASH], n_gram_hash2: set[HASH]) -> float:
    # returns a score between 0 and 1
    intersection_length: int = len(n_gram_hash1.intersection(n_gram_hash2))
    union_length: int = len(n_gram_hash1.union(n_gram_hash2))
    return intersection_length / union_length


def should_evaluate_based_on_similarity_score(n_grams_list: list[set[HASH]], n_gram_hash1: set[HASH], max_allowed_score: float = MAX_ALLOWED_SIMILARITY) -> bool:
    for curr_n_gram_hash in n_grams_list:
        if get_similarity_score(n_gram_hash1=n_gram_hash1, n_gram_hash2=curr_n_gram_hash) > max_allowed_score:
            return False

    return True


def go_thru_n_gram_phase(token_list: list[Token]) -> bool:
    tuple_list: list[Token_Tuple] = n_gram(token_list=token_list)
    hashed_tuple: set[HASH] = make_set_of_n_gram_hashes(tuple_list=tuple_list)
    should_read = read_n_gram_hash_list(
        should_evaluate_based_on_similarity_score, hashed_tuple)

    return should_read


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


