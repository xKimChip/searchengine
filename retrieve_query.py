# Once you have built the inverted index, you are ready to test document retrieval with queries. At the very least, the search should be able to deal with boolean queries: AND only. If you wish, you can sort the retrieved documents based on tf-idf scoring (you are not required to do so now, but it will be required for the final search engine). This can be done using the cosine similarity method. Feel free to use a library to compute cosine similarity once you have the term frequencies and inverse document frequencies (although it should be very easy for you to write your own implementation).
#  You may also add other weighting/scoring mechanisms to help refine the search results.

import pickle
from typing import Any
from index_construction import resulting_pickle_file_name, Posting
from multiprocessing import Pool, cpu_count
import threading

token = str


def get_unpickled_document(pickle_file: str) -> Any:
    with open(pickle_file, 'rb'):
        return pickle.loads(pickle_file)


inverted_index: dict[token, list[Posting]
                     ] = get_unpickled_document(resulting_pickle_file_name)


def get_query_result(query_term: token) -> set[Posting]:
    return set(inverted_index[query_term])


def get_query_results_and(query_terms: list[token]) -> list[Posting]:

    result = set()

    for query in query_terms:
        query_res: list[Posting] = get_query_result(query)
        if result:
            result.intersection_update(query_res)

    res_list = list(result)
    return sorted(res_list, key=lambda curr_posting: curr_posting.tf_idf)


def get_query_results_and_multithreaded(query_terms: list[token]) -> list[Posting]:
    if not query_terms:
        return list()
    results: list[Posting] = list()
    results_lock: threading.Lock = threading.Lock()
    threads: list[threading.Thread] = list()

    def get_internal_query_result(query_term: token) -> set[Posting]:
        query_res = set(inverted_index[query_term])
        with results_lock:
            results.append(query_res)

    for query in query_terms:
        new_thread = threading.Thread(
            target=get_internal_query_result, args=query)
        threads.append(new_thread)
        new_thread.start()

    for thread in threads:
        thread.join()

    final_result: set[Posting] = results[0]
    for result in results[1:]:
        final_result.intersection_update(result)

    return sorted(final_result, key=lambda curr_posting: curr_posting.tf_idf)


def parse_queries(query_list: list[token]) -> list[list[token]]:
    result = list()
    curr_query_list: list[token] = list()
    for query in query_list:
        if query == 'AND':
            continue
        elif query == 'OR':
            result.append(curr_query_list)
        else:
            curr_query_list.append(query.lower())
        # if query is AND, skip and keep appending to the same list
        # if query is OR, append curr_query_list to result list and then 0 out curr_query_list
        # go next
        # else query is regular query, keep appending

    return result


def get_query_results_from_user_input(queries_list: list[list[token]]):
    query_results: list[list[Posting]] = list()
    query_results_lock: threading.Lock = thread.Lock()
    threads: list[threading.Threads] = list()

    def append_query_to_query_results(query_terms):
        query_res: list[Posting] = get_query_results_and_multithreaded(
            query_terms)
        with query_results_lock:
            query_results.append(query_res)

    for query in queries_list:
        new_thread = threading.Thread(
            target=append_query_to_query_results, args=query)
        threads.append(new_thread)
        new_thread.start()

    for thread in threads:
        thread.join()

    final_result = set(query_results[0])
    for result in query_results[1:]:
        final_result = final_result.union(result)

    return sorted(final_result, key=lambda posting: posting.tf_idf)


exit_statements = ["EXIT PLZ", "GOODBYE QUERY"]


def main():

    while True:
        try:
            user_query = input()
            if user_query in exit_statements:
                print("Bye bye.")
                break
        except EOFError:
            print("User entered Ctrl + D. Bye bye.")
