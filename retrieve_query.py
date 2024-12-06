# Once you have built the inverted index, you are ready to test document retrieval with queries. At the very least, the search should be able to deal with boolean queries: AND only. If you wish, you can sort the retrieved documents based on tf-idf scoring (you are not required to do so now, but it will be required for the final search engine). This can be done using the cosine similarity method. Feel free to use a library to compute cosine similarity once you have the term frequencies and inverse document frequencies (although it should be very easy for you to write your own implementation).
#  You may also add other weighting/scoring mechanisms to help refine the search results.

import pickle
from typing import Any
from index_construction import resulting_pickle_file_name, Posting
from multiprocessing import Pool, cpu_count
import threading

token = str
Position = int


EXTRA_PRINTS_ACTIVE: bool = False
MAX_LINKS_SHOWN: int = 5
TESTING: bool = False


def interpret_line(file: _io.TextIOWrapper) -> list[Posting]:
    # expecting format: [num_chars_to_read] [doc_id tf_idf weight]
    # this will return a posting
    num_chars_to_read = 0
    while (curr_char := file.read(1)).isnumeric():

        num_chars_to_read = num_chars_to_read * 10 + int(curr_char)

    postings_strings_list: list[str] = file.read(num_chars_to_read).split(',')
    posting_list = list[Posting]
    for posting_str in postings_strings_list:
        #  using this formatting '{self.doc_id} {self.tf} {self.weight} {self.tf_idf}'
        posting_parts = posting_str.split()
        doc_id = int(posting_parts[0])

        tf = 0  # tf made 0 because it is not used so no use in calculating the value
        # resulting tf idf value after weighting accounted for
        tf_idf = float(posting_parts[2])*float(posting_parts[3])
        # arguably this step should be done previously as this calculation can be made during the index consutrction

        resulting_posting: Posting = Posting(doc_id, tf, tf_idf)
        posting_list.append(resulting_posting)

    return posting_list


def seek_to_correct_position(query_term: token, term_seek_dictionary: dict[token][Position], file: _io.TextIOWrapper):
    file.seek(term_seek_dictionary.get(query_term))


def get_unpickled_document(pickle_file: str) -> Any:
    with open(pickle_file, 'rb') as opened_pickle_file:
        return pickle.load(opened_pickle_file)


if TESTING:
    inverted_index = {
        "hello": [Posting(2, 2, 3), Posting(3, 2, 3), Posting(4, 2, 3), Posting(1, 2, 3), Posting(5, 2, 3),],
        "run": [Posting(2, 2, 3)],
        "walk": [Posting(3, 2, 3)],
        "live": [Posting(4, 2, 3)],
        "exist": [Posting(5, 2, 3)],
        "believe": [Posting(5, 2, 3)],
        "goodbye": [Posting(6, 2, 3)],
    }
else:
    inverted_index: dict[token, list[Posting]
                         ] = get_unpickled_document(resulting_pickle_file_name)


if EXTRA_PRINTS_ACTIVE:
    print(f'inverted_index= {inverted_index}')


def get_query_result(query_term: token) -> set[Posting]:
    return set(inverted_index.get(query_term)) if inverted_index.get(query_term) else set()


def get_query_results_and(query_terms: list[token]) -> list[Posting]:

    result = set()

    for query in query_terms:
        query_res: list[Posting] = get_query_result(query)
        result.intersection_update(query_res)

    return sorted(result, key=lambda curr_posting: curr_posting.tf_idf)


def get_query_results_and_multithreaded(query_terms: list[token]) -> list[Posting]:
    if not query_terms:
        return list()
    results: list[Posting] = list()
    results_lock: threading.Lock = threading.Lock()
    threads: list[threading.Thread] = list()

    def get_internal_query_result(query_term: token) -> list[Posting]:
        query_res = get_query_result(query_term)
        with results_lock:
            results.append(query_res)

    for query in query_terms:
        if EXTRA_PRINTS_ACTIVE:
            print(f'Args = {query}')
        new_thread = threading.Thread(
            target=get_internal_query_result, args=[query])
        threads.append(new_thread)
        new_thread.start()

    for thread in threads:
        thread.join()

    if not results:
        return None
    final_result: set[Posting] = results[0]
    for result in results[1:]:
        final_result.intersection_update(result)

    return sorted(final_result, key=lambda curr_posting: curr_posting.tf_idf)


def parse_queries(query_list: list[token]) -> list[list[token]]:
    result: list[list[token]] = list()
    curr_query_list: list[token] = list()
    for query in query_list.split():
        match query:
            case 'AND':
                continue
            case 'OR':
                result.append(curr_query_list)
                curr_query_list = list()
            case _:
                curr_query_list.append(query.lower())
            # this should be altered when/if we take positioning into account
            # currently any phrase together is still just two and statements

    result.append(curr_query_list)

    # if query is AND, skip and keep appending to the same list
    # if query is OR, append curr_query_list to result list and then 0 out curr_query_list
    # go next
    # else query is regular query, keep appending

    return result


def get_query_results_from_user_input(queries_list: list[list[token]]) -> list[Posting]:
    query_results: list[list[Posting]] = list()
    query_results_lock: threading.Lock = threading.Lock()
    threads: list[threading.Threads] = list()

    def append_query_to_query_results(query_terms):
        query_res: list[Posting] = get_query_results_and_multithreaded(
            query_terms)
        with query_results_lock:
            query_results.append(query_res)

    for query in queries_list:
        new_thread = threading.Thread(
            target=append_query_to_query_results, args=[query])
        threads.append(new_thread)
        new_thread.start()

    for thread in threads:
        thread.join()

    if EXTRA_PRINTS_ACTIVE:
        print(f'query_results = {query_results}')
    if not query_results:
        return list()
    final_result = set(query_results[0])
    for result in query_results[1:]:
        final_result = final_result.union(result)

    return sorted(final_result, key=lambda posting: posting.tf_idf)


exit_statements = ["EXIT PLZ", "GOODBYE QUERY"]


def main():

    while True:
        try:
            user_query = input(f'Please input your next query: ')
            if user_query in exit_statements:
                print("Bye bye.")
                break
        except EOFError:
            print("User entered Ctrl + D. Bye bye.")
            break
        except KeyboardInterrupt:
            print("Goodbye you little interuptee")
            break

        # if TESTING:
        #     print(f'{curr_query_from_user}')

        #     print(f'Getting first query singlethreaded:')
        #     single_threaded = get_query_results_and(curr_query_from_user[0])
        #     print(f'{single_threaded}')
        #     print(f'Getting first query multithreaded:', end='\t')

        #     result = get_query_results_and_multithreaded(
        #         curr_query_from_user[0])
        #     print(f'{result}')
        #     print(f'Got correct result: single = multi', end='\t')
        #     print(f'{single_threaded == result}')
        #     # query_results: list[Posting] = get_query_results_from_user_input(
        #     #     curr_query_from_user)

        parsed_user_input = parse_queries(user_query)
        if EXTRA_PRINTS_ACTIVE:
            print(f'Parsed user query = {parsed_user_input}')
        query_results = get_query_results_from_user_input(parsed_user_input)
        if not query_results:
            query_links = None
        else:
            query_links = [
                curr_posting.doc_id for curr_posting in query_results[:MAX_LINKS_SHOWN]]
        if not query_links:
            print(f'No results found')
        else:
            for link in query_links:
                print(link)


if __name__ == "__main__":
    main()
