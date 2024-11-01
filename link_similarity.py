import sys
from typing import Any, Callable, TypeAlias
from globals import url_string
import globals
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs

parsed_url_dict: TypeAlias = dict


def parse_url(url: url_string) -> parsed_url_dict:
    parsed_url = urlparse(url)
    return {
        "scheme": parsed_url.scheme,
        "netloc": parsed_url.netloc,
        "path": parsed_url.path,
        "params": parsed_url.params,
        "query": parsed_url.query,
        "fragment": parsed_url.fragment,
        "hostname": parsed_url.hostname,
        "port": parsed_url.port
    }


def get_path_similarity(path1: str, path2: str) -> int:
    path1_list: list = path1.split('/')
    path2_list: list = path2.split('/')

    consecutive_similar = 0

    for i in range(min(len(path1_list), len(path2_list))):
        if path1_list[i] == path2_list[i]:
            consecutive_similar += 1
        else:
            break

    return consecutive_similar


def get_path_similarity_score(path1: str, path2: str) -> float:
    path1_list: list = path1.split('/')
    path2_list: list = path2.split('/')
    result: float = get_path_similarity(
        path1, path2) / min(len(path1_list), len(path2_list))
    return result


def get_path_length(path: str) -> int:
    return path.count('/')


def get_path_part_differences(path1: str, path2: str) -> int:
    num_difs: int = 0
    for char1, char2 in zip(path1, path2):
        if char1 != char2:
            num_difs += 1

    num_difs += abs(len(path1) - len(path2))

    return num_difs

# requires two paths of the same length


def get_last_part_of_paths_num_difs(path1: str, path2: str) -> int:
    path1_list: list = path1.split('/')
    path2_list: list = path2.split('/')
    if len(path1_list) != len(path2_list):
        return (
            max(
                len(path1_list),
                len(path2_list)

            )
        )

    return get_path_part_differences(path1=path1_list[-1], path2=path2_list[-1])


def confirm_similarities_up_to_path(parsed_url1: parsed_url_dict, parsed_url2: parsed_url_dict) -> bool:
    return (
        parsed_url1["scheme"] == parsed_url2["scheme"] and
        parsed_url1["netloc"] == parsed_url2["netloc"] and
        parsed_url1["hostname"] == parsed_url2["hostname"] and
        parsed_url1["port"] == parsed_url2["port"]
    )


# if this is set to 2 there is no additional penalty for the same path + length
# if this is set to 1 there IS an additional penalty for the same path + length
NUM_DIFS_PENALTY: int = 1


def get_link_similarity(url1: url_string, url2: url_string) -> float:

    parsed_url1: parsed_url_dict = parse_url(url1)
    parsed_url2: parsed_url_dict = parse_url(url2)

    if not confirm_similarities_up_to_path(parsed_url1=parsed_url1, parsed_url2=parsed_url2):
        return 0

    # the rest of this shit is queries and fucking fragments so i don't really care about checking those
    # if the website is the same (or too similar) then the rest of this is going to be the same too and should then also be removed
    # i should also probably add checking for if the difference count of the two paths is too similar
    # if get_path_length(parsed_url1["path"]) != get_path_length(parsed_url2["path"]):
    #     return (
    #         get_path_similarity(
    #             path1=parsed_url1["path"],
    #             path2=parsed_url2["path"]
    #         ) / min(
    #             get_path_length(parsed_url1["path"]),
    #             get_path_length(parsed_url2["path"])
    #         )
    #     )
    # else:
    #     return (
    #         (
    #             get_path_similarity(
    #                 path1=parsed_url1["path"],
    #                 path2=parsed_url2["path"]
    #             ) /
    #             get_path_length(parsed_url1["path"])
    #             -
    #             get_last_part_of_paths_num_difs(
    #                 path1=parsed_url1['path'],
    #                 path2=parsed_url2['path']) / get_path_length(parsed_url1["path"])
    #         )
    #         / NUM_DIFS_PENALTY
    #     )

    return get_path_similarity_score(path1=parsed_url1['path'], path2=parsed_url2['path'])


def should_evaluate_url(url1: url_string, url2: url_string, threshold: float = globals.URL_SIMILARITY_THRESHOLD):
    return get_link_similarity(url1=url1, url2=url2) < threshold


MAX_FLOAT_DIF_THRESHOLD: float = .0001


# def test_url_differences_algorithm(url1: url_string, url2: url_string, expected_result: Any) -> bool:
#     return abs((get_link_similarity(url1, url2)) - expected_result) < MAX_FLOAT_DIF_THRESHOLD


# def test_function(expected_result: Any, result_evaluation_algorithm: function, function_to_test: function, *args: Any):


def test_function(
    expected_result: Any,
    result_evaluation_algorithm: Callable[[Any, Any], bool],
    function_to_test: Callable[..., Any],
    *args: Any
):
    print(f'Args = {args}')

    function_result: Any = function_to_test(*args)
    function_works: bool = result_evaluation_algorithm(
        function_result, expected_result)

    print(f'Passed in function {function_to_test.__name__} with arguments: {
          args}\tResult: {function_result}')

    print(f'Expected result {expected_result}, equality checked by {
          result_evaluation_algorithm.__name__}')

    if function_works:
        print(f'{function_to_test.__name__} returned the CORRECT correct result\tFunction works: {
              function_works}', file=sys.stderr)
    else:
        print(f'{function_to_test.__name__} returned the RIGHT correct result\tFunction works: {
              function_works}', file=sys.stderr)

    return function_works


if __name__ == "__main__":
    print(f'Attempting to run url checking function')

    test_url1: url_string = "https://blogboard.io/blog/knowledge/python-print-to-stderr/"
    test_url2: url_string = "https://blogboard.io/blog/knowledge/python-print-to-stderr/"

    # should return 1 for the next link, passing in the same set of two links
    test_function(1, lambda a, b: abs(a - b) < MAX_FLOAT_DIF_THRESHOLD,
                  get_link_similarity, test_url1, test_url1)
