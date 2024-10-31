from typing import TypeAlias
import urllib
from globals import url_string
import globals

parsed_url_dict: TypeAlias = dict


def parse_url(url: url_string) -> parsed_url_dict:
    parsed_url = urllib.urlparse(url)
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

    return consecutive_similar


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


def get_link_similarity(url1, url2) -> float:

    parsed_url1: parsed_url_dict = url1
    parsed_url2: parsed_url_dict = url2

    if not confirm_similarities_up_to_path(parsed_url1=parsed_url1, parsed_url2=parsed_url2):
        return 0

    # the rest of this shit is queries and fucking fragments so i don't really care about checking those
    # if the website is the same (or too similar) then the rest of this is going to be the same too and should then also be removed
    # i should also probably add checking for if the difference count of the two paths is too similar
    if get_path_length(parsed_url1["path"]) != get_path_length(parsed_url2["path"]):
        return (
            get_path_similarity(
                path1=parsed_url1["path"],
                path2=parsed_url2["path"]
            ) / min(
                get_path_length(parsed_url1["path"]),
                get_path_length(parsed_url2["path"])
            )
        )
    else:
        return (
            (
                get_path_similarity(
                    path1=parsed_url1["path"],
                    path2=parsed_url2["path"]
                ) /
                get_path_length(parsed_url1["path"])
                +
                get_last_part_of_paths_num_difs(
                    path1=parsed_url1,
                    path2=parsed_url2) / get_path_length(parsed_url1["path"])
            )
            / NUM_DIFS_PENALTY
        )


def should_evaluate_url(url1: url_string, url2: url_string, threshold: float = globals.URL_SIMILARITY_THRESHOLD):
    return get_link_similarity(url1=url1, url2=url2) < threshold
