import re
from typing import Any, Callable, TypeAlias
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from bs4 import BeautifulSoup
import random
from collections import deque
import globals
from globals import (Token, Token_Tuple, HASH, url_string)


# Global data structures


# Define allowed domains and paths


def tokenize(text_content: str):
    tokens = []
    token_chars = []
    skipping_long_token = False  # Flag to indicate if we are skipping a long token

    try:
        for char in text_content:
            # Only consider ASCII alphanumeric characters.
            if char.isascii() and char.isalnum():
                if not skipping_long_token:
                    token_chars.append(char.lower())
                    if len(token_chars) > globals.MAX_TOKEN_LENGTH:
                        # Token is too long, skip it
                        token_chars = []
                        skipping_long_token = True  # Start skipping the rest of this token
            else:
                if token_chars:
                    token = ''.join(token_chars)
                    tokens.append(token)
                    token_chars = []
                # Reset the skipping flag after non-alphanumeric character
                skipping_long_token = False

        # In case the text ends while we're in the middle of a token
        if token_chars and not skipping_long_token:
            token = ''.join(token_chars)
            tokens.append(token)

    except Exception as e:
        print(f"Unexpected error occurred during tokenization: {e}")

    return tokens

# Define the function to compute word frequencies


def compute_word_frequencies(tokens):
    global word_frequencies

    for token in tokens:
        word_frequencies[token] += 1

# Define the function to filter out stop words


def filter_stop_words(tokens):
    # Define a list of English stop words
    stop_words = set([
        'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an',
        'and', 'any', 'are', 'as', 'at', 'be', 'because', 'been', 'before',
        'being', 'below', 'between', 'both', 'but', 'by', 'could', 'did', 'do',
        'does', 'doing', 'down', 'during', 'each', 'few', 'for', 'from',
        'further', 'had', 'has', 'have', 'having', 'he', 'her', 'here', 'hers',
        'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in', 'into',
        'is', 'it', "it's", 'its', 'itself', 'just', 'me', 'more', 'most',
        'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only',
        'or', 'other', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 's',
        'same', 'she', "she's", 'should', 'so', 'some', 'such', 't', 'than',
        'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves',
        'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to',
        'too', 'under', 'until', 'up', 'very', 'was', 'we', 'were', 'what',
        'when', 'where', 'which', 'while', 'who', 'whom', 'why', 'will', 'with',
        'you', 'your', 'yours', 'yourself', 'yourselves'
    ])

    filtered_tokens = [token for token in tokens if token not in stop_words]
    return filtered_tokens

# Define the is_valid function to filter URLs


def is_valid(url):
    try:
        # Remove fragment, if any
        url, _ = urldefrag(url)

        parsed = urlparse(url)

        # Check if the scheme is http or https
        if parsed.scheme not in {"http", "https"}:
            return False

        # Extract components
        netloc = parsed.netloc.lower()
        path = parsed.path.lower()
        query = parsed.query.lower()

        # Check if the netloc is one of the allowed domains
        if any(domain in netloc for domain in globals.allowed_domains):
            # Additional check for today.uci.edu
            if "today.uci.edu" in netloc:
                if not path.startswith(globals.today_uci_edu_path):
                    return False

            # Exclude disallowed domains
            disallowed_domains = {'gitlab.ics.uci.edu',
                                  'swiki.ics.uci.edu', 'wiki.ics.uci.edu'}
            if netloc in disallowed_domains:
                return False

            # Exclude URLs with disallowed file extensions
            if  (re.search(r".*(search|login|logout|api|admin|raw|static|calendar|event).*",parsed.path.lower()) or
                re.search(r".*(page|p)/?d+", parsed.path.lower()) or
                re.search(r".*(sessionid|sid|session)=[\w\d]{32}.*",parsed.query.lower()) or
                re.match(
                r".*\.(css|js|bmp|gif|jpe?g|ico"
                r"|png|tiff?|mid|mp2|mp3|mp4"
                r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
                r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
                r"|epub|dll|cnf|tgz|sha1"
                r"|thmx|mso|arff|rtf|jar|csv"
                    r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", path)):
                return False

            # Exclude URLs with excessive query parameters
            if len(parse_qs(query)) > 2:
                return False

            # Exclude URLs with specific query parameters
            disallowed_params = {'do', 'tab_details',
                                 'tab_files', 'image', 'ns'}
            query_params = set(parse_qs(query).keys())
            if disallowed_params.intersection(query_params):
                return False

            # Limit the number of query parameters
            if len(query_params) > 2:
                return False

            # Exclude URLs with repetitive patterns
            if has_repetitive_pattern(url):
                return False

            return True
        else:
            return False

    except TypeError:
        print("TypeError for ", url)
        return False


def has_repetitive_pattern(url):
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    segments = path.split('/')

    # Check for repetition in path segments
    if len(segments) != len(set(segments)):
        return True

    # Check for repetitive query parameters
    query = parsed.query
    params = parse_qs(query)
    if len(params) != len(set(params)):
        return True

    return False

# Define the function to extract next links from the page


def extract_next_links(url, resp):
    links = []

    # Check if the response is valid
    if resp.status != 200 or resp.raw_response is None:
        return links

    # Check if the Content-Type is text/html
    content_type = resp.raw_response.headers.get('Content-Type', '').lower()
    if not content_type or not content_type.startswith('text/html'):
        return links

    try:
        # Parse the HTML content
        soup = BeautifulSoup(resp.raw_response.content, 'lxml')

        # Find all <a> tags with href attributes
        for tag in soup.find_all('a', href=True):
            href = tag.get('href')

            if href:
                # Ignore JavaScript links
                if href.lower().startswith('javascript:'):
                    continue

                # Resolve relative URLs to absolute URLs
                absolute_url = urljoin(url, href)

                # Remove fragment identifiers
                absolute_url, _ = urldefrag(absolute_url)

                # Check if the URL has a valid scheme
                parsed_href = urlparse(absolute_url)
                if parsed_href.scheme in {'http', 'https'}:
                    links.append(absolute_url)

    except Exception as e:
        print(f"Error parsing {url}: {e}")

    return links
# Define the main scraper function


def n_gram(token_list: list[Token], n_grams: int = globals.DEFAULT_N_GRAM_SIZE) -> list[Token_Tuple]:
    # determine what percentage of the document to select
    # set the value between 0 and 1
    AMOUNT_OF_LIST_TO_SELECT: float = 1

    tuple_list: list[tuple] = list()
    for i in range(0, len(token_list), n_grams):
        # curr_tuple: tuple[Token, Token, Token] = tuple()
        curr_list_of_elements: list[Token] = list()
        if (random.random() <= AMOUNT_OF_LIST_TO_SELECT):
            for j in range(i, min(i + n_grams, len(token_list))):
                curr_list_of_elements.append(token_list[j])

            resultant_n_tuple: Token_Tuple = tuple(curr_list_of_elements)
            print(f'Appending the tuple : {resultant_n_tuple}')
            tuple_list.append(resultant_n_tuple)

    return set(tuple_list)


def create_list_of_n_gram_hashes(tuple_list: list[tuple[Token]]) -> list[HASH]:
    resultant_hash_list: list[HASH] = list()
    for token_tuple in tuple_list:
        resultant_hash_list.append(hash(token_tuple))

    return resultant_hash_list


def make_set_of_n_gram_hashes(tuple_list: list[tuple[Token]]) -> list[HASH]:
    set(create_list_of_n_gram_hashes(tuple_list=tuple_list))


def get_similarity_score(n_gram_hash1: set[HASH], n_gram_hash2: set[HASH]) -> float:
    # returns a score between 0 and 1
    intersection_length: int = len(n_gram_hash1.intersection(n_gram_hash2))
    union_length: int = len(n_gram_hash1.union(n_gram_hash2))
    return intersection_length / union_length


def should_evaluate_based_on_similarity_score(n_grams_list: list[set[HASH]], n_gram_hash1: set[HASH], max_allowed_score: float = globals.MAX_ALLOWED_SIMILARITY) -> float:
    for curr_n_gram_hash in n_grams_list:
        if get_similarity_score(n_gram_hash1=n_gram_hash1, n_gram_hash2=curr_n_gram_hash) > max_allowed_score:
            return False

    return True


def go_thru_n_gram_phase(token_list: list[Token]) -> bool:
    tuple_list: list[Token_Tuple] = n_gram(token_list=token_list)
    hashed_tuple: set[HASH] = make_set_of_n_gram_hashes(tuple_list=tuple_list)
    should_read = globals.read_n_gram_hash_list(
        should_evaluate_based_on_similarity_score, hashed_tuple)

    return should_read


def scraper(url, resp):
    global unique_urls, longest_page, subdomains

    # Defragment the URL
    url, _ = urldefrag(url)

    # Check if the URL is unique
    if url in unique_urls:
        return []
    unique_urls.add(url)

    # Update subdomains count
    parsed_url = urlparse(url)
    if "uci.edu" in parsed_url.netloc:
        subdomain = parsed_url.netloc.lower()
        subdomains[subdomain] += 1

    should_go_thru_website: bool = True

    # Process the page content if the response is valid
    if resp.status == 200 and resp.raw_response is not None:
        content_type = resp.raw_response.headers.get(
            'Content-Type', '').lower()
        if content_type and content_type.startswith('text/html'):
            try:
                # Parse the HTML content
                soup = BeautifulSoup(resp.raw_response.content, 'lxml')

                # Extract visible text from the page
                text = soup.get_text(separator=' ', strip=True)

                # Tokenize the text content
                tokens = tokenize(text)

                # Remove stop words from tokens
                filtered_tokens = filter_stop_words(tokens)

                # should_go_thru_website = go_thru_n_gram_phase(filtered_tokens)

                if should_go_thru_website:
                    # Compute word frequencies
                    compute_word_frequencies(filtered_tokens)

                    # Update longest page
                    word_count = len(filtered_tokens)
                    if word_count > longest_page['word_count']:
                        longest_page['word_count'] = word_count
                        longest_page['url'] = url

            except Exception as e:
                print(f"Error processing content from {url}: {e}")
    valid_links = list()
    if should_go_thru_website:
        # Extract next links
        links = extract_next_links(url, resp)

        # Filter links using is_valid
        valid_links = [link for link in links if is_valid(link)]

    return valid_links


# Testing purposes:
if __name__ == "__main__":
    # Print total unique pages
    print(f"Total unique pages: {len(globals.unique_urls)}")

    # Print the longest page info
    print(f"Longest page URL: {globals.longest_page['url']}")
    print(f"Longest page word count: {globals.longest_page['word_count']}")

    # Print top 50 words
    sorted_words = sorted(globals.word_frequencies.items(),
                          key=lambda item: item[1], reverse=True)
    top_50_words = sorted_words[:50]
    print("Top 50 words:")
    for word, freq in top_50_words:
        print(f"{word}: {freq}")

    # Print subdomains
    sorted_subdomains = sorted(globals.subdomains.items())
    print("Subdomains:")
    for subdomain, count in sorted_subdomains:
        print(f"{subdomain}, {count}")
