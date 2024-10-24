import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup
from collections import defaultdict

# Global data structures
unique_urls = set()
longest_page = {
    'url': '',
    'word_count': 0
}
word_frequencies = defaultdict(int)
subdomains = defaultdict(int)
MAX_TOKEN_LENGTH = 10000  # Set a reasonable maximum length

# Define allowed domains and paths
allowed_domains = [
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
    "today.uci.edu"
]

today_uci_edu_path = "/department/information_computer_sciences"

# Define the tokenizer function
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
                    if len(token_chars) > MAX_TOKEN_LENGTH:
                        # Token is too long, skip it
                        token_chars = []
                        skipping_long_token = True  # Start skipping the rest of this token
            else:
                if token_chars:
                    token = ''.join(token_chars)
                    tokens.append(token)
                    token_chars = []
                skipping_long_token = False  # Reset the skipping flag after non-alphanumeric character

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

        # Extract the netloc and path
        netloc = parsed.netloc.lower()
        path = parsed.path.lower()

        # Check if the netloc is one of the allowed domains
        if any(domain in netloc for domain in allowed_domains):
            # Additional check for today.uci.edu
            if "today.uci.edu" in netloc:
                if not path.startswith(today_uci_edu_path):
                    return False
            # Exclude URLs with disallowed file extensions
            if re.match(
                r".*\.(css|js|bmp|gif|jpe?g|ico"
                r"|png|tiff?|mid|mp2|mp3|mp4"
                r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
                r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
                r"|epub|dll|cnf|tgz|sha1"
                r"|thmx|mso|arff|rtf|jar|csv"
                r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", path):
                return False
            # URL is valid
            return True
        else:
            # Netloc is not in allowed domains
            return False

    except TypeError:
        print("TypeError for ", url)
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
                    print(absolute_url) # Debug purposes
                    links.append(absolute_url)

    except Exception as e:
        print(f"Error parsing {url}: {e}")

    return links

# Define the main scraper function
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

    # Extract next links
    links = extract_next_links(url, resp)

    # Filter links using is_valid
    valid_links = [link for link in links if is_valid(link)]

    # Process the page content if the response is valid
    if resp.status == 200 and resp.raw_response is not None:
        content_type = resp.raw_response.headers.get('Content-Type', '').lower()
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

                # Compute word frequencies
                compute_word_frequencies(filtered_tokens)

                # Update longest page
                word_count = len(filtered_tokens)
                if word_count > longest_page['word_count']:
                    longest_page['word_count'] = word_count
                    longest_page['url'] = url

            except Exception as e:
                print(f"Error processing content from {url}: {e}")

    return valid_links


# Testing purposes:
if __name__ == "__main__":
    # Print total unique pages
    print(f"Total unique pages: {len(unique_urls)}")
    
    # Print the longest page info
    print(f"Longest page URL: {longest_page['url']}")
    print(f"Longest page word count: {longest_page['word_count']}")
    
    # Print top 50 words
    sorted_words = sorted(word_frequencies.items(), key=lambda item: item[1], reverse=True)
    top_50_words = sorted_words[:50]
    print("Top 50 words:")
    for word, freq in top_50_words:
        print(f"{word}: {freq}")
    
    # Print subdomains
    sorted_subdomains = sorted(subdomains.items())
    print("Subdomains:")
    for subdomain, count in sorted_subdomains:
        print(f"{subdomain}, {count}")
