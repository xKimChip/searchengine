import json 

# repetitive code that only changes 1 line, but can be refactor later 

def update_unique_urls(url):
    with open('data.json', 'r+') as f:
        content = json.load(f)
        content['unique_urls'].append(url)

        f.seek(0)
        json.dump(content, f)
        f.truncate()


def update_longest_page_url(url):
    with open('data.json', 'r+') as f:
        content = json.load(f)
        content['longest_page']['url'] = url 

        f.seek(0)
        json.dump(content, f)
        f.truncate()

def update_longest_page_wc(count):
    with open('data.json', 'r+') as f:
        content = json.load(f)
        content['longest_page']['word_count'] = count

        f.seek(0)
        json.dump(content, f)
        f.truncate()

def update_word_frequencies(word):
    with open('data.json', 'r+') as f:
        content = json.load(f)
        content['word_frequencies'][word] = content['word_frequencies'].get(word, 0) + 1

        f.seek(0)
        json.dump(content, f)
        f.truncate()


