import json 

# repetitive code that only changes 1 line, but can be refactor later 
filename = 'data.json'

def update_unique_urls():
    with open(filename, 'r+') as f:
        content = json.load(f)
        content['unique_urls'] += 1

        f.seek(0)
        json.dump(content, f)
        f.truncate()


def update_longest_page_url(url):
    with open(filename, 'r+') as f:
        content = json.load(f)
        content['longest_page']['url'] = url 

        f.seek(0)
        json.dump(content, f)
        f.truncate()

def update_longest_page_wc(count):
    with open(filename, 'r+') as f:
        content = json.load(f)
        content['longest_page']['word_count'] = count

        f.seek(0)
        json.dump(content, f)
        f.truncate()

def update_word_frequencies(word):
    with open(filename, 'r+') as f:
        content = json.load(f)
        content['word_frequencies'][word] = content['word_frequencies'].get(word, 0) + 1

        f.seek(0)
        json.dump(content, f)
        f.truncate()

def reset_json():
    content = {
        "unique_urls": 0,
        "longest_page": {
            "url": "",
            "word_count": 0
        },
        "word_frequencies": {}
    }

    with open(filename, 'w') as f:
        json.dump(content, f)


