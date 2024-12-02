#import globals 
import nltk
#nltk.download('wordnet')
lemma = nltk.wordnet.WordNetLemmatizer()

MAX_TOKEN_LENGTH = 10000


def tokenize(text_content: str):
    tokens = []
    token_chars = []
    skipping_long_token = False  # Flag to indicate if we are skipping a long token

    try:
        for char in text_content:
            # Only consider ASCII alphanumeric characters.
            if char.isascii() and char.isalnum() or char == '\'':
                if not skipping_long_token:
                    token_chars.append(char.lower())
                    if len(token_chars) > MAX_TOKEN_LENGTH:
                        # Token is too long, skip it
                        token_chars = []
                        skipping_long_token = True  # Start skipping the rest of this token
            else:
                if token_chars:
                    token = ''.join(token_chars)
                    token = token.strip(" '")       #added strip to tokenizer
                    tokens.append(lemma.lemmatize(token))
                    #tokens.append(token)
                    token_chars = []
                # Reset the skipping flag after non-alphanumeric character
                skipping_long_token = False

        # In case the text ends while we're in the middle of a token
        if token_chars and not skipping_long_token:
            token = ''.join(token_chars)
            token = token.strip(" '")       #added strip to tokenizer
            tokens.append(lemma.lemmatize(token))
            #tokens.append(token)

    except Exception as e:
        print(f"Unexpected error occurred during tokenization: {e}")

    return tokens