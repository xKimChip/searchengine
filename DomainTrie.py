class TrieNode:
    def __init__(self):
        self.children = {}
        self.is_end_of_domain = False

    def __repr__(self, level=0):
        ret = "\t" * level + \
            ("[END] " if self.is_end_of_domain else "") + "Node\n"
        for key, child in self.children.items():
            ret += "\t" * level + f"- {key} -> " + child.__repr__(level + 1)
        return ret


class DomainTrie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, domain):
        # Split the domain into parts, reversing for easier insertion
        parts = domain.split('.')[::-1]
        current = self.root
        for part in parts:
            if part not in current.children:
                current.children[part] = TrieNode()
            current = current.children[part]
        current.is_end_of_domain = True

    def search(self, domain):
        # Split and reverse the domain as in insert
        parts = domain.split('.')[::-1]
        current = self.root
        for part in parts:
            if part not in current.children:
                return False
            current = current.children[part]
        return current.is_end_of_domain

    def starts_with(self, domain_prefix):
        # Split and reverse the domain prefix
        parts = domain_prefix.split('.')
        print(f' parts = {parts}')
        current = self.root
        for part in parts:
            if part not in current.children:
                return False
            current = current.children[part]
        return True

    def get_num_unique_domains(self) -> int:
        curr_node: TrieNode = self.root
        num_domains_total: int = 0

        def dfs(node: TrieNode):
            count = 1 if node.is_end_of_domain else 0
            for child in node.children.values():
                count += dfs(child)

            return count

        num_domains_total = dfs(curr_node)
        return num_domains_total

    def get_num_unique_subdomains_for_domain(self, node):

        def dfs(node: TrieNode):
            print(f'Subdomains of {node}', end='\t')
            count = 1
            for child in node.children.values():
                count += dfs(child)
            return count
        res = dfs(node)

        print(f'{res}')
        return res

    def __repr__(self):
        return repr(self.root)


if __name__ == "__main__":
    trie = DomainTrie()
    trie.insert("https://chatgpt.com/c/672307c3-53d4-8002-af43-2031ffe19897")
    trie.insert("www.example.com")
    trie.insert("mail.example.com")
    trie.insert("support.example.org")

    print(trie.search("www.example.com"))        # True
    # False (not inserted as full domain)
    print(trie.search("example.com"))
    print(trie.starts_with("example.com"))       # True
    print(trie.starts_with("support.example"))   # False
    print(trie.search("support.example.org"))

    print(f'{trie}')
