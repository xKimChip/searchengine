def page_rank(graph, damping_factor=0.85, max_iterations=100, tol=1e-6):
    # Step 1: Initialize variables
    nodes = list(graph.keys())
    N = len(nodes)
    ranks = {node: 1 / N for node in nodes}  # Initial PageRank values
    new_ranks = ranks.copy()  # To store updated PageRank values

    # Step 2: Create a set of dangling nodes (nodes with no outgoing links)
    dangling_nodes = {node for node in nodes if not graph[node]}

    # Step 3: Iteratively calculate PageRank
    for iteration in range(max_iterations):
        # Distribute PageRank from dangling nodes
        dangling_rank_sum = sum(ranks[node] for node in dangling_nodes)
        for node in nodes:
            # Base rank contribution
            new_ranks[node] = (1 - damping_factor) / N
            # Contribution from dangling nodes
            new_ranks[node] += damping_factor * dangling_rank_sum / N
            # Contribution from incoming links
            for other_node in nodes:
                if node in graph[other_node]:  # If there's a link from other_node to node
                    new_ranks[node] += damping_factor * ranks[other_node] / len(graph[other_node])

        # Check for convergence
        delta = sum(abs(new_ranks[node] - ranks[node]) for node in nodes)
        ranks = new_ranks.copy()  # Update ranks
        if delta < tol:
            break

    return ranks
