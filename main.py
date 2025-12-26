# The line `from fastapi import FastAPI, Form` is importing the `FastAPI` class and the `Form` class
# from the `fastapi` module in Python. This allows you to use these classes in your code to create
# FastAPI applications and handle form data in your API endpoints.
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

app = FastAPI()

class Node(BaseModel):
    id: str

class Edge(BaseModel):
    source: str
    target: str

class Pipeline(BaseModel):
    nodes: List[Node]
    edges: List[Edge]

def is_dag(nodes, edges):
    graph = {n.id: [] for n in nodes}
    for e in edges:
        graph[e.source].append(e.target)

    visited = set()
    stack = set()

    def dfs(node):
        if node in stack:
            return False
        if node in visited:
            return True

        stack.add(node)
        for nei in graph.get(node, []):
            if not dfs(nei):
                return False
        stack.remove(node)
        visited.add(node)
        return True

    return all(dfs(n.id) for n in nodes)

@app.post("/pipelines/parse")
def parse_pipeline(pipeline: Pipeline):
    num_nodes = len(pipeline.nodes)
    num_edges = len(pipeline.edges)
    dag = is_dag(pipeline.nodes, pipeline.edges)

    return {
        "num_nodes": num_nodes,
        "num_edges": num_edges,
        "is_dag": dag
    }

