from __future__ import annotations

import networkx as nx
from objects.bbox import BBox


class GraphCropper:
    @staticmethod
    def crop_to_bbox(graph: nx.MultiDiGraph, bbox: BBox) -> nx.MultiDiGraph:
        nodes_to_keep = [
            node
            for node, data in graph.nodes(data=True)
            if "x" in data and "y" in data and bbox.contains_point(data["x"], data["y"])
        ]
        subgraph = graph.subgraph(nodes_to_keep).copy()
        if graph.graph.get("crs"):
            subgraph.graph["crs"] = graph.graph["crs"]
        return subgraph

    @staticmethod
    def crop_and_prune_largest(graph: nx.MultiDiGraph, bbox: BBox) -> nx.MultiDiGraph:
        cropped = GraphCropper.crop_to_bbox(graph, bbox)
        if cropped.number_of_nodes() == 0:
            return cropped
        components = (
            nx.weakly_connected_components(cropped)
            if cropped.is_directed()
            else nx.connected_components(cropped)
        )
        largest_nodes = max(components, key=len)
        result = cropped.subgraph(largest_nodes).copy()
        if graph.graph.get("crs"):
            result.graph["crs"] = graph.graph["crs"]
        return result
