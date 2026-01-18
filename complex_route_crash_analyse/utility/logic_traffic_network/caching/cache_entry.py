from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import osmnx as ox
import networkx as nx

from objects.bbox import BBox


class CacheStage:
    RAW = "raw"
    PROCESSED = "processed"


@dataclass
class GraphCacheEntry:
    stage: str
    bbox: BBox
    network_type: str
    custom_filter: str
    version: str
    graph_path: Path

    def load_graph(self) -> nx.MultiDiGraph:
        return ox.load_graphml(self.graph_path)

    def contains(self, target: BBox) -> bool:
        return self.bbox.contains_bbox(target)

    def area(self) -> float:
        return self.bbox.area()
