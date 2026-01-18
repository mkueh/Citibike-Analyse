from __future__ import annotations

from hashlib import md5
import pickle
from pathlib import Path
from typing import Callable, Iterable, Optional
import networkx as nx
import osmnx as ox

from objects.bbox import BBox
from utility.logic_traffic_network.caching.cache_entry import CacheStage, GraphCacheEntry


CropFn = Callable[[nx.MultiDiGraph, BBox], nx.MultiDiGraph]
BuildFn = Callable[[], nx.MultiDiGraph]


class GraphCache:
    """
    Caches graphml files for raw and processed stages and supports reusing a larger
    cached graph by cropping to a smaller bbox.
    """

    def __init__(self, cache_dir: Path, stage: str, version: str) -> None:
        self.cache_dir = cache_dir
        self.stage = stage
        self.version = version
        self.entries: list[GraphCacheEntry] = []
        self._load_entries()

    def fetch(
        self,
        bbox: BBox,
        network_type: str,
        custom_filter: str,
        build_fn: BuildFn,
        crop_fn: CropFn,
    ) -> nx.MultiDiGraph:
        existing = self._find_covering_entry(bbox, network_type, custom_filter)
        if existing:
            graph = existing.load_graph()
            if not existing.bbox.equals(bbox):
                graph = crop_fn(graph, bbox)
                print(f"Using cached {self.stage} graph (bbox {existing.bbox}) and cropping to {bbox}")
                return self._store_graph(graph, bbox, network_type, custom_filter)
            print(f"Using cached {self.stage} graph for bbox {bbox}")
            return graph

        graph = build_fn()
        print(f"Caching new {self.stage} graph for bbox {bbox}")
        return self._store_graph(graph, bbox, network_type, custom_filter)

    def _store_graph(
        self,
        graph: nx.MultiDiGraph,
        bbox: BBox,
        network_type: str,
        custom_filter: str,
    ) -> nx.MultiDiGraph:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        hash_value = md5(
            f"{self.stage}_{self.version}_{bbox.west}_{bbox.south}_{bbox.east}_{bbox.north}_{network_type}_{custom_filter}".encode()
        ).hexdigest()
        graph_path = self.cache_dir / f"{hash_value}.graphml"
        ox.save_graphml(graph, graph_path)
        entry = GraphCacheEntry(
            stage=self.stage,
            bbox=bbox,
            network_type=network_type,
            custom_filter=custom_filter,
            version=self.version,
            graph_path=graph_path,
        )
        pickle.dump(entry, open(graph_path.with_suffix(".cacheinfo"), "wb"))
        self.entries.append(entry)
        return graph

    def _find_covering_entry(
        self, bbox: BBox, network_type: str, custom_filter: str
    ) -> Optional[GraphCacheEntry]:
        candidates: Iterable[GraphCacheEntry] = (
            e
            for e in self.entries
            if e.stage == self.stage
            and e.version == self.version
            and e.network_type == network_type
            and e.custom_filter == custom_filter
            and e.contains(bbox)
        )
        best: Optional[GraphCacheEntry] = None
        for entry in candidates:
            if best is None or entry.area() < best.area():
                best = entry
        return best

    def _load_entries(self) -> None:
        if not self.cache_dir.exists():
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        for cache_file in self.cache_dir.glob("*.cacheinfo"):
            try:
                entry: GraphCacheEntry = pickle.load(open(cache_file, "rb"))
                if entry.version != self.version or entry.stage != self.stage:
                    continue
                self.entries.append(entry)
            except Exception as e:
                print(f"Failed to load cache entry {cache_file}: {e}")
