from enum import Enum
from pathlib import Path
import osmnx as ox
import networkx as nx

from objects.bbox import BBox
from utility.logic_traffic_network.caching.cache_entry import CacheStage
from utility.logic_traffic_network.caching.graph_cache import GraphCache
from utility.logic_traffic_network.caching.graph_cropper import GraphCropper
from utility.logic_traffic_network.edge_processing.edge_filter import EdgeFilter
from utility.logic_traffic_network.edge_processing.edge_ferry import EdgeFerry


class LoadTrafficNetwork:
    """Load and cache OSM traffic graphs (bike) with optional ferry augmentation."""

    RAW_CACHE_VERSION = "1"
    PROCESSED_CACHE_VERSION = "2"

    def __init__(self, cache_dir: Path) -> None:
        """Set up graph caches."""
        self.raw_cache = GraphCache(cache_dir / "raw", CacheStage.RAW, self.RAW_CACHE_VERSION)
        self.processed_cache = GraphCache(
            cache_dir / "processed", CacheStage.PROCESSED, self.PROCESSED_CACHE_VERSION
        )

    def load_bike_network_graph_from_bbox(self, bbox: BBox) -> nx.MultiDiGraph:
        """Load processed bike graph for bbox (with caching)."""
        return self._load_graph(bbox, "bike")

    def _load_graph(self, bbox: BBox, network_type: str, custom_filter: str = "") -> nx.MultiDiGraph:
        """Load graph (processed) for bbox/network/filter, printing stats."""
        print(f"Loading graph for bbox {bbox}")
        processed_graph = self._load_processed_graph(bbox, network_type, custom_filter)
        print(f"Loaded graph with {len(processed_graph.nodes)} nodes and {len(processed_graph.edges)} edges")
        return processed_graph

    def _load_processed_graph(self, bbox: BBox, network_type: str, custom_filter: str) -> nx.MultiDiGraph:
        """Return processed graph, building/caching as needed."""
        def build_processed() -> nx.MultiDiGraph:
            raw_graph = self._load_raw_graph(bbox, network_type, custom_filter)
            return self.graph_postprocessing(raw_graph)

        return self.processed_cache.fetch(
            bbox=bbox,
            network_type=network_type,
            custom_filter=custom_filter,
            build_fn=build_processed,
            crop_fn=GraphCropper.crop_and_prune_largest,
        )

    def _load_raw_graph(self, bbox: BBox, network_type: str, custom_filter: str) -> nx.MultiDiGraph:
        """Return raw graph, building/caching as needed."""
        def build_raw() -> nx.MultiDiGraph:
            return self._load_graph_from_osmnx(bbox, network_type, custom_filter)

        return self.raw_cache.fetch(
            bbox=bbox,
            network_type=network_type,
            custom_filter=custom_filter,
            build_fn=build_raw,
            crop_fn=GraphCropper.crop_to_bbox,
        )

    def _load_graph_from_osmnx(self, bbox: BBox, network_type: str, custom_filter: str = "") -> nx.MultiDiGraph:
        """Download graph from OSMnx, then merge ferry edges."""
        base_graph = ox.graph_from_bbox(
            bbox=bbox.to_tuple(), network_type=network_type, simplify=True, custom_filter=custom_filter
        )
        return self._add_ferry_edges(base_graph, bbox)

    def _add_ferry_edges(self, graph: nx.MultiDiGraph, bbox: BBox) -> nx.MultiDiGraph:
        """
        Fetch route=ferry ways in the bbox and merge them into the base graph.
        """
        try:
            ferry_graph = ox.graph_from_bbox(
                bbox=bbox.to_tuple(), network_type="all", simplify=True, custom_filter='["route"="ferry"]'
            )
            if len(ferry_graph.edges) == 0:
                return graph
            for _, _, _, data in ferry_graph.edges(keys=True, data=True):
                data["route"] = data.get("route", "ferry")
                data["highway"] = data.get("highway", "ferry")
                data["ferry"] = True
            merged = nx.compose(graph, ferry_graph)
            merged.graph["crs"] = graph.graph.get("crs", ferry_graph.graph.get("crs"))
            print(f"Added ferry edges: {len(ferry_graph.edges)}")
            return merged
        except Exception as e:
            print(f"Failed to add ferry edges: {e}")
            return graph

    def graph_postprocessing(self, graph: nx.MultiDiGraph) -> nx.MultiDiGraph:
        """Clean, simplify, and project graph; keep largest component in WGS84."""
        # Clean topology in projected CRS, then switch back to WGS84 for mapping/folium
        graph_projected = ox.project_graph(graph)
        graph_projected = ox.consolidate_intersections(graph_projected, tolerance=3, rebuild_graph=True)  # type: ignore
        graph_projected = EdgeFerry.connect_ferry_terminals(graph_projected)

        nodes_gdf, edges_gdf = ox.graph_to_gdfs(graph_projected)
        edges_gdf = EdgeFilter.filter_street_edges(edges_gdf)
        edges_gdf = EdgeFilter.dissolve_bidirectional_edges(edges_gdf)

        nodes_latlon = nodes_gdf.to_crs(epsg=4326)
        nodes_latlon["x"] = nodes_latlon.geometry.x
        nodes_latlon["y"] = nodes_latlon.geometry.y
        edges_latlon = edges_gdf.to_crs(epsg=4326)

        graph_latlon = ox.graph_from_gdfs(nodes_latlon, edges_latlon)
        graph_latlon.graph["crs"] = "epsg:4326"
        largest_nodes = self._largest_component_nodes(graph_latlon)
        graph_latlon = graph_latlon.subgraph(largest_nodes).copy()
        graph_latlon.graph["crs"] = "epsg:4326"
        return graph_latlon

    def print_traffic_network_in_folium_map(self, graph: nx.MultiDiGraph):
        """Render graph edges in a Folium map (debug helper)."""
        import folium
        from folium.plugins import Fullscreen

        nodes = ox.graph_to_gdfs(graph, edges=False)
        center_lat = nodes["y"].mean()
        center_lon = nodes["x"].mean()

        fmap = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles="cartodbpositron")
        Fullscreen().add_to(fmap)

        edges = ox.graph_to_gdfs(graph, nodes=False)
        for _, row in edges.iterrows():
            coords = [(y, x) for x, y in row["geometry"].coords]
            folium.PolyLine(
                locations=coords,
                color="#34495e",
                weight=2,
                opacity=0.7,
            ).add_to(fmap)

        fmap.show_in_browser()

    def _largest_component_nodes(self, graph: nx.MultiDiGraph):
        """Return node set of largest weakly connected component."""
        if graph.is_directed():
            components = nx.weakly_connected_components(graph)
        else:
            components = nx.connected_components(graph)
        return max(components, key=len)
