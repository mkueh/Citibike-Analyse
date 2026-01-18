import math
from typing import Any
from networkx import MultiDiGraph
import osmnx as ox
import networkx as nx
from joblib import Parallel, delayed


class GraphBuilder:
    """Build shortest-path routes on a cached OSM graph."""

    graph: MultiDiGraph
    precomputed_component_map: dict[int, int]
    
    def _init_precomputed_component_map(self):
        """Map node -> weakly connected component id for connectivity checks."""
        self.precomputed_component_map = {}
        for idx, comp in enumerate(nx.weakly_connected_components(self.graph)):
            for n in comp:
                self.precomputed_component_map[n] = idx
    
    def __init__(self, graph: MultiDiGraph):
        """Wrap a MultiDiGraph and precompute connectivity map."""
        self.graph = graph
        self._init_precomputed_component_map()
    
    def build_routes(
        self,
        rides: list[tuple[tuple[float, float], tuple[float, float]]],
        n_jobs: int = -1,
    ):
        """Compute routes for many origin/destination pairs in parallel.

        Args:
            rides: List of ((start_lat, start_lon), (end_lat, end_lon)).
            n_jobs: Parallel jobs for joblib (default -1 = all cores).

        Returns:
            Tuple of (routes, lengths_m, methods) preserving input order.
        """
        total = len(rides)
        results = Parallel(n_jobs=n_jobs, prefer="processes")(
            delayed(self._build_route_task)(idx, total, start, end) for idx, (start, end) in enumerate(rides)
        )
        results = sorted(results, key=lambda r: r[0])
        list_routes = [r[1] for r in results]
        list_lengths = [r[2] for r in results]
        list_methods = [r[3] for r in results]
        return list_routes, list_lengths, list_methods

    def _build_route_task(self, idx: int, total: int, start, end):
        """Joblib task wrapper with logging."""
        print(f"Building route {idx+1}/{total}...")
        route, length, method = self._build_route(start, end)
        return idx, route, length, method

    def _build_route(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
    ) -> tuple[list[tuple[float, float]], float, str] | None:
        """Compute a single route; fallback to straight line if not routable."""
        start_lat, start_lon = start
        end_lat, end_lon = end
        start_x, start_y = start_lon, start_lat
        end_x, end_y = end_lon, end_lat
        
        try:
            s_node = ox.distance.nearest_nodes(self.graph, start_x, start_y)
            e_node = ox.distance.nearest_nodes(self.graph, end_x, end_y)
        except Exception as e:
            print(f"Error finding nearest nodes: {e}")
            length = self._haversine_m(start[0], start[1], end[0], end[1])
            return [start, end], length, "direct_fallback"

        if (
            self.precomputed_component_map.get(s_node) is not None
            and self.precomputed_component_map.get(s_node)
            == self.precomputed_component_map.get(e_node)
        ):
            try:
                route = ox.shortest_path(self.graph, s_node, e_node, weight="length")
                if not route:
                    route = nx.shortest_path(self.graph.to_undirected(), s_node, e_node, weight="length")
                if route:
                    coords_proj, length = self.nodes_to_coords(route)
                    return coords_proj, length, "routed"
            except Exception:
                pass

        length = self._haversine_m(start[0], start[1], end[0], end[1])
        return [(start_lon, start_lat), (end_lon, end_lat)], length, "direct_fallback"

    def _find_closest_node(
        self,
        point: tuple[float, float],
    ) -> Any:
        """Find nearest graph node to (lon, lat) point."""
        x, y = point
        try:
            closest_node = ox.distance.nearest_nodes(self.graph, x, y)
            return closest_node
        except Exception as e:
            print(f"Error finding nearest node: {e}")
            return None
        
    def _check_if_nodes_in_same_component(
        self,
        u_node: Any,
        v_node: Any,
    ) -> bool:
        """Return True if two nodes share the same weak component."""
        comp_u = self.precomputed_component_map.get(u_node)
        comp_v = self.precomputed_component_map.get(v_node)
        return comp_u is not None and comp_u == comp_v

    def nodes_to_coords(self, route_nodes: list[int]) -> tuple[list[tuple[float, float]], float]:
        """Convert node path to (lon, lat) coordinates and total length (meters)."""
        coords_proj = [(self.graph.nodes[route_nodes[0]]["x"],
                        self.graph.nodes[route_nodes[0]]["y"])]
        length_local = 0.0
        for u, v in zip(route_nodes[:-1], route_nodes[1:]):
            edge_datas = self.graph.get_edge_data(u, v)
            if not edge_datas:
                continue
            edge_data = min(edge_datas.values(),
                            key=lambda d: d.get("length", float("inf")))
            length_local_edge = float(edge_data.get("length", 0.0))
            length_local += length_local_edge
            geom = edge_data.get("geometry")
            if geom is not None:
                coords_proj.extend(list(geom.coords)[1:])
            else:
                coords_proj.append((self.graph.nodes[v]["x"], self.graph.nodes[v]["y"]))
        return coords_proj, length_local
    
    def _haversine_m(self, lat1, lng1, lat2, lng2) -> float:
        """Great-circle distance in meters between two lat/lon points."""
        r = 6371000
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlmb = math.radians(lng2 - lng1)
        a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
        return 2 * r * math.asin(math.sqrt(a))
