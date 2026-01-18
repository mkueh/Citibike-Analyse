import osmnx as ox
import networkx as nx
import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString

from utility.logic_traffic_network.edge_processing.tag_processing import TagProcessing

class EdgeFerry:
    


    @staticmethod
    def is_ferry_edge(row) -> bool:
        if row.get("ferry") is True:
            return True
        route_tags = TagProcessing.normalize_tags(row.get("route"))
        if any(tag.lower() == "ferry" for tag in route_tags):
            return True

        for field in ("highway", "service"):
            tags = TagProcessing.normalize_tags(row.get(field))
            if any("ferry" in tag.lower() for tag in tags):
                return True

        name_val = row.get("name")
        if isinstance(name_val, str) and "ferry" in name_val.lower():
            return True
        return False
    
    @staticmethod
    def connect_ferry_terminals(graph: nx.MultiDiGraph, max_distance: float = 500.0) -> nx.MultiDiGraph:
        """
        Snap ferry terminal nodes to nearest street nodes to ensure reachability.
        """
        ferry_nodes: set[int] = set()
        for u, v, data in graph.edges(data=True):
            if data.get("ferry"):
                ferry_nodes.add(u)
                ferry_nodes.add(v)
        if not ferry_nodes:
            return graph

        nodes_gdf = ox.graph_to_gdfs(graph, edges=False)
        ferry_nodes_gdf = nodes_gdf.loc[nodes_gdf.index.isin(ferry_nodes)]
        street_nodes_gdf = nodes_gdf.loc[~nodes_gdf.index.isin(ferry_nodes)]
        if ferry_nodes_gdf.empty or street_nodes_gdf.empty:
            return graph

        ferry_nodes_gdf = ferry_nodes_gdf.to_crs(graph.graph["crs"])
        street_nodes_gdf = street_nodes_gdf.to_crs(graph.graph["crs"])

        nearest = gpd.sjoin_nearest(
            ferry_nodes_gdf,
            street_nodes_gdf,
            how="left",
            distance_col="dist",
        )
        nearest = nearest[nearest["dist"] <= max_distance]
        if nearest.empty:
            return graph

        right_col = "index_right" if "index_right" in nearest.columns else None
        if right_col is None:
            candidates = [c for c in nearest.columns if c.startswith("index_")]
            if candidates:
                right_col = candidates[0]
        if right_col is None:
            return graph

        for ferry_node, row in nearest.iterrows():
            street_node = row.get(right_col)
            if pd.isna(street_node):
                continue
            street_node = int(street_node)
            dist = float(row["dist"])
            pt_ferry = ferry_nodes_gdf.loc[ferry_node].geometry
            pt_street = street_nodes_gdf.loc[street_node].geometry
            geom = LineString([pt_ferry, pt_street])
            attrs = {"length": dist, "ferry": True, "highway": "ferry_link", "geometry": geom}
            graph.add_edge(ferry_node, street_node, **attrs)
            graph.add_edge(street_node, ferry_node, **attrs)
        return graph
    
