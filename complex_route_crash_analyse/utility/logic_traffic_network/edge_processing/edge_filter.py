import geopandas as gpd
from shapely.ops import linemerge, unary_union
import pandas as pd

from utility.logic_traffic_network.edge_processing.edge_ferry import EdgeFerry
from utility.logic_traffic_network.edge_processing.tag_processing import TagProcessing

class EdgeFilter:
    
    foot_exclude = {"footway", "path", "steps", "pedestrian", "corridor", "escalator", "elevator", "bridleway", "subway"}
    motorway_exclude = {"motorway", "motorway_link", "trunk", "trunk_link"}
    misc_exclude = {"construction"}
    
    @staticmethod
    def filter_street_edges(edges_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Drop sidewalk/footpath-like features and non-bikeable links (keep ferries).
        """
        if "highway" not in edges_gdf.columns and "route" not in edges_gdf.columns:
            return edges_gdf

        before = len(edges_gdf)
        filtered = edges_gdf[edges_gdf.apply(EdgeFilter._keep_row, axis=1)]
        if before != len(filtered):
            print(f"Filtered non-street/bikeable edges: {before} -> {len(filtered)}")
        return filtered

    @staticmethod
    def _keep_row(row) -> bool:
        ferry = EdgeFerry.is_ferry_edge(row)

        railway_tags = TagProcessing.normalize_tags(row.get("railway"))
        if railway_tags and not ferry:
            return False

        highway_tags = TagProcessing.normalize_tags(row.get("highway"))
        if ferry:
            return True
        if not highway_tags:
            # No highway tag and not a ferry => drop.
            return False
        if any(tag in EdgeFilter.motorway_exclude for tag in highway_tags):
            return False
        if all(tag in EdgeFilter.foot_exclude for tag in highway_tags):
            return False
        if any(tag in EdgeFilter.misc_exclude for tag in highway_tags):
            return False

        bicycle_tags = TagProcessing.normalize_tags(row.get("bicycle"))
        if bicycle_tags and any(tag == "no" for tag in bicycle_tags):
            return False
        return True
    
    @staticmethod
    def dissolve_bidirectional_edges(edges_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        keep_cols = ["u", "v", "geometry", "length"]
        if "ferry" in edges_gdf.columns:
            keep_cols.append("ferry")
        edges_gdf = edges_gdf.reset_index()[keep_cols]
        edges_gdf = gpd.GeoDataFrame(edges_gdf, geometry="geometry", crs=edges_gdf.crs)
        edges_gdf["uv"] = edges_gdf.apply(lambda r: tuple(sorted((r["u"], r["v"]))), axis=1)
        aggfunc = {"ferry": "max", "length": "sum"} if "ferry" in edges_gdf.columns else {"length": "sum"}
        dissolved = edges_gdf.dissolve(by="uv", as_index=False, aggfunc=aggfunc)
        dissolved["geometry"] = dissolved.geometry.map(EdgeFilter._merge_geometry)
        dissolved["length"] = dissolved.geometry.length
        dissolved[["u", "v"]] = pd.DataFrame(dissolved["uv"].tolist(), index=dissolved.index)
        dissolved["key"] = 0
        dissolved = gpd.GeoDataFrame(
            dissolved.drop(columns="uv").set_index(["u", "v", "key"]),
            geometry="geometry",
            crs=edges_gdf.crs,
        )
        return dissolved
    
    @staticmethod
    def _merge_geometry(geom):
        if geom.geom_type == "LineString":
            return geom
        merged = linemerge(unary_union(geom))
        if merged.geom_type == "MultiLineString":
            return max(merged.geoms, key=lambda g: g.length)
        return merged


    
