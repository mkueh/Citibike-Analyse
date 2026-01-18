#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import List, Sequence

import folium
import polars as pl
from shapely.geometry import Polygon

from objects.enriched_crash_cluster import EnrichedCrashCluster
from utility.generator_enriched_cluster import GeneratorEnrichedCluster
from utility.calc_length import LengthCalculator
from utility.load_precomputed_routes import load_precomputed_routes_pickle
from utility.load_crashdata import LoadCrashData
from objects.bbox import BBox

# Algorithm parameters
ROOT = Path(__file__).resolve().parents[1]
RAW_NYPD = ROOT / "raw_data" / "nypd"
PRECOMPUTED_ROUTES_PATH = ROOT / "outputs" / "precomputed_routes_500.pkl"
CLUSTER_BUFFER_M_DEFAULT = 50.0
CLUSTER_MAX_SIZE_DEFAULT = 10
CLUSTER_MAX_DIST_DEFAULT = 50.0
OUTPUT_DEFAULT = ROOT / "outputs" / "ride_crash_map.html"

# Map settings
PRINT_ONLY_CLUSTERS_IF_INTERSECTED = True
PRINT_ROUTES = False

def polygon_to_geojson(poly: Polygon) -> dict:
    """Convert shapely polygon to minimal GeoJSON dict."""
    return {
        "type": "Feature",
        "properties": {},
        "geometry": poly.__geo_interface__,
    }

def make_map(
    rides: pl.DataFrame,
    clusters: List[EnrichedCrashCluster],
    routes: Sequence[Sequence[tuple[float, float]]],
    lengths: Sequence[float],
    output: Path,
):
    """Render crash buffers and (optionally) routes into a Folium map."""
    center_lat = rides["start_lat"].mean() # type: ignore
    center_lng = rides["start_lng"].mean() # type: ignore
    fmap = folium.Map(location=[center_lat, center_lng], zoom_start=12, tiles="cartodbpositron") # type: ignore

    # Crash areas layer
    cluster_marker = []
    crash_buffer_print = []

    for idx, cluster in enumerate(clusters):
        centroid_lng, centroid_lat = cluster.centroid.x, cluster.centroid.y
        count = cluster.count
        intersection_count = cluster.rides_intersection_count
        crash_per_100k_rides = cluster.crash_per_rides
        max_dist = cluster.max_dist
        if PRINT_ONLY_CLUSTERS_IF_INTERSECTED and intersection_count > 0:
            marker = folium.CircleMarker(
                location=[centroid_lat, centroid_lng],
                radius=3,
                color="#c0392b",
                fill=True,
                fill_opacity=0.9,
                popup=folium.Popup(
                    f"Cluster crashes: {count}<br/>Max dist to centroid: {max_dist:.1f} m (buffer #{idx+1})<br/>Intersections: {intersection_count}<br/>Crashes per ride: {crash_per_100k_rides:.2f}",
                    max_width=220,
                ),
            )
            cluster_marker.append(marker)
            crash_buffer_print.append(polygon_to_geojson(cluster.buffer))
        elif not PRINT_ONLY_CLUSTERS_IF_INTERSECTED:
            marker = folium.CircleMarker(
                location=[centroid_lat, centroid_lng],
                radius=3,
                color="#c0392b",
                fill=True,
                fill_opacity=0.9,
                popup=folium.Popup(
                    f"Cluster crashes: {count}<br/>Max dist to centroid: {max_dist:.1f} m (buffer #{idx+1})<br/>Intersections: {intersection_count}<br/>Crashes per ride: {crash_per_100k_rides:.2f}",
                    max_width=220,
                ),
            )
            
            cluster_marker.append(marker)
            crash_buffer_print.append(polygon_to_geojson(cluster.buffer))

    crash_geojson = {"type": "FeatureCollection", "features": crash_buffer_print}
    folium.GeoJson(
        crash_geojson,
        name=f"Crash areas ({CLUSTER_BUFFER_M_DEFAULT}m buffer)",
        style_function=lambda _: {"color": "#c0392b", "fillColor": "#e74c3c", "fillOpacity": 0.15, "weight": 1},
    ).add_to(fmap)
    
    # popup fix
    for marker in cluster_marker:
        marker.add_to(fmap)

    if PRINT_ROUTES:
        # Routes and markers
        for idx, (row, coords, length) in enumerate(
            zip(rides.iter_rows(named=True), routes, lengths), start=1
        ):
            start = (row["start_lat"], row["start_lng"])
            end = (row["end_lat"], row["end_lng"])
            coords_to_plot = coords if coords else [start, end]
            duration_h = None
            if row.get("ended_at") and row.get("started_at"):
                duration_h = (row["ended_at"] - row["started_at"]).total_seconds() / 3600
            duration_min = duration_h * 60 if duration_h is not None else None
            speed_kmh = (length / 1000 / duration_h) if duration_h and duration_h > 0 else None
            speed_text = f"{speed_kmh:.2f} km/h" if speed_kmh is not None else "n/a"
            dur_text = f"{duration_min:.1f} min" if duration_min is not None else "n/a"
            popup_html = f"""
            <b>Ride {row['ride_id']}</b><br/>
            Length: {length/1000:.2f} km<br/>
            Duration: {dur_text}<br/>
            Speed: {speed_text}
            """
            folium.PolyLine(
                locations=coords_to_plot,
                color="#2980b9",
                weight=3,
                opacity=0.7,
                tooltip=f"Ride {row['ride_id']} | {length/1000:.2f} km",
                popup=folium.Popup(popup_html, max_width=300),
            ).add_to(fmap)
            folium.CircleMarker(location=start, radius=3, color="#27ae60", fill=True, fill_opacity=0.9).add_to(fmap)
            folium.CircleMarker(location=end, radius=3, color="#f39c12", fill=True, fill_opacity=0.9).add_to(fmap)
            if idx % 25 == 0:
                print(f"Rendered {idx} rides...")

    folium.LayerControl().add_to(fmap)
    output.parent.mkdir(parents=True, exist_ok=True)
    fmap.save(str(output))
    print(f"Wrote map to {output}")


def main():
    """Load precomputed routes, cluster crashes, and write crash/route map."""
    # Lade Daten
    pickle_path = PRECOMPUTED_ROUTES_PATH
    if pickle_path.exists():
        pre = load_precomputed_routes_pickle(pickle_path)
    else:
        raise FileNotFoundError("No precomputed routes found (pickle or parquet).")
    rides = pre.df

    # BBox from rides (used for crash filter)
    top = max(rides["start_lat"].max(), rides["end_lat"].max())  # type: ignore
    bottom = min(rides["start_lat"].min(), rides["end_lat"].min())  # type: ignore
    right = max(rides["start_lng"].max(), rides["end_lng"].max())  # type: ignore
    left = min(rides["start_lng"].min(), rides["end_lng"].min())  # type: ignore
    bbox = BBox(north=top, south=bottom, east=right, west=left)

    start_min = rides["started_at"].min()  # type: ignore
    end_max = rides["ended_at"].max() if "ended_at" in rides.columns else rides["started_at"].max()  # type: ignore

    # Crash buffers im metrischen CRS clustern (meter Abstände korrekt) und für die Karte nach WGS84 zurücktransformieren
    crash_loader = LoadCrashData(RAW_NYPD, cluster_buffer_m=CLUSTER_BUFFER_M_DEFAULT, cluster_max_size=CLUSTER_MAX_SIZE_DEFAULT, cluster_max_dist_m=CLUSTER_MAX_DIST_DEFAULT)
    clusters = crash_loader.load_crash_cluster(start_min, end_max, bbox=bbox)

    # Precomputed routes liegen als (lon, lat) Paare; für Folium drehen
    routes = pre.routes if pre.routes else []
    if not routes:
        raise ValueError("No route geometries present in precomputed data.")
    routes_latlon = [[(lat, lon) for lon, lat in coords] if coords else [] for coords in routes]

    lengths = []
    for coords, row in zip(routes_latlon, rides.iter_rows(named=True)):
        length = LengthCalculator.calc_lengths(coords)
        lengths.append(length)

    enriched_cluster = GeneratorEnrichedCluster(
        buffers=[cluster.buffer for cluster in clusters],
        routes=[coords for coords in routes],
    ).generate_enriched_clusters(
        crash_clusters=clusters,
    )

    make_map(rides,  enriched_cluster,
             routes_latlon, lengths, OUTPUT_DEFAULT)
    
if __name__ == "__main__":
    main()
