#!/usr/bin/env python3
"""
Visualisiere vorcomputierte Routen.
Primär unterstützt Pickle-Output aus route_precompute_routes.py; fällt zurück auf Parquet-Metadaten.
Alle Routen werden gezeichnet; Crash-Treffer (falls Flag vorhanden) hervorgehoben.
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import folium
import polars as pl
from folium.plugins import Fullscreen

from utility.load_precomputed_routes import load_precomputed_routes_pickle
from utility.calc_length import LengthCalculator

INPUT_PICKLE = Path("outputs/precomputed_routes_500.pkl")
OUTPUT_MAP = Path("outputs/precomputed_routes_map.html")


def load_data():
    """Load precomputed routes pickle; raise if missing."""
    if INPUT_PICKLE.exists():
        print(f"Loading routes from {INPUT_PICKLE}")
        return load_precomputed_routes_pickle(INPUT_PICKLE)
    raise FileNotFoundError("No precomputed routes found (pickle).")


def make_map(df: pl.DataFrame, routes: Sequence[Sequence[tuple[float, float]]], output: Path):
    """Render all routes to a Folium map; highlight start/end markers."""
    if df.is_empty():
        raise ValueError("No ride data available to center the map.")
    center_lat = df["start_lat"].mean()  # type: ignore
    center_lng = df["start_lng"].mean()  # type: ignore
    fmap = folium.Map(location=[center_lat, center_lng], zoom_start=12, tiles="cartodbpositron")
    Fullscreen().add_to(fmap)

    for idx, coords in enumerate(routes):
        length = LengthCalculator.calc_lengths(coords)
        latlon = [(lat, lon) for lon, lat in coords]
        start_time = df["started_at"][idx]
        end_time = df["ended_at"][idx]
        color = "#1e00ff"
        popup_parts = [
            f"ride_id: {df['ride_id'][idx]}",
            f"length: {length:.1f} m",
            f"duration: {(end_time - start_time).total_seconds()/60:.1f} min" if start_time and end_time else "",
            f"speed: {length/((end_time - start_time).total_seconds()/3600)/1000:.1f} km/h" if start_time and end_time and (end_time - start_time).total_seconds() > 0 else "",
        ]
        popup_html = "<br>".join(part for part in popup_parts if part)
        folium.PolyLine(latlon, color=color, weight=2, opacity=0.7, popup=folium.Popup(popup_html, max_width=300)).add_to(fmap)
        folium.CircleMarker(location=latlon[0], radius=3, color="green", fill=True, fill_color="green", fill_opacity=0.9).add_to(fmap)
        folium.CircleMarker(location=latlon[-1], radius=3, color="red", fill=True, fill_color="red", fill_opacity=0.9).add_to(fmap) 

    output.parent.mkdir(parents=True, exist_ok=True)
    fmap.save(str(output))
    print(f"Wrote map to {output}")


def main():
    """Load precomputed routes and generate map HTML."""
    pre = load_data()
    make_map(pre.df, pre.routes, OUTPUT_MAP)


if __name__ == "__main__":
    main()
