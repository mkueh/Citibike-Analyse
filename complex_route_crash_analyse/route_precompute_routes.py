from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import List

import polars as pl
from pydantic import BaseModel

from utility.load_ridedata import LoadRideData
from objects.bbox import BBox
from logic.graph_builder import GraphBuilder
from utility.load_traffic_network import LoadTrafficNetwork

ROOT = Path(__file__).resolve().parents[1]
RAW_CITI = ROOT / "raw_data" / "citi_bike"
CACHE_DIR = ROOT / "cache" / "traffic_network"
DEFAULT_SETTINGS = Path(__file__).with_name("route_precompute_settings.json")


class PrecomputeSettings(BaseModel):
    """Configuration for precomputing route samples."""
    sample_size: int = 40_000
    bbox_pad: float = 0.02
    random_seed: int = 42
    n_jobs: int = -1
    output_path: Path = ROOT / "cache" / "precomputed_routes_50k.pkl"


def load_settings(path: Path) -> PrecomputeSettings:
    """Load settings from JSON file."""
    data = json.loads(path.read_text())
    return PrecomputeSettings.model_validate(data)


def bbox_from_rides(rides: pl.DataFrame, pad: float) -> BBox:
    """Compute bounding box from ride start/end coords with padding."""
    top = max(rides["start_lat"].max(), rides["end_lat"].max()) + pad  # type: ignore
    bottom = min(rides["start_lat"].min(), rides["end_lat"].min()) - pad  # type: ignore
    right = max(rides["start_lng"].max(), rides["end_lng"].max()) + pad  # type: ignore
    left = min(rides["start_lng"].min(), rides["end_lng"].min()) - pad  # type: ignore
    return BBox(north=top, south=bottom, east=right, west=left)


def main():
    """Sample rides, build routes on OSM graph, and persist pickle payload."""
    settings_path = DEFAULT_SETTINGS
    settings = load_settings(settings_path)

    rides = LoadRideData(RAW_CITI).sample_rides(settings.random_seed, settings.sample_size)
    bbox = bbox_from_rides(rides, settings.bbox_pad)

    loader = LoadTrafficNetwork(CACHE_DIR)
    graph = loader.load_bike_network_graph_from_bbox(bbox)

    rides_list: List[tuple[tuple[float, float], tuple[float, float]]] = [
        ((row["start_lat"], row["start_lng"]), (row["end_lat"], row["end_lng"]))
        for row in rides.iter_rows(named=True)
    ]
    builder = GraphBuilder(graph)
    routes, lengths, methods = builder.build_routes(rides_list, n_jobs=settings.n_jobs)

    payload = {
        "routes": routes,
        "lengths": lengths,
        "methods": methods,
        "rides": rides.to_dict(as_series=False),
        "bbox": bbox.__dict__,
        "settings": settings.dict(),
    }
    settings.output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(settings.output_path, "wb") as f:
        pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"Saved precomputed routes to {settings.output_path}")


if __name__ == "__main__":
    main()
