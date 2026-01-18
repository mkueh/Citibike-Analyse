from __future__ import annotations

from pathlib import Path
import pickle
import polars as pl


class PrecomputedRoutes:
    """Container for rides metadata and route geometries."""

    def __init__(self, df: pl.DataFrame, routes: list[list[tuple[float, float]]]) -> None:
        """Create wrapper around rides DataFrame and routes.

        Args:
            df: Rides metadata (polars DataFrame).
            routes: List of routes as sequences of (lon, lat) tuples.
        """
        self.df = df
        self.routes = routes

def load_precomputed_routes_pickle(path: Path) -> PrecomputedRoutes:
    """Load precomputed routes from a pickle payload.

    Args:
        path: Pickle file path produced by `route_precompute_routes.py`.

    Returns:
        PrecomputedRoutes with rides DataFrame and route geometries.
    """
    payload = pickle.load(open(path, "rb"))
    df = pl.from_dict(payload.get("rides", {})) if "rides" in payload else pl.DataFrame()
    routes = payload.get("routes", [])
    return PrecomputedRoutes(df, routes)
