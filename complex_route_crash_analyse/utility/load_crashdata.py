from pathlib import Path
from typing import List
from pyproj import Transformer
from shapely.geometry import Point, Polygon
import polars as pl

from objects.crash_cluster import CrashCluster


class LoadCrashData:
    """Load and cluster cyclist-involved crash points."""
    
    CRASH_SCHEMA = {
        "CRASH DATE": pl.Utf8,
        "CRASH TIME": pl.Utf8,
        "LATITUDE": pl.Utf8,
        "LONGITUDE": pl.Utf8,
        "NUMBER OF CYCLIST INJURED": pl.Int64,
        "NUMBER OF CYCLIST KILLED": pl.Int64,
    }
    
    def _init_transformers(self):
        """Create forward/inverse transformers between WGS84 and Web Mercator."""
        fwd = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        inv = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)
        return fwd, inv

    def __init__(self, nypd_raw_path: Path, cluster_buffer_m: float = 50.0, cluster_max_size: int = 10, cluster_max_dist_m: float = 50.0):
        """Configure crash loader and clustering parameters.

        Args:
            nypd_raw_path: Folder containing `Motor_Vehicle_Collisions_Crashes.csv`.
            cluster_buffer_m: Radius for buffer polygons (meters).
            cluster_max_size: Max points per cluster (greedy grouping).
            cluster_max_dist_m: Max distance between seed and point to join cluster (meters).
        """
        self.nypd_raw_path = nypd_raw_path
        self.forward_transformer, self.inverse_transformer = self._init_transformers()
        self.cluster_buffer_m = cluster_buffer_m
        self.cluster_max_size = cluster_max_size
        self.cluster_max_dist_m = cluster_max_dist_m
    
    def load_crash_cluster(self, min_datetime, max_datetime, bbox=None) -> List[CrashCluster]:
        """Load cyclist crashes in time/BBox window and cluster them.

        Args:
            min_datetime: Lower bound (inclusive) for crash_datetime.
            max_datetime: Upper bound (inclusive) for crash_datetime.
            bbox: Optional BBox to spatially filter crashes.

        Returns:
            List of CrashCluster with centroid, buffer, count, and max_dist.
        """
        crash_path = self.nypd_raw_path / "Motor_Vehicle_Collisions_Crashes.csv"
        if not crash_path.exists():
            raise FileNotFoundError(f"Crash data missing: {crash_path}")

        scan = pl.scan_csv(crash_path, schema_overrides=self.CRASH_SCHEMA, null_values=[""])
        scan = scan.with_columns(
            [
                pl.col("LATITUDE").str.replace(",", ".").cast(pl.Float64, strict=False).alias("latitude"),
                pl.col("LONGITUDE").str.replace(",", ".").cast(pl.Float64, strict=False).alias("longitude"),
                pl.concat_str([pl.col("CRASH DATE"), pl.col("CRASH TIME")], separator=" ")
                .str.strptime(pl.Datetime, "%m/%d/%Y %H:%M", strict=False)
                .alias("crash_datetime"),
            ]
        )
        filter_expr = (
            pl.col("crash_datetime").is_not_null()
            & (pl.col("crash_datetime") >= min_datetime)
            & (pl.col("crash_datetime") <= max_datetime)
            & (
                (pl.col("NUMBER OF CYCLIST INJURED") > 0)
                | (pl.col("NUMBER OF CYCLIST KILLED") > 0)
            )
            & pl.col("latitude").is_not_null()
            & pl.col("longitude").is_not_null()
        )
        if bbox is not None:
            filter_expr = (
                filter_expr
                & (pl.col("latitude") >= bbox.south)
                & (pl.col("latitude") <= bbox.north)
                & (pl.col("longitude") >= bbox.west)
                & (pl.col("longitude") <= bbox.east)
            )
        crashes = scan.filter(filter_expr).select(["latitude", "longitude"]).collect()
        points = [Point(*self.forward_transformer.transform(lng, lat)) for lat, lng in zip(crashes["latitude"], crashes["longitude"])]
        clusters = self._cluster_points(points, max_dist_m=self.cluster_max_dist_m, max_size=self.cluster_max_size, buffer_m=self.cluster_buffer_m)

        print(f"Crash buffers: {len(clusters):,} (r={self.cluster_buffer_m} m) from {len(points):,} crashes")
        return clusters

    def _cluster_points(self, points: list[Point], max_dist_m: float, max_size: int, buffer_m: float) -> list[CrashCluster]:
        """Greedy spatial clustering in projected meters, returns CrashCluster list."""
        if not points:
            return []
        unassigned = set(range(len(points)))
        clusters: list[CrashCluster] = []
        while unassigned:
            seed_idx = unassigned.pop()
            seed = points[seed_idx]
            cluster_idxs = [seed_idx]
            for idx in list(unassigned):
                if len(cluster_idxs) >= max_size:
                    break
                if seed.distance(points[idx]) <= max_dist_m:
                    cluster_idxs.append(idx)
                    unassigned.discard(idx)
            pts = [points[i] for i in cluster_idxs]
            cx = sum(p.x for p in pts) / len(pts)
            cy = sum(p.y for p in pts) / len(pts)
            centroid_3857 = Point(cx, cy)
            max_dist = max(centroid_3857.distance(p) for p in pts) if pts else 0.0
            buffer_3857 = centroid_3857.buffer(buffer_m)
            
            centroid_lon, centroid_lat = self.inverse_transformer.transform(centroid_3857.x, centroid_3857.y)
            centroid = Point(centroid_lon, centroid_lat)
            
            buffer_coords_4326 = [self.inverse_transformer.transform(x, y) for x, y in buffer_3857.exterior.coords]
            buffer = Polygon(buffer_coords_4326)
            
            clusters.append(CrashCluster(centroid, buffer, len(pts), max_dist))
        return clusters
