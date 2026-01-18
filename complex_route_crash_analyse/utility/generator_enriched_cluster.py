from typing import Counter, Dict, List, Tuple

from shapely import LineString, Polygon, STRtree

from objects.crash_cluster import CrashCluster
from objects.enriched_crash_cluster import EnrichedCrashCluster


class GeneratorEnrichedCluster:
    """Enrich crash clusters with route intersection counts."""

    intersection_map: Dict[int, int]

    def __init__(self, buffers: List[Polygon], routes: List[List[Tuple[float, float]]]):
        """Precompute buffer↔route intersections for later enrichment.

        Args:
            buffers: Crash cluster buffers (polygons, lon/lat).
            routes: Routes as sequences of (lon, lat) coordinates.
        """
        self.intersection_map = self._count_cluster_intersections(buffers, routes)

    def generate_enriched_clusters(
        self,
        crash_clusters: List[CrashCluster],
    ) -> List[EnrichedCrashCluster]:
        """Attach intersection counts to CrashCluster instances.

        Args:
            crash_clusters: Base crash clusters to enrich.

        Returns:
            List of EnrichedCrashCluster with rides_intersection_count/crash_per_rides.
        """
        enriched_clusters = []
        for idx, cluster in enumerate(crash_clusters):
            rides_intersection_count = self.intersection_map.get(idx, 0)
            enriched_cluster = EnrichedCrashCluster(
                crash_cluster=cluster, rides_intersection_count=rides_intersection_count
            )
            enriched_clusters.append(enriched_cluster)
        return enriched_clusters

    def _count_cluster_intersections(
        self, buffers: List[Polygon], routes: List[List[Tuple[float, float]]]
    ) -> Dict[int, int]:
        """Count how many routes intersect each buffer polygon.

        Args:
            buffers: Crash cluster buffers (polygons, lon/lat).
            routes: List of routes as (lon, lat) sequences.

        Returns:
            Mapping buffer_index -> intersection count (only buffers hit at least once are present).
        """
        line_strings = [LineString(r) for r in routes if len(r) >= 2]

        if not line_strings:
            return {}

        tree = STRtree(buffers)
        indices = tree.query(line_strings)

        intersection_counts = Counter()
        for route_idx, buffer_idx in zip(indices[0], indices[1]):
            route_geom = line_strings[route_idx]
            buffer_geom = buffers[buffer_idx]

            if buffer_geom.intersects(route_geom):
                intersection_counts[int(buffer_idx)] += 1

        return dict(intersection_counts)
