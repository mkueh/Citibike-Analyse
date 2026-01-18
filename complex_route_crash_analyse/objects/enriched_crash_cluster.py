from objects.crash_cluster import CrashCluster


class EnrichedCrashCluster(CrashCluster):
    """Crash cluster enriched with ride intersections.

    Explanation:
    Extends CrashCluster by counting intersecting routes and computing crash-per-ride ratios.
    """
    
    rides_intersection_count: int
    crash_per_rides: float
    
    def __init__(self, crash_cluster: CrashCluster, rides_intersection_count: int):
        """Copy base cluster and attach ride intersection metadata.

        Args:
            crash_cluster: Base cluster to enrich.
            rides_intersection_count: Number of routes intersecting this buffer.
        """
        super().__init__(
            centroid=crash_cluster.centroid,
            buffer=crash_cluster.buffer,
            count=crash_cluster.count,
            max_dist=crash_cluster.max_dist,
        )
        self.rides_intersection_count = rides_intersection_count
        self._calculate_crash_per_rides(rides_intersection_count)
        
    def _calculate_crash_per_rides(self, total_rides: int):
        """Compute crashes per ride for this cluster.

        Args:
            total_rides: Number of intersecting routes.

        Returns:
            None; sets crash_per_rides attribute.
        """
        if total_rides > 0:
            self.crash_per_rides = (self.count / total_rides)
        else:
            self.crash_per_rides = 0.0
