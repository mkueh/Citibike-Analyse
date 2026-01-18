from shapely import Point, Polygon


class CrashCluster:
    """Geometric crash cluster with centroid, buffer polygon, and stats.

    Explanation:
    Encapsulates a cluster of crash points, storing centroid, buffered area, number of crashes, and max point distance.
    """

    centroid: Point
    buffer: Polygon
    count: int
    max_dist: float
    
    def __init__(self, centroid: Point, buffer: Polygon, count: int, max_dist: float):
        """Create a crash cluster container.

        Args:
            centroid: Cluster centroid (lon/lat).
            buffer: Polygon buffer around the centroid in WGS84.
            count: Number of crashes in the cluster.
            max_dist: Max distance (meters) from centroid to any member point.
        """
        self.centroid = centroid
        self.buffer = buffer
        self.count = count
        self.max_dist = max_dist
