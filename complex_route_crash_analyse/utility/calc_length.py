from math import radians, sin, cos, asin, sqrt
from typing import Sequence


class LengthCalculator:
    """Compute cumulative length for coordinate sequences."""
    
    @staticmethod
    def calc_lengths(coords: Sequence[tuple[float, float]]) -> float:
        """Sum haversine distances along a polyline.

        Args:
            coords: Sequence of (lon, lat) tuples in WGS84.

        Returns:
            Total length in meters.
        """
        total_length = 0.0
        for i in range(1, len(coords)):
            lon1, lat1 = coords[i - 1]
            lon2, lat2 = coords[i]
            segment_length = LengthCalculator._haversine_m(lat1, lon1, lat2, lon2)
            total_length += segment_length
        return total_length

    @staticmethod
    def _haversine_m(lat1, lon1, lat2, lon2):
        """Great-circle distance between two lat/lon points in meters."""
        r = 6371000
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
        return 2 * r * asin(sqrt(a))
