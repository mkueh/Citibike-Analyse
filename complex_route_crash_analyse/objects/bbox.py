class BBox:
    """Axis-aligned bounding box in WGS84.

    Explanation:
    Stores north/south/east/west limits and provides helpers for comparisons and containment.
    """

    north: float
    south: float
    east: float
    west: float
    
    def __init__(self, north: float, south: float, east: float, west: float) -> None:
        """Create a bounding box.

        Args:
            north: Northern latitude.
            south: Southern latitude.
            east: Eastern longitude.
            west: Western longitude.
        """
        self.north = north
        self.south = south
        self.east = east
        self.west = west  

    def to_tuple(self, order: list[str] = ["west", "south", "east", "north"]) -> tuple:
        """Return coordinates in the requested order (default osmnx format).

        Args:
            order: Sequence of attribute names to emit.

        Returns:
            Tuple with values in the given order.
        """
        return tuple(getattr(self, key) for key in order)
    
    def equals(self, other: "BBox", tol: float = 1e-4) -> bool:
        """Check approximate equality with tolerance.

        Args:
            other: Box to compare.
            tol: Allowed absolute deviation per side.

        Returns:
            True if all sides differ by less than tol.
        """
        return (
            abs(self.north - other.north) < tol
            and abs(self.south - other.south) < tol
            and abs(self.east - other.east) < tol
            and abs(self.west - other.west) < tol
        )

    def contains_bbox(self, other: "BBox", tol: float = 1e-9) -> bool:
        """Check if this box fully contains another box (with tolerance).

        Args:
            other: Box to test.
            tol: Margin of error.

        Returns:
            True if other is inside this box.
        """
        return (
            self.west - tol <= other.west
            and self.east + tol >= other.east
            and self.south - tol <= other.south
            and self.north + tol >= other.north
        )

    def contains_point(self, lon: float, lat: float, tol: float = 0.0) -> bool:
        """Check if lon/lat lies inside this box (with tolerance).

        Args:
            lon: Longitude.
            lat: Latitude.
            tol: Margin of error.

        Returns:
            True if point is inside or on the boundary.
        """
        return (
            self.west - tol <= lon <= self.east + tol
            and self.south - tol <= lat <= self.north + tol
        )

    def area(self) -> float:
        """Return area of the box in degree² (approx, not projected)."""
        return abs((self.north - self.south) * (self.east - self.west))
        
    def __str__(self) -> str:
        return f"BBox(north={self.north}, south={self.south}, east={self.east}, west={self.west})"
