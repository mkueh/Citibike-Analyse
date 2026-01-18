from pathlib import Path
import polars as pl


class LoadRideData:
    """Loader for Citi Bike CSV data (2023–2025)."""

    raw_citi_path: Path
    
    CITI_SCHEMA = {
    "ride_id": pl.Utf8,
    "started_at": pl.Datetime,
    "ended_at": pl.Datetime,
    "start_lat": pl.Float64,
    "start_lng": pl.Float64,
    "end_lat": pl.Float64,
    "end_lng": pl.Float64,
    }
    
    def _get_files(self) -> list[Path]:
        """Collect all year folders with Citi Bike CSVs."""
        return (
            sorted((self.raw_citi_path / "2023-citibike-tripdata").glob("*.csv"))
            + sorted((self.raw_citi_path / "2024-citibike-tripdata").glob("*.csv"))
            + sorted((self.raw_citi_path / "2025-citibike-tripdata").glob("*.csv"))
        )
    
    def __init__(self, raw_citi_path: Path) -> None:
        """Init loader with base raw_data path."""
        self.raw_citi_path = raw_citi_path
    
    def load_rides(self, max_rides: int) -> pl.DataFrame:
        """Load rides with required coords; optional hard limit.

        Args:
            max_rides: Limit rows (<=0 means no limit).

        Returns:
            DataFrame with ride_id/time/coords columns.
        """
        files = self._get_files()
        if not files:
            raise FileNotFoundError("No Citi Bike CSVs found in raw_data/citi_bike/*-citibike-tripdata")
        scan = (
            pl.scan_csv(files, schema_overrides=self.CITI_SCHEMA, try_parse_dates=True, ignore_errors=True)
            .filter(
                pl.col("start_lat").is_not_null()
                & pl.col("start_lng").is_not_null()
                & pl.col("end_lat").is_not_null()
                & pl.col("end_lng").is_not_null()
            )
            .select(list(self.CITI_SCHEMA.keys()))
        )
        if max_rides and max_rides > 0:
            scan = scan.limit(max_rides)
        rides = scan.collect()
        print(f"Loaded rides: {len(rides):,}")
        return rides
    
    def sample_rides(self, seed: int, sample_size: int) -> pl.DataFrame:
        """Deterministically sample rides via hash-based ordering.

        Args:
            seed: Hash seed for reproducible order.
            sample_size: Number of rides to return.

        Returns:
            Sampled rides DataFrame.
        """
        files = self._get_files()

        scan = (
            pl.scan_csv(files, schema_overrides=self.CITI_SCHEMA, try_parse_dates=True, ignore_errors=True)
            .filter(
                pl.col("start_lat").is_not_null()
                & pl.col("start_lng").is_not_null()
                & pl.col("end_lat").is_not_null()
                & pl.col("end_lng").is_not_null()
            )
            .with_columns(pl.col("ride_id").hash(seed=seed).alias("rand"))
            .sort("rand")
            .limit(sample_size)
            .select(list(self.CITI_SCHEMA.keys()))
        )
        rides = scan.collect()
        print(f"Using rides sample: {len(rides):,}")
        return rides
