# Citi Bike Analysen

Kurzübersicht der Notebooks:
- `00_data_overview.ipynb`: Lädt alle Citi-Bike-CSV (2023–2025) aus `raw_data/citi_bike`, prüft Zeitraum/Zeilenanzahl, fehlende Koordinaten/Zeitspalten und Basisverteilungen.
- `01_time_and_usage_patterns.ipynb`: Zeitliche Nutzungsmuster (Stunde/Wochentag/Monat), Vergleich Member vs. Casual und Bike-Typen, Dauerverteilungen.
- `02_long_distance_bikes.ipynb`: Berechnet Distanzen, filtert lange Fahrten, zeigt Top-Langstrecken und Muster nach Zeit/Bike-Typ, Plausibilitätschecks für Geokoordinaten.
- `03_station_and_route_hotspots.ipynb`: Aggregiert meistgenutzte Start-/End-Stationen und häufige Routenpaare, Ranking-Basis für Hotspot-Analysen.

Wenig Start=End, Räder werden also als Nahverkehrsmittel verwendet