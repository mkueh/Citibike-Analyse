# Complex Route Crash Analyse

Zweck: Citi-Bike-Fahrten mit NYC-Crash-Daten zusammenbringen, Routen vorrechnen und Crash-Hotspots visualisieren.

## Kernskripte
- `route_precompute_routes.py`  
  - Ziehe eine Stichprobe von Fahrten (Einstellungen in `route_precompute_settings.json`), baue ein OSM-Bike-Netz (inkl. Fähren-Kanten), berechne kürzeste Wege und speichere alles als Pickle (`cache/precomputed_routes_*.pkl`).
- `plot_precomputed_routes.py`  
  - Lädt das Pickle und erzeugt eine Folium-Karte aller vorcomputierten Routen.
- `map_routes_crashes.py`  
  - Lädt vorcomputierte Routen und clustert NYPD-Cyclist-Crashes, errechnet Überschneidungen Routen↔Crash-Bereucge und rendert eine Folium-Karte (`outputs/ride_crash_map.html`).
- `route_precompute_settings.json`  
  - Parameter für die Stichprobe (sample_size, bbox_pad), Parallelität (`n_jobs`), Zufalls-Seed und Ausgabepfad.

## Utilities
- `utility/load_ridedata.py` – Liest Citi-Bike-CSV (2023–2025), filtert fehlende Koordinaten, optional Sampling für das precomputing.
- `utility/load_crashdata.py` – Lädt NYPD-Crash-CSV, filtert auf Fahrrad-Beteiligung und Zeitraum/BBox, clustert Punkte im metrischen CRS; liefert CrashCluster mit Buffer/Geometrie.
- `utility/load_traffic_network.py` – Baut/Cached OSM-Grafen (bike), fügt Fähren hinzu, berreinigt/vereinfacht Topologie (Konsolidierung, Edge-Filter, Largest Component).
- `utility/load_precomputed_routes.py` – Helfer zum Laden von Pickle der vorcomputierten Routen.
- `utility/generator_enriched_cluster.py` – Errechnet die Anzahl Routen, die Crash-Buffer schneiden, und erzeugt EnrichedCrashCluster.
- `utility/calc_length.py` – Haversine-Längenberechnung für Routenkoordinaten, da diese in LAT/LON EPSG:4326 vorliegen.
- `utility/logic_traffic_network/*` – Caching, Zuschneiden und Edge-Processing für die OSM-Grafen (intern genutzt von `load_traffic_network.py`).

## Objekte
- `objects/bbox.py`, `objects/crash_cluster.py`, `objects/enriched_crash_cluster.py` – Kleine Datenhalter für BBox, Crash-Cluster, angereicherte Cluster (inkl. Intersections count).

## Ablauf (Kurzform)
1) `route_precompute_routes.py` ausführen, um Routen vorzuberechnen.  
2) Optional: `plot_precomputed_routes.py` für einen schnellen Routen-Check.  
3) `map_routes_crashes.py` ausführen, um Crash-Cluster und Routen zu visualisieren.

Zu finden sind die Ergebnisse unter `outputs`