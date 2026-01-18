# NYPD Crash-Analysen

Kurzübersicht der Notebooks:
- `00_data_overview.ipynb`: Lädt Crash-CSV aus `raw_data/nypd`, prüft Umfang/Zeitraum/Borough-Abdeckung, bereinigt Datum/Zeit und prüft Koordinatenverfügbarkeit.
- `01_time_and_severity_patterns.ipynb`: Zeitliche Muster (Stunde/Wochentag/Monat) mit Fokus auf Fahrrad-relevante Fälle, Verletzungs-/Todesfälle im Zeitverlauf.
- `02_locations_and_hotspots.ipynb`: Räumliche Verteilung nach Boroughs, Top-Straßen/Intersection-Hotspots, Cluster-/Heatmap-Basis aus Koordinaten.
- `03_vehicles_and_factors.ipynb`: Häufigkeit von Fahrzeugtypen, Analyse der `contributing_factor_*`-Felder, Schweregrade (injuries/fatalities) je Faktor.
