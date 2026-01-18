## Datenquellen:
 - Crashes: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data
 - Fahrradleihdaten: https://s3.amazonaws.com/tripdata/index.html
 - Straßendaten: osmnx / openstreetmap

## Initialisierung
- Python 3.12+ bereitstellen.
- Abhängigkeiten installieren: `uv sync` (legt `.venv` im Projekt an).
- Rohdaten laden:
  - Citi Bike: `python3 raw_data/citi_bike/load_data.py` (lädt 2023–2025 ZIPs und entpackt sie in die Jahresordner).
  - NYPD Crashes: Anleitung in `raw_data/nypd/README.md` befolgen (CSV von der NYC-Open-Data-Seite laden und umbenennen).

## Ordnerstruktur (Kurzüberblick)
Tiefere Informationen sind in den Unterordner zu finden.

- `raw_data/`
  - `citi_bike/` heruntergeladene Citi-Bike-ZIPs + entpackte CSVs (jährliche Unterordner).
  - `nypd/` Crash-CSV gemäß Anleitung.
- `data_analyse/` Analyseskripte/-notebooks.
- `complex_route_crash_analyse` Komplexere Analyse mit errechneten Routen und Crash Clustern .
- `outputs/` Ergebnisse/Artefakte aus Analysen.
- `cache/` zwischengespeicherte Daten (z. B. Graphen, vorcomputierte Routen).

## Research

- Regen und Unfallstatistik: https://www.researchgate.net/publication/339422560_Relationship_Between_Traffic_Volume_and_Accident_Frequency_at_Intersections
 
  - Bei leerer Straße (Congestion Level 1): Das Unfallrisiko ist bei Regen 5-mal höher als bei Trockenheit (RR≈5).
    Grund: Auf leeren Straßen wird schneller gefahren. Kommen Nässe (längerer Bremsweg) und schlechte Sicht hinzu, 
    führt das bei hohen Geschwindigkeiten oft zu Unfällen.

  -  Bei verstopfter Straße (Congestion Level 15): Das Unfallrisiko bei Regen ist fast identisch mit dem bei Trockenheit (RR≈1).
    Grund: Im Stau oder dichten Verkehr ("stop-and-go") fahren alle so langsam, dass der Bremsweg auf nasser Fahrbahn keine Rolle mehr spielt. 
    Die Unfälle passieren hier primär wegen der Dichte (Auffahrunfälle), egal ob es regnet oder nicht.

  - **Wetter also erstmal ignorieren, da dafür auch Verkehrsdichte nötig ist.**

## Resultat

Die Vorberrechnungen wurden auf 500 Routen begrenzt für einen einfachen Test. Bei einer genaueren Analyse der Crash-Cluster (Unfallschwerpunkte) müssen deutlich mehr Routen vorberechnet werden für ein genaues Bild. Zusätzlich kann mittels weitere Daten wie Wetter und Verkehrsdaten ein noch genaueres Bild von Unfallschwerpunkte und Zeit analysiert werden welches dann auch für eine Routenfinden genutzt werden kann die besonders sichere Routen generiert.