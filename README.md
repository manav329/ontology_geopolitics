# Ontology Geopolitics

A **multi-domain geopolitical risk analysis system** that builds a Neo4j knowledge graph from real-world data sources and computes derived intelligence metrics across defense, economy, climate, and geopolitics domains.

## What It Does

The system ingests raw data from multiple sources, normalizes it, and inserts it into a typed relational knowledge graph with 40+ relationship types. It then computes cross-domain risk scores and strategic influence metrics that are served through a REST API.

Key capabilities:
- **Political alignment** – UN General Assembly voting similarity between countries
- **Diplomatic centrality** – network analysis of inter-state interactions
- **Trade dependency** – bilateral import/export dependency ratios
- **Military capability** – composite defence indices
- **Climate vulnerability** – emissions trends and climate stress scores
- **Global Risk / Strategic Influence / Vulnerability Index** – aggregated cross-domain composite metrics

## Project Structure

```
ontology_geopolitics/
├── analytics/          # Compute derived scores for each domain
│   ├── climate/
│   ├── composite/      # Cross-domain: global risk, influence, vulnerability
│   ├── defenses/
│   ├── economy/
│   └── geopolitics/
├── api/
│   └── main.py         # FastAPI application (dynamically loads module routers)
├── common/             # Shared utilities and core ontology
│   ├── config.py       # Centralised thresholds and weights
│   ├── db.py           # Neo4j connection management
│   ├── entity_mapper.py
│   ├── graph_ops.py    # Schema-enforced graph writes
│   ├── ontology.py     # 40+ typed relationship definitions
│   └── intelligence/   # Reusable calculation library
│       ├── aggregation.py
│       ├── composite.py
│       ├── dependency.py
│       ├── growth.py
│       ├── normalization.py
│       └── similarity.py
├── data/
│   ├── raw/            # External source files (V-Dem, GDELT, UNGA, etc.)
│   └── processed/      # Cached Parquet files
├── modules/            # Domain-specific ETL pipelines
│   ├── climate/
│   ├── defense/
│   ├── economy/
│   └── geopolitics/    # Fully implemented (V-Dem, GDELT, UNGA)
├── scripts/
│   ├── run_all.py      # Main orchestration entry point
│   ├── check_db.py
│   └── test_connection.py
└── requirements.txt
```

## Data Sources

| Source | Domain | Description |
|--------|--------|-------------|
| [V-Dem](https://www.v-dem.net/) | Geopolitics | Political system classifications (democracy/autocracy scores) |
| [GDELT](https://www.gdeltproject.org/) | Geopolitics | Diplomatic events and inter-state interactions |
| [UNGA Voting](https://dataverse.harvard.edu/dataset.xhtml?persistentId=hdl:1902.1/12379) | Geopolitics | UN General Assembly voting records |
| Defence expenditure data | Defence | Military spending and capability indicators |
| Trade flow data | Economy | Bilateral import/export volumes |
| Climate data | Climate | Emissions, vulnerability, and stress indicators |

## Dependencies

All Python dependencies are listed in `requirements.txt`:

| Package | Purpose |
|---------|---------|
| `neo4j` | Neo4j Python driver |
| `python-dotenv` | Load credentials from `.env` file |
| `pycountry` | Country/region metadata and name normalization |
| `networkx` | Graph analysis (centrality, community detection) |
| `scipy` | Scientific computing |
| `python-louvain` | Louvain community detection algorithm |
| `pyarrow` | Parquet file support for processed data cache |
| `fastapi` | REST API framework |
| `uvicorn` | ASGI server for FastAPI |
| `pandas` | Data manipulation and transformation |

Install all dependencies:

```bash
pip install -r requirements.txt
```

## Setup

### Prerequisites

- Python 3.9+
- A running [Neo4j](https://neo4j.com/download/) instance (local or AuraDB)

### Configuration

Create a `.env` file in the project root:

```env
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
```

### Test the database connection

```bash
python scripts/test_connection.py
```

## Running the Pipeline

Run the full ETL pipeline to load, clean, and insert data into Neo4j:

```bash
python scripts/run_all.py
```

This orchestrates all modules in order: load raw data → clean → compute similarity → insert into the graph → run analytics.

Check the database state after ingestion:

```bash
python scripts/check_db.py
```

## Running the API

Start the FastAPI server:

```bash
uvicorn api.main:app --reload
```

The API will be available at `http://localhost:8000`.  
Interactive documentation (Swagger UI) is at `http://localhost:8000/docs`.

## Architecture

The system follows an **ontology-driven ETL + scoring** architecture:

```
Raw Data Sources
      │
      ▼
  modules/       ← Domain ETL pipelines (load → clean → insert)
      │
      ▼
 Neo4j Graph     ← Typed knowledge graph (40+ relationship types)
      │
      ▼
 analytics/      ← Derived scores per domain + composite metrics
      │
      ▼
   api/          ← FastAPI REST endpoints
```

All graph edges follow a consistent schema with `value`, `normalized_weight`, `year`, and `confidence` properties, enforced by `common/graph_ops.py`.

## Current Status

| Module | Status |
|--------|--------|
| Geopolitics (V-Dem, GDELT, UNGA) | ✅ Fully implemented |
| Defence | ⚠️ Pipeline structure in place, data not yet wired |
| Economy | ⚠️ Pipeline structure in place, data not yet wired |
| Climate | ⚠️ Pipeline structure in place, data not yet wired |
| Composite analytics | ✅ Framework ready (global risk, influence, vulnerability) |
| REST API | ✅ FastAPI app with geopolitics routes |
