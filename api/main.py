from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(
    title="Global Ontology Engine",
    description="Multi-domain intelligence system",
    version="1.0.0"
)

# Each router is optional — only loads if that module exists on this branch.
# When all modules merge into main, all four will load automatically.

try:
    from modules.defense.routes import router as defense_router
    app.include_router(defense_router)
except ImportError:
    pass

try:
    from modules.geopolitics.routes import router as geopolitics_router
    app.include_router(geopolitics_router)
except ImportError:
    pass

try:
    from modules.economy.routes import router as economy_router
    app.include_router(economy_router)
except ImportError:
    pass

try:
    from modules.climate.routes import router as climate_router
    app.include_router(climate_router)
except ImportError:
    pass


@app.get("/")
def root():
    from fastapi.routing import APIRouter
    active = [r.prefix for r in app.routes if hasattr(r, "prefix") and r.prefix]
    return {
        "status": "running",
        "active_modules": active,
        "docs": "/docs"
    }