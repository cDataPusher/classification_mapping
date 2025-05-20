from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.requests import Request
import duckdb
import pandas as pd
import os
from pydantic import BaseModel
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "terminology.duckdb")

con = duckdb.connect(DB_PATH)
con.execute(
    """
    CREATE TABLE IF NOT EXISTS codes(
        terminology TEXT,
        year INTEGER,
        code TEXT,
        description TEXT
    );
    """
)

con.execute(
    """
    CREATE TABLE IF NOT EXISTS mappings(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_terminology TEXT,
        source_year INTEGER,
        source_code TEXT,
        target_terminology TEXT,
        target_year INTEGER,
        target_code TEXT,
        responsible TEXT,
        timestamp TIMESTAMP
    );
    """
)

app = FastAPI(title="Classification Mapping")

app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

class MappingIn(BaseModel):
    source_terminology: str
    source_year: int
    source_code: str
    target_terminology: str
    target_year: int
    target_code: str
    responsible: str = "Dummy"

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload")
async def upload(terminology: str = Form(...), year: int = Form(...), file: UploadFile = File(...)):
    df = pd.read_csv(file.file, sep=";", names=["code", "description"])  # expects code;description
    df["terminology"] = terminology
    df["year"] = year
    con.execute("INSERT INTO codes SELECT * FROM df")
    return {"rows": len(df)}

@app.post("/mapping")
async def create_mapping(mapping: MappingIn):
    con.execute(
        "INSERT INTO mappings VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            mapping.source_terminology,
            mapping.source_year,
            mapping.source_code,
            mapping.target_terminology,
            mapping.target_year,
            mapping.target_code,
            mapping.responsible,
            datetime.utcnow(),
        ],
    )
    return {"status": "ok"}

@app.get("/codes")
async def get_code(terminology: str, year: int, code: str):
    result = con.execute(
        "SELECT description FROM codes WHERE terminology=? AND year=? AND code=?",
        [terminology, year, code],
    ).fetchone()
    if result:
        return {"code": code, "description": result[0]}
    return JSONResponse(status_code=404, content={"detail": "Code not found"})

@app.get("/mapping")
async def get_mapping(source_terminology: str, source_year: int, source_code: str, target_year: int):
    result = con.execute(
        """
        SELECT target_code FROM mappings
        WHERE source_terminology=? AND source_year=? AND source_code=? AND target_year=?
        """,
        [source_terminology, source_year, source_code, target_year],
    ).fetchone()
    if result:
        return {"target_code": result[0]}
    return JSONResponse(status_code=404, content={"detail": "Mapping not found"})
