from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import List, Annotated, Optional
from datetime import date
import models 
from database import SessionLocal, engine
from sqlalchemy.orm import Session



app = FastAPI()
models.Base.metadata.create_all(bind=engine)

# Pydantic schema para la respuesta
class StockItem(BaseModel):
    date: date
    open: float
    high: float
    low: float
    close: float
    adj_close: float
    volume: int

    class Config:
        orm_mode = True  # Para convertir el modelo SQLAlchemy a Pydantic

class Item(BaseModel):
    name: str
    description: Optional[str] = None
    price: float
    tax: Optional[float] = None

    class Config:
        orm_mode = True

# Función para obtener la sesión de la base de datos
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


db_dependency = Annotated[Session, Depends(get_db)]
# Endpoint GET con filtros y paginación
@app.get("/stocks/")
async def get_stocks(
    db: db_dependency,
    skip: int = 0,
    limit: int = 100,
    date_from: Optional[date] = Query(None, description="Filtrar desde esta fecha"),
    date_to: Optional[date] = Query(None, description="Filtrar hasta esta fecha")
):
    # Iniciar la consulta
    query = db.query(models.StockData)

    # Filtrar por fechas si se proporcionan
    if date_from:
        query = query.filter(models.StockData.date >= date_from)
    if date_to:
        query = query.filter(models.StockData.date <= date_to)

    # Paginación
    stocks = query.offset(skip).limit(limit).all()

    return stocks

@app.get("/items/{item_id}")
def read_item(item_id: int, query: str = None):
    return {"item_id": item_id, "query": query}

@app.post("/items/")
def create_item(item: Item):
    return {"name": item.name, "description": item.description, "price": item.price}
