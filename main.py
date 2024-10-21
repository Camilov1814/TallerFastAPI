from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from psycopg2 import IntegrityError
from pydantic import BaseModel
from typing import List, Annotated, Optional
from datetime import date
import models 
from database import SessionLocal, engine
from sqlalchemy.orm import Session
import boto3
import json
from botocore.exceptions import NoCredentialsError
import os
from datetime import datetime

# from sqlalchemy.exc import IntegrityError
 


app = FastAPI()
models.Base.metadata.create_all(bind=engine) # Crear las tablas en la base de datos

# Pydantic schema para la respuesta
class StockData(BaseModel):
    close: float
    low: float
    open: float
    date: date  
    high: float
    adj_close: float
    volume: int

    class Config:
        # orm_mode = True  # Para convertir el modelo SQLAlchemy a Pydantic
        from_attributes = True

# Función para obtener la sesión de la base de datos
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


db_dependency = Annotated[Session, Depends(get_db)]



# Endpoint GET con filtros y paginación
#############################################################################################
@app.get("/stocks/")
async def get_stocks(
    db: db_dependency,
    page: int = 1,
    limit: int = 100,
    date_from: Optional[date] = Query(None, description="Filtrar desde esta fecha"),
    date_to: Optional[date] = Query(None, description="Filtrar hasta esta fecha")
):
    
    # Validar los parámetros de paginación 400 - Bad Request
    if page < 1:
        raise HTTPException(status_code=400, detail="El número de página debe ser mayor o igual a 1")
    if limit < 1:
        raise HTTPException(status_code=400, detail="El límite debe ser mayor o igual a 1")

    # Iniciar la consulta
    query = db.query(models.StockData)

    # Filtrar por fechas si se proporcionan
    if date_from:
        query = query.filter(models.StockData.date >= date_from)
    if date_to:
        query = query.filter(models.StockData.date <= date_to)

    total_records = query.count()
    max_pages = (total_records + limit - 1) // limit  # Redondear hacia arriba
    # Verificar si la página solicitada está fuera de rango
    if page > max_pages:
        raise HTTPException(
            status_code=400, 
            detail=f"La página solicitada {page} está fuera del rango. Solo hay {max_pages} páginas disponibles."
        )

    page = (page - 1) * limit

    # Paginación
    stocks = query.offset(page).limit(limit).all()

    if not stocks:
        raise HTTPException(status_code=404, detail="No se encontraron datos para los filtros proporcionados")

    return stocks


def date_converter(o):
    if isinstance(o, date):
        return o.isoformat()
    raise TypeError(f"Type {o} not serializable")

## Método POST para agregar datos a la base de datos y subir a S3
#############################################################################################
s3 = boto3.client('s3', aws_access_key_id=os.getenv("ACCESS_KEY"), aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"), region_name=os.getenv("REGION"))

BUCKET_NAME = os.getenv("OUR_BUCKET_NAME")

@app.post("/stocks/")
async def create_stock_data(stock_data: list[StockData]):
    db = SessionLocal()
    created_count = 0
    stock_entries = []  # Lista para almacenar los registros que se agregarán al bucket

    for stock in stock_data:
        # Verificar si ya existe una acción con la misma fecha
        existing_stock = db.query(models.StockData).filter(models.StockData.date == stock.date).first()
        if existing_stock:
            raise HTTPException(
                status_code=409,
                detail=f"La acción con la fecha {stock.date} ya existe en la base de datos. Se agregaron {created_count} elementos a la base de datos.",
            )

        # Crear un objeto de la clase StockData
        stock_entry = models.StockData(
            close=stock.close,
            low=stock.low,
            open=stock.open,
            date=stock.date,
            high=stock.high,
            adj_close=stock.adj_close,
            volume=stock.volume
        )
        try:
            db.add(stock_entry)
            db.commit()
            created_count += 1
            # Convertir date a cadena ISO antes de subir a S3
            stock_dict = stock.model_dump()
            stock_dict['date'] = stock_dict['date'].isoformat()  # Convertir la fecha a string
            stock_entries.append(stock_dict)

            # Agregar a la lista para subir a S3
            stock_entries.append(stock.model_dump())  # Usamos dict() para convertir a JSON-friendly

        except IntegrityError:
            db.rollback()  # Deshacer cambios en caso de error
            continue

    # Subir el archivo JSON a S3
    if stock_entries:
        file_name = f"stocks_{stock.date}.json"
        try:
            response = s3.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body = json.dumps(stock_entries, default=date_converter),
                ContentType='application/json'
            )        
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f"El archivo {file_name} se subió correctamente a {BUCKET_NAME}.")
            else:
                raise HTTPException(status_code=500, detail="No se pudo subir el archivo a S3.")
        except NoCredentialsError:
            raise HTTPException(status_code=500, detail="Error de credenciales de AWS.")        

    total_count = db.query(models.StockData).count()  # Total de registros en la base de datos
    db.close()

    return JSONResponse(content={
        "created_count": created_count,
        "total_count": total_count
    })