from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import pandas as pd
import numpy as np
import uvicorn

model = joblib.load('./data/inflation_model.pkl')
scaler = joblib.load('./data/scaler.pkl')

app = FastAPI(
    title="Inflation Forecast API",
    description="Прогнозирование инфляции на 6 месяцев вперед",
    version="1.0.0"
)

class InflationData(BaseModel):
    CPIAUCSL: float
    UNRATE: float
    FEDFUNDS: float
    M2SL: float
    SP500: float
    DGS10: float
    GDP: float
    INDPRO: float

    class Config:
        json_schema_extra = {
            "example": {
                "CPIAUCSL": 287.5,
                "UNRATE": 4.1,
                "FEDFUNDS": 5.25,
                "M2SL": 15466.6,
                "SP500": 4800,
                "DGS10": 4.28,
                "GDP": 21751.238,
                "INDPRO": 101.03
            }
        }

@app.get("/")
def root():
    return {
        "message": "Inflation Forecast API",
        "docs": "/docs",
        "health": "/health"
    }

@app.get("/health")
def health():
    return {
        "status": "✅ OK",
        "model_loaded": model is not None,
        "scaler_loaded": scaler is not None
    }

@app.post("/predict")
def predict(data: InflationData):
    input_df = pd.DataFrame([data.dict()])
    
    input_scaled = scaler.transform(input_df)
    
    prediction = model.predict(input_scaled)[0]
    
    return {
        "forecast_6months": round(prediction, 2),
        "current_cpi": data.CPIAUCSL,
        "expected_change": round(prediction - data.CPIAUCSL, 2),
        "expected_change_percent": round((prediction/data.CPIAUCSL - 1)*100, 2)
    }

@app.post("/predict_simple")
def predict_simple(cpi: float, unrate: float):
    dummy_data = {
        'CPIAUCSL': cpi,
        'UNRATE': unrate,
        'FEDFUNDS': 5.0,
        'M2SL': 15000,
        'SP500': 4500,
        'DGS10': 4.0,
        'GDP': 20000,
        'INDPRO': 100
    }
    
    input_df = pd.DataFrame([dummy_data])
    input_scaled = scaler.transform(input_df)
    prediction = model.predict(input_scaled)[0]
    
    return {"forecast_6months": round(prediction, 2)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)