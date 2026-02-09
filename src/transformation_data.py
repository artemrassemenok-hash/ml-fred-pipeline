import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.preprocessing import StandardScaler
import joblib
import matplotlib.pyplot as plt

def clean_data(path):
    df = pd.read_csv(path, parse_dates=True, index_col=0)
    print(f"загружено строк {df.shape[0]} строк и {df.shape[1]} столбцов")
    df = df.drop(["_download_date", "_download_timestamp"], axis=1, errors="ignore")
    
    monthly_col_names = ["CPIAUCSL", "UNRATE", "FEDFUNDS", "M2SL", "INDPRO", 'GDP']
    
    for col in monthly_col_names:
        if col in df.columns:
            df[col] = df[col].ffill()
    
    monthly_data = pd.DataFrame()
    for col in monthly_col_names:
        if col in df.columns:
            monthly_data[col] = df[col].resample("ME").last()  

    if 'SP500' in df.columns:
        monthly_data["SP500"] = df["SP500"].resample("ME").mean()  

    if "DGS10" in df.columns:
        monthly_data["DGS10"] = df["DGS10"].resample("ME").mean()  
    
    print(f" После очистки: {len(monthly_data)} месяцев")
    return monthly_data
    
def normalize_data(monthly_data):

    
    print("Нормализация данных")
    
    features_to_scale = monthly_data.select_dtypes(include=['float64', 'int64']).columns.tolist()
    print(f"Признаки для нормализации: {features_to_scale}")
    
    split_idx = int(len(monthly_data) * 0.8)
    
    train_data = monthly_data.iloc[:split_idx].copy()
    test_data = monthly_data.iloc[split_idx:].copy()
    
    print(f"Train: {len(train_data)} месяцев ({train_data.index[0].date()} - {train_data.index[-1].date()})")
    print(f"Test: {len(test_data)} месяцев ({test_data.index[0].date()} - {test_data.index[-1].date()})")
    
    scaler = StandardScaler()  
    
    scaler.fit(train_data[features_to_scale])

    train_scaled = pd.DataFrame(
        scaler.transform(train_data[features_to_scale]),
        columns=features_to_scale,
        index=train_data.index
    )
    
    test_scaled = pd.DataFrame(
        scaler.transform(test_data[features_to_scale]),
        columns=features_to_scale,
        index=test_data.index
    )
    
    output_dir = "./data"
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    scaler_path = os.path.join(output_dir, "scaler.pkl")
    joblib.dump(scaler, scaler_path)
    print(f"Scaler сохранен: {scaler_path}")
    
    print("\n Статистика ДО нормализации (train):")
    print(f"  CPI: mean={train_data['CPIAUCSL'].mean():.2f}, std={train_data['CPIAUCSL'].std():.2f}")
    print(f"  UNRATE: mean={train_data['UNRATE'].mean():.2f}, std={train_data['UNRATE'].std():.2f}")
    
    print("\n Статистика ПОСЛЕ нормализации (train):")
    print(f"  CPI: mean={train_scaled['CPIAUCSL'].mean():.2f}, std={train_scaled['CPIAUCSL'].std():.2f}")
    print(f"  UNRATE: mean={train_scaled['UNRATE'].mean():.2f}, std={train_scaled['UNRATE'].std():.2f}")

    return {
        'train_data': train_data,      
        'test_data': test_data,        
        'train_scaled': train_scaled,  
        'test_scaled': test_scaled,  
        'scaler': scaler,              
        'features': features_to_scale  
    }

def main():
    monthly_data = clean_data("data/fred_daily_data.csv")

    normalized = normalize_data(monthly_data)
    
    print(f"\n Нормализация завершена!")
    print(f"   Train shape: {normalized['train_scaled'].shape}")
    print(f"   Test shape: {normalized['test_scaled'].shape}")
    
    return normalized


if __name__ == "__main__":
    main()