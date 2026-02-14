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
    
    print("Статистика ДО нормализации (train):")
    print(f"CPI: mean={train_data['CPIAUCSL'].mean():.2f}, std={train_data['CPIAUCSL'].std():.2f}")
    print(f"  UNRATE: mean={train_data['UNRATE'].mean():.2f}, std={train_data['UNRATE'].std():.2f}")
    
    print("Статистика ПОСЛЕ нормализации (train):")
    print(f"CPI: mean={train_scaled['CPIAUCSL'].mean():.2f}, std={train_scaled['CPIAUCSL'].std():.2f}")
    print(f"UNRATE: mean={train_scaled['UNRATE'].mean():.2f}, std={train_scaled['UNRATE'].std():.2f}")

    return {
        'train_data': train_data,      
        'test_data': test_data,        
        'train_scaled': train_scaled,  
        'test_scaled': test_scaled,  
        'scaler': scaler,              
        'features': features_to_scale  
    }


def create_features_and_target(data):
    """Создаём признаки и целевую переменную"""
    
    features = data.copy()
    
    features['target'] = features['CPIAUCSL'].shift(-6)
    
    for lag in [1, 2, 3, 6, 12]:
        features[f'cpi_lag_{lag}m'] = features['CPIAUCSL'].shift(lag)
    
    for lag in [1, 3, 6]:
        features[f'unrate_lag_{lag}m'] = features['UNRATE'].shift(lag)
        features[f'fedfunds_lag_{lag}m'] = features['FEDFUNDS'].shift(lag)
    
    features['cpi_pct_1m'] = features['CPIAUCSL'].pct_change(1)
    features['cpi_pct_12m'] = features['CPIAUCSL'].pct_change(12)
    
    features['cpi_ma_3m'] = features['CPIAUCSL'].rolling(3).mean()
    features['cpi_ma_12m'] = features['CPIAUCSL'].rolling(12).mean()
    
    features['real_rate'] = features['FEDFUNDS'] - features['cpi_pct_12m'] * 100
    features['yield_spread'] = features['DGS10'] - features['FEDFUNDS']
    
    features = features.dropna()
    
    print(f"Признаков создано: {features.shape[1]}")
    print(f"Строк для обучения: {features.shape[0]}")
    
    return features

def train_model(features):
    """Обучает модель предсказания инфляции"""
    
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import mean_absolute_error, r2_score
    import joblib
    
    exclude_cols = ['target', 'CPIAUCSL']
    X = features.drop(columns=[c for c in exclude_cols if c in features.columns])
    y = features['target']
    
    split = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]
    
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print(f"РЕЗУЛЬТАТЫ МОДЕЛИ:")
    print(f"MAE: {mae:.2f} (ошибка в пунктах CPI)")
    print(f"R²:  {r2:.3f}")
    
    feat_imp = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False).head(10)
    
    print(f"Важные признаки:")
    print(feat_imp.head().to_string(index=False))
    
    joblib.dump(model, './data/inflation_model.pkl')
    print(f"Модель сохранена: ./data/inflation_model.pkl")
    
    return model, X_train, X_test, y_train, y_test

def main():
    monthly_data = clean_data("data/fred_daily_data.csv")
    
    normalized = normalize_data(monthly_data)
    
    features = create_features_and_target(monthly_data)  
    model, X_train, X_test, y_train, y_test = train_model(features)
    
    return model

if __name__ == "__main__":
    main()