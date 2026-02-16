from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import time

FRED_API_KEY = "11b3ddb5b880da8059e280fd7015dc35"

SERIES_TO_DOWNLOAD = [
    'CPIAUCSL',   
    'UNRATE',        
    'FEDFUNDS',    
    'M2SL',        
    'SP500',       
    'DGS10',       
    'GDP',         
    'INDPRO',      
]

CSV_FILE_PATH = "/opt/airflow/data/fred_daily_data.csv"

REQUEST_TIMEOUT = 15  
DELAY_BETWEEN_REQUESTS = 0.5  

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=15), 
}

dag = DAG(
    'fred_daily_working',
    default_args=default_args,
    description='Рабочая загрузка данных из FRED',
    schedule_interval='0 12 * * *', 
    catchup=False,
    tags=['fred', 'production'],
)

def download_fred_working(**kwargs):
    print(f"Начало выполнения: {datetime.now()}")
    print(f"Будет загружено показателей: {len(SERIES_TO_DOWNLOAD)}")
    
    all_data = {}
    success_count = 0
    fail_count = 0
    
    for idx, series_id in enumerate(SERIES_TO_DOWNLOAD, 1):
        print(f"\n[{idx}/{len(SERIES_TO_DOWNLOAD)}] Загружаю {series_id}...")
        
        try:
            url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': series_id,
                'api_key': FRED_API_KEY,
                'file_type': 'json',
                'observation_start': '2020-01-01',
            }
            
            response = requests.get(
                url, 
                params=params, 
                timeout=(10, REQUEST_TIMEOUT)
            )
            
            if response.status_code == 200:
                data = response.json()
                observations = data.get('observations', [])
                
                if observations:
                    dates = []
                    values = []
                    
                    for obs in observations:
                        val = obs['value']
                        if val != '.' and val is not None:
                            dates.append(obs['date'])
                            try:
                                values.append(float(val))
                            except ValueError:
                                continue  
                    
                    if values:
                        series = pd.Series(
                            values, 
                            index=pd.to_datetime(dates),
                            name=series_id
                        )
                        all_data[series_id] = series
                        success_count += 1
            
                        print(f"Успех: {len(values)} записей")
                        print(f"Диапазон: {dates[0]} - {dates[-1]}")
                        print(f"Последнее: {values[-1]:.2f}")
                    else:
                        print(f"Нет числовых данных")
                        fail_count += 1
                else:
                    print(f"Пустой ответ")
                    fail_count += 1
                    
            elif response.status_code == 429:
                print(f"Rate limit превышен. Жду 5 секунд...")
                time.sleep(5)
                fail_count += 1
            else:
                print(f"HTTP {response.status_code}: {response.text[:100]}")
                fail_count += 1
                
        except requests.exceptions.Timeout:
            print(f"Таймаут запроса")
            fail_count += 1
        except Exception as e:
            print(f"Ошибка: {str(e)[:100]}")
            fail_count += 1
        
        if idx < len(SERIES_TO_DOWNLOAD):
            time.sleep(DELAY_BETWEEN_REQUESTS)
    
    print(f"ИТОГИ ЗАГРУЗКИ:")
    print(f"Успешно: {success_count}/{len(SERIES_TO_DOWNLOAD)}")
    print(f"Неудачно: {fail_count}/{len(SERIES_TO_DOWNLOAD)}")
    
    if all_data:
        df = pd.DataFrame(all_data)
        
        df = df.sort_index()
        
        df['_download_date'] = datetime.now().strftime('%Y-%m-%d')
        df['_download_timestamp'] = datetime.now()
        
        os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
        
        df.to_csv(CSV_FILE_PATH, index=True)
        
        print(f"ДАННЫЕ СОХРАНЕНЫ:")
        print(f"Файл: {CSV_FILE_PATH}")
        print(f"Размер: {len(df)} строк × {len(df.columns)} колонок")
        print(f"Объём: {os.path.getsize(CSV_FILE_PATH) / 1024:.1f} KB")
        
        print(f"ПОСЛЕДНИЕ ЗНАЧЕНИЯ:")
        for col in list(df.columns)[:6]:  
            if col.startswith('_'):
                continue
            last_val = df[col].iloc[-1] if not df[col].isna().all() else 'N/A'
            print(f"   {col}: {last_val}")
        
        summary_path = CSV_FILE_PATH.replace('.csv', '_summary.txt')
        with open(summary_path, 'w') as f:
            f.write(f"FRED Data Download Summary\n")
            f.write(f"Date: {datetime.now()}\n")
            f.write(f"Total series: {len(SERIES_TO_DOWNLOAD)}\n")
            f.write(f"Successfully downloaded: {success_count}\n")
            f.write(f"Failed: {fail_count}\n")
            f.write(f"Data shape: {df.shape}\n")
        
        print(f"Summary сохранён в: {summary_path}")
        
        return CSV_FILE_PATH
        
    else:
        print("КРИТИЧЕСКАЯ ОШИБКА: Не загружено ни одного показателя!")
        
        error_path = CSV_FILE_PATH.replace('.csv', '_ERROR.txt')
        with open(error_path, 'w') as f:
            f.write(f"FRED Download FAILED\n")
            f.write(f"Time: {datetime.now()}\n")
            f.write(f"All {len(SERIES_TO_DOWNLOAD)} series failed\n")
        
        print(f"Ошибка записана в: {error_path}")
        
        raise Exception(f"Failed to download any FRED data. Check {error_path}")

download_task = PythonOperator(
    task_id='download_fred_data',
    python_callable=download_fred_working,
    dag=dag,
)

download_task