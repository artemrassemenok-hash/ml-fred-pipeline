from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import time

# --- НАСТРОЙКИ --- #
FRED_API_KEY = "11b3ddb5b880da8059e280fd7015dc35"

SERIES_TO_DOWNLOAD = [
    'CPIAUCSL',    # Consumer Price Index
    'UNRATE',      # Unemployment Rate  
    'FEDFUNDS',    # Federal Funds Rate
    'M2SL',        # Money Supply M2
    'SP500',       # S&P 500 Index
    'DGS10',       # 10-Year Treasury Yield
    'GDP',         # Gross Domestic Product
    'INDPRO',      # Industrial Production
]

CSV_FILE_PATH = "/opt/airflow/data/fred_daily_data.csv"

# Настройки для стабильной работы
REQUEST_TIMEOUT = 15  # секунд
DELAY_BETWEEN_REQUESTS = 0.5  # секунд
# ----------------------------- #

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=15),  # 15 минут макс
}

dag = DAG(
    'fred_daily_working',
    default_args=default_args,
    description='Рабочая загрузка данных из FRED',
    schedule_interval='0 12 * * *',  # Каждый день в 12:00 UTC
    catchup=False,
    tags=['fred', 'production'],
)

def download_fred_working(**kwargs):
    """
    РАБОЧАЯ версия загрузки FRED данных
    Использует тот же подход, что и успешный тестовый DAG
    """
    print(f"⏰ Начало выполнения: {datetime.now()}")
    print(f"📊 Будет загружено показателей: {len(SERIES_TO_DOWNLOAD)}")
    
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
                'observation_start': '2020-01-01',  # Можно изменить
                # Без limit - берём все данные
            }
            
            # Запрос с таймаутом
            response = requests.get(
                url, 
                params=params, 
                timeout=(10, REQUEST_TIMEOUT)
            )
            
            if response.status_code == 200:
                data = response.json()
                observations = data.get('observations', [])
                
                if observations:
                    # Парсим данные
                    dates = []
                    values = []
                    
                    for obs in observations:
                        val = obs['value']
                        if val != '.' and val is not None:
                            dates.append(obs['date'])
                            try:
                                values.append(float(val))
                            except ValueError:
                                continue  # Пропускаем некорректные
                    
                    if values:
                        # Создаём временной ряд
                        series = pd.Series(
                            values, 
                            index=pd.to_datetime(dates),
                            name=series_id
                        )
                        all_data[series_id] = series
                        success_count += 1
                        
                        # Показываем статистику
                        print(f"   ✅ Успех: {len(values)} записей")
                        print(f"      Диапазон: {dates[0]} - {dates[-1]}")
                        print(f"      Последнее: {values[-1]:.2f}")
                    else:
                        print(f"   ⚠️ Нет числовых данных")
                        fail_count += 1
                else:
                    print(f"   ⚠️ Пустой ответ")
                    fail_count += 1
                    
            elif response.status_code == 429:
                print(f"   ⚠️ Rate limit превышен. Жду 5 секунд...")
                time.sleep(5)
                fail_count += 1
            else:
                print(f"   ❌ HTTP {response.status_code}: {response.text[:100]}")
                fail_count += 1
                
        except requests.exceptions.Timeout:
            print(f"   ⏰ Таймаут запроса")
            fail_count += 1
        except Exception as e:
            print(f"   💥 Ошибка: {str(e)[:100]}")
            fail_count += 1
        
        # Пауза между запросами чтобы не спамить API
        if idx < len(SERIES_TO_DOWNLOAD):
            time.sleep(DELAY_BETWEEN_REQUESTS)
    
    # --- СОХРАНЕНИЕ РЕЗУЛЬТАТОВ --- #
    print(f"\n{'='*50}")
    print(f"📈 ИТОГИ ЗАГРУЗКИ:")
    print(f"   Успешно: {success_count}/{len(SERIES_TO_DOWNLOAD)}")
    print(f"   Неудачно: {fail_count}/{len(SERIES_TO_DOWNLOAD)}")
    print(f"{'='*50}\n")
    
    if all_data:
        # Собираем DataFrame
        df = pd.DataFrame(all_data)
        
        # Сортируем по дате
        df = df.sort_index()
        
        # Добавляем метаданные
        df['_download_date'] = datetime.now().strftime('%Y-%m-%d')
        df['_download_timestamp'] = datetime.now()
        
        # Создаём папку если нет
        os.makedirs(os.path.dirname(CSV_FILE_PATH), exist_ok=True)
        
        # Сохраняем в CSV
        df.to_csv(CSV_FILE_PATH, index=True)
        
        print(f"💾 ДАННЫЕ СОХРАНЕНЫ:")
        print(f"   Файл: {CSV_FILE_PATH}")
        print(f"   Размер: {len(df)} строк × {len(df.columns)} колонок")
        print(f"   Объём: {os.path.getsize(CSV_FILE_PATH) / 1024:.1f} KB")
        
        # Показываем последние значения
        print(f"\n📊 ПОСЛЕДНИЕ ЗНАЧЕНИЯ:")
        for col in list(df.columns)[:6]:  # Покажем первые 6 показателей
            if col.startswith('_'):
                continue
            last_val = df[col].iloc[-1] if not df[col].isna().all() else 'N/A'
            print(f"   {col}: {last_val}")
        
        # Сохраняем также summary файл
        summary_path = CSV_FILE_PATH.replace('.csv', '_summary.txt')
        with open(summary_path, 'w') as f:
            f.write(f"FRED Data Download Summary\n")
            f.write(f"Date: {datetime.now()}\n")
            f.write(f"Total series: {len(SERIES_TO_DOWNLOAD)}\n")
            f.write(f"Successfully downloaded: {success_count}\n")
            f.write(f"Failed: {fail_count}\n")
            f.write(f"Data shape: {df.shape}\n")
        
        print(f"\n📝 Summary сохранён в: {summary_path}")
        
        return CSV_FILE_PATH
        
    else:
        print("❌ КРИТИЧЕСКАЯ ОШИБКА: Не загружено ни одного показателя!")
        
        # Создаём файл с ошибкой
        error_path = CSV_FILE_PATH.replace('.csv', '_ERROR.txt')
        with open(error_path, 'w') as f:
            f.write(f"FRED Download FAILED\n")
            f.write(f"Time: {datetime.now()}\n")
            f.write(f"All {len(SERIES_TO_DOWNLOAD)} series failed\n")
        
        print(f"📝 Ошибка записана в: {error_path}")
        
        # Принудительно вызываем исключение чтобы Airflow знал о проблеме
        raise Exception(f"Failed to download any FRED data. Check {error_path}")

# Создаём задачу
download_task = PythonOperator(
    task_id='download_fred_data',
    python_callable=download_fred_working,
    dag=dag,
)

download_task