import pandas as pd
import json
import requests
from datetime import datetime
import re

# Constantes de Configuração
BASE_API_URL = "https://www.albion-online-data.com/api/v2/stats/prices"
NULL_TIMESTAMP = "0001-01-01T00:00:00" # Valor padrão da API para datas nulas

def load_sample_data(filepath: str = 'sample_data.json') -> pd.DataFrame:
    """
    Carrega dados de um arquivo JSON local para fins de teste e desenvolvimento.
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        
        # Compatibilidade com formatos antigos de sample_data
        if 'timestamp' in df.columns:
            df['timestamp_sell_min'] = df['timestamp']
            df['timestamp_buy_max'] = df['timestamp']
            df = df.drop(columns=['timestamp'])
            
        # Extração de Tier baseada no padrão de ID (ex: T4_...)
        df['tier'] = df['item_id'].str.extract(r'T(\d)')[0].fillna(0).astype(int)
        
        print(f"DEBUG: Dados de amostra carregados de {filepath}.")
        return df

    except FileNotFoundError:
        print(f"ERRO: Arquivo '{filepath}' não encontrado.")
        return pd.DataFrame()
    except Exception as e:
        print(f"ERRO: Falha ao processar sample data: {e}")
        return pd.DataFrame()

def fetch_prices_real(items: list[str], cities: list[str], qualities: list[int]) -> pd.DataFrame:
    """
    Busca preços atuais na API pública do Albion Data Project.
    """
    if not items or not cities:
        print("AVISO: Lista de itens ou cidades vazia.")
        return pd.DataFrame()

    # Preparação dos parâmetros da URL
    item_list_str = ",".join(items).upper()
    
    params = {
        'locations': ",".join(cities),
        'qualities': ",".join(map(str, qualities))
    }
    
    url = f"{BASE_API_URL}/{item_list_str}"
    
    print(f"API REQUEST: {url} | Params: {params}")
    
    try:
        # Timeout de 10s para evitar travamento do app
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status() 
        
        data = response.json()
        
        if not data:
            print("API: Nenhum dado retornado para os filtros selecionados.")
            return pd.DataFrame()

        # --- Normalização e Limpeza ---
        df = pd.DataFrame(data)
        
        # Mapeamento para nomes de colunas internos do projeto
        df = df.rename(columns={
            'sell_price_min_date': 'timestamp_sell_min',
            'buy_price_max_date': 'timestamp_buy_max'
        })
        
        # Enriquecimento de dados
        df['tier'] = df['item_id'].str.extract(r'T(\d)')[0].fillna(0).astype(int)
        
        # Tratamento de datas nulas da API
        df['timestamp_sell_min'] = df['timestamp_sell_min'].replace(NULL_TIMESTAMP, pd.NaT)
        df['timestamp_buy_max'] = df['timestamp_buy_max'].replace(NULL_TIMESTAMP, pd.NaT)

        # Garantia de Schema (Cols obrigatórias)
        expected_cols = [
            'item_id', 'city', 'quality', 
            'sell_price_min', 'timestamp_sell_min',
            'buy_price_max', 'timestamp_buy_max',
            'tier'
        ]
        
        # Preenche colunas faltantes caso a API mude o formato
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None

        return df[expected_cols]
    except requests.Timeout:
        print("ERRO API: Timeout na conexão (servidor demorou a responder).")
        return pd.DataFrame()
    except requests.RequestException as e:
        print(f"ERRO API: Falha na requisição: {e}")
        return pd.DataFrame()

def fetch_sales_history(items: list[str], city: str, quality: int = 1) -> dict:
    try:
        if not items:
            return {}

        chunk_size = 20
        results = {}
        location_param = city.replace(" ", "%20")
        time_scale = 24 

        for i in range(0, len(items), chunk_size):
            chunk = items[i:i+chunk_size]
            items_str = ",".join(chunk)
            
            url = f"https://www.albion-online-data.com/api/v2/stats/history/{items_str}?locations={location_param}&qualities={quality}&time-scale={time_scale}"
            
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                continue
                
            data = response.json()
            
            for entry in data:
                item_id = entry.get('item_id')
                history_data = entry.get('data', []) 
                
                if not history_data:
                    results[item_id] = 0
                    continue
                
                relevant_data = history_data[-3:] 
                total_items = sum([d.get('item_count', 0) for d in relevant_data])
                days = len(relevant_data)
                
                avg_daily = int(total_items / days) if days > 0 else 0
                results[item_id] = avg_daily
                
        return results

    except Exception as e:
        print(f"ERRO fetch_sales_history: {e}")
        return {}    

if __name__ == "__main__":
    print("Execute 'streamlit run app.py' para usar a ferramenta.")