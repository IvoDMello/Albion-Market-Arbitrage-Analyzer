import sqlite3
import pandas as pd
import os
from datetime import datetime, timezone

# Configurações do Banco de Dados
DB_FILE = "albion_market.db"
TABLE_NAME = "market_prices"

def init_db(db_file: str = DB_FILE):
    os.makedirs(os.path.dirname(os.path.abspath(db_file)), exist_ok=True)
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        item_id TEXT NOT NULL,
        city TEXT NOT NULL,
        quality INTEGER NOT NULL,
        sell_price_min INTEGER,
        timestamp_sell_min TEXT,
        buy_price_max INTEGER,
        timestamp_buy_max TEXT,
        tier INTEGER,
        PRIMARY KEY (item_id, city, quality)
    ) WITHOUT ROWID;
    """
    
    create_index_query = f"""
    CREATE INDEX IF NOT EXISTS idx_item_id
    ON {TABLE_NAME} (item_id);
    """
    
    try:
        with sqlite3.connect(db_file) as con:
            con.execute(create_table_query)
            con.execute(create_index_query)
    except sqlite3.Error as e:
        print(f"ERRO DB: Falha ao inicializar banco: {e}")

def clean_dataframe(df):
    """
    Função de Limpeza Profunda:
    Recupera números reais de dados corrompidos (bytes/tuplas) no SQLite.
    """
    df = df.copy()
    
    # --- Helper: Extrator Genérico ---
    def extract_number(val):
        try:
            if isinstance(val, (int, float)): return int(val)
            
            # Tenta limpar string suja
            if isinstance(val, str):
                if ',' in val: val = val.split(',')[0] # "4,0,0" -> 4
                val = val.replace("b'", "").replace("'", "")
                return int(float(val))
                
            # Tenta pegar primeiro item de lista/tupla
            if isinstance(val, (list, tuple)): return int(val[0])
            
            # Tenta converter bytes para int
            if isinstance(val, bytes):
                return int.from_bytes(val[:4], "little") # Assume 4 bytes int
                
            return 0
        except:
            return 0

    # --- 1. Limpeza de TIER ---
    df['tier'] = df['tier'].apply(extract_number)

    # --- 2. Limpeza de QUALIDADE ---
    def clean_quality(val):
        try:
            if isinstance(val, bytes): return int.from_bytes(val[:1], "little")
            return extract_number(val)
        except:
            return 1
            
    df['quality'] = df['quality'].apply(clean_quality)

    # --- 3. Limpeza de PREÇOS ---
    for col in ['sell_price_min', 'buy_price_max']:
        df[col] = df[col].apply(extract_number)

    # --- 4. Limpeza de DATAS ---
    for col in ['timestamp_sell_min', 'timestamp_buy_max']:
        df[col] = df[col].astype(str).replace({'nan': '', 'NaT': '', '<NA>': '', 'None': ''})
        
    return df

def insert_prices(df: pd.DataFrame, db_file: str = DB_FILE) -> int:
    if df.empty: return 0

    cols_to_db = [
        'item_id', 'city', 'quality', 
        'sell_price_min', 'timestamp_sell_min',
        'buy_price_max', 'timestamp_buy_max',
        'tier'
    ]
    
    for col in cols_to_db:
        if col not in df.columns:
            df[col] = None
            
    # LIMPA ANTES DE SALVAR
    df = clean_dataframe(df)

    df_clean = df[cols_to_db].dropna(subset=['item_id', 'city', 'quality'])
    df_clean = df_clean[df_clean['item_id'] != ""]

    if df_clean.empty: return 0

    try:
        with sqlite3.connect(db_file) as con:
            count_before = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
            data_tuples = [tuple(x) for x in df_clean.to_records(index=False)]
            insert_query = f"""
            INSERT OR REPLACE INTO {TABLE_NAME} (
                item_id, city, quality,
                sell_price_min, timestamp_sell_min,
                buy_price_max, timestamp_buy_max,
                tier
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """
            cursor = con.cursor()
            cursor.executemany(insert_query, data_tuples)
            con.commit()
            count_after = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
            return count_after - count_before
    except sqlite3.Error as e:
        print(f"ERRO DB: {e}")
        return 0

def get_prices(db_file: str = DB_FILE) -> pd.DataFrame:
    """
    Recupera histórico E LIMPA A SAÍDA VISUALMENTE.
    """
    if not os.path.exists(db_file):
        return pd.DataFrame()
        
    try:
        with sqlite3.connect(db_file) as con:
            query = f"SELECT * FROM {TABLE_NAME} ORDER BY item_id, city"
            df = pd.read_sql_query(query, con)
            
            # --- LIMPEZA VISUAL NA LEITURA ---
            df = clean_dataframe(df)
            
            time_cols = ['timestamp_sell_min', 'timestamp_buy_max']
            for col in time_cols:
                df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')
            
            return df
    except sqlite3.Error as e:
        print(f"ERRO DB LEITURA: {e}")
        return pd.DataFrame()

def purge_stale(hours: int = 168, db_file: str = DB_FILE) -> int:
    """Remove registros cujos preços de venda não foram atualizados nos últimos `hours` horas."""
    if not os.path.exists(db_file):
        return 0
    try:
        with sqlite3.connect(db_file) as con:
            cur = con.execute(
                f"""
                DELETE FROM {TABLE_NAME}
                WHERE timestamp_sell_min != ''
                  AND timestamp_sell_min IS NOT NULL
                  AND datetime(timestamp_sell_min) < datetime('now', '-{int(hours)} hours')
                """
            )
            con.commit()
            return cur.rowcount
    except sqlite3.Error as e:
        print(f"ERRO DB purge_stale: {e}")
        return 0

def get_last_update(db_file: str = DB_FILE) -> datetime | None:
    """Retorna o datetime da última atualização registrada no banco de dados."""
    if not os.path.exists(db_file):
        return None
    try:
        with sqlite3.connect(db_file) as con:
            cur = con.execute(
                f"SELECT MAX(timestamp_sell_min) FROM {TABLE_NAME} WHERE timestamp_sell_min != ''"
            )
            result = cur.fetchone()
            if result and result[0]:
                dt = pd.to_datetime(result[0], utc=True, errors='coerce')
                return dt.to_pydatetime() if not pd.isna(dt) else None
            return None
    except sqlite3.Error:
        return None