import pandas as pd
from datetime import datetime, timezone

def find_arbitrage(df_prices: pd.DataFrame, fee_pct: float, transport_cost: int = 0, top_n: int = 50, method: str = 'sell_order') -> pd.DataFrame:
    """
    Calcula arbitragem com duas estratégias de venda.
    method: 'instant' (Vende para Buy Order) ou 'sell_order' (Coloca Sell Order).
    """
    if df_prices.empty:
        return pd.DataFrame()

    # 1. Preparação e Tipagem
    df = df_prices.copy()
    cols_num = ['sell_price_min', 'buy_price_max']
    for col in cols_num:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Garante datas em UTC
    cols_time = ['timestamp_sell_min', 'timestamp_buy_max']
    for col in cols_time:
        df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')

    # Filtra registros inválidos
    df = df.dropna(subset=cols_time)
    
    # Sempre precisamos de um preço de venda válido para COMPRAR o item
    df = df[df['sell_price_min'] > 0]

    if df.empty:
        return pd.DataFrame()

    # 2. Confiança
    now_utc = datetime.now(timezone.utc)
    MAX_AGE_HOURS = 72.0 

    df['age_hours_buy'] = (now_utc - df['timestamp_sell_min']).dt.total_seconds() / 3600
    df['confidence_buy'] = (1.0 - (df['age_hours_buy'] / MAX_AGE_HOURS)).clip(0, 1)
    
    # Para venda, a confiança depende do método
    if method == 'instant':
        df['age_hours_sell'] = (now_utc - df['timestamp_buy_max']).dt.total_seconds() / 3600
    else:
        # Se for vender como Sell Order, a recência do Sell Price também importa
        df['age_hours_sell'] = (now_utc - df['timestamp_sell_min']).dt.total_seconds() / 3600
        
    df['confidence_sell'] = (1.0 - (df['age_hours_sell'] / MAX_AGE_HOURS)).clip(0, 1)

    df['item_key'] = df['item_id'] + '_Q' + df['quality'].astype(str)

    # 3. Definição dos Lados (Compra vs Venda)
    
    # LADO A: COMPRA (Sempre compramos da Sell Order mais barata)
    df_buy = df.rename(columns={
        'city': 'buy_city',
        'sell_price_min': 'buy_price',
        'timestamp_sell_min': 'timestamp_buy',
        'confidence_buy': 'confidence_buy'
    })[['item_key', 'buy_city', 'buy_price', 'timestamp_buy', 'confidence_buy']]

    # LADO B: VENDA (Depende da Estratégia)
    if method == 'instant':
        # Estratégia: Vender para Buy Order (Instantâneo)
        df_sell = df.rename(columns={
            'city': 'sell_city',
            'buy_price_max': 'sell_price', # Vende pelo preço que estão pagando
            'timestamp_buy_max': 'timestamp_sell',
            'confidence_sell': 'confidence_sell'
        })
        # Filtrar apenas quem tem ordem de compra
        df_sell = df_sell[df_sell['sell_price'] > 0]
        
    else:
        # Estratégia: Colocar Sell Order (Trading/Transporte)
        df_sell = df.rename(columns={
            'city': 'sell_city',
            'sell_price_min': 'sell_price', # Vende competindo com o menor preço de venda
            'timestamp_sell_min': 'timestamp_sell',
            'confidence_sell': 'confidence_sell'
        })

    df_sell = df_sell[['item_key', 'sell_city', 'sell_price', 'timestamp_sell', 'confidence_sell']]

    # 4. Cruzamento
    df_arb = pd.merge(df_buy, df_sell, on='item_key')

    # 5. Filtros
    df_arb = df_arb[df_arb['buy_city'] != df_arb['sell_city']]

    # 6. Cálculos
    df_arb['gross_profit'] = df_arb['sell_price'] - df_arb['buy_price']
    
    fee_multiplier = 1.0 - (fee_pct / 100.0)
    df_arb['net_profit'] = (df_arb['sell_price'] * fee_multiplier) - df_arb['buy_price'] - transport_cost

    df_arb = df_arb[df_arb['net_profit'] > 0]

    if df_arb.empty:
        return pd.DataFrame()

    # profit_pct representa a oportunidade de mercado (após taxas, antes do transporte)
    # para que o filtro de ROI% não seja distorcido pelo custo de transporte
    df_arb['profit_pct'] = ((df_arb['sell_price'] * fee_multiplier) - df_arb['buy_price']) / df_arb['buy_price'] * 100.0
    df_arb['confidence_score'] = (df_arb['confidence_buy'] + df_arb['confidence_sell']) / 2.0
    
    df_final = df_arb[[
        'item_key', 'buy_city', 'sell_city', 'buy_price', 'sell_price',
        'gross_profit', 'net_profit', 'profit_pct', 'confidence_score',
        'timestamp_buy', 'timestamp_sell'
    ]]
    
    df_final = df_final.rename(columns={'item_key': 'item_id_quality'})

    return df_final.sort_values(by='net_profit', ascending=False).head(top_n).reset_index(drop=True)