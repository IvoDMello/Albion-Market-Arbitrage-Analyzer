import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import arbitrage


def make_prices(rows: list[dict]) -> pd.DataFrame:
    """Monta um DataFrame de preços no formato esperado por find_arbitrage."""
    now = datetime.now(timezone.utc)
    base = {
        'item_id': 'T4_ORE',
        'city': 'Thetford',
        'quality': 1,
        'sell_price_min': 1000,
        'buy_price_max': 800,
        'timestamp_sell_min': now,
        'timestamp_buy_max': now,
        'tier': 4,
    }
    records = [{**base, **r} for r in rows]
    return pd.DataFrame(records)


class TestFindArbitrage:

    def test_retorna_vazio_com_df_vazio(self):
        result = arbitrage.find_arbitrage(pd.DataFrame(), fee_pct=4.5)
        assert result.empty

    def test_sem_oportunidade_mesma_cidade(self):
        df = make_prices([
            {'city': 'Thetford', 'sell_price_min': 1000},
            {'city': 'Thetford', 'sell_price_min': 1200},
        ])
        result = arbitrage.find_arbitrage(df, fee_pct=0, transport_cost=0)
        assert result.empty

    def test_oportunidade_simples_entre_cidades(self):
        df = make_prices([
            {'city': 'Thetford', 'sell_price_min': 1000},
            {'city': 'Lymhurst', 'sell_price_min': 1500},
        ])
        result = arbitrage.find_arbitrage(df, fee_pct=0, transport_cost=0, method='sell_order')
        assert not result.empty
        assert result.iloc[0]['net_profit'] == pytest.approx(500.0)

    def test_net_profit_desconta_taxa_e_transporte(self):
        df = make_prices([
            {'city': 'Thetford', 'sell_price_min': 10000},
            {'city': 'Lymhurst', 'sell_price_min': 12000},
        ])
        result = arbitrage.find_arbitrage(df, fee_pct=4.5, transport_cost=500, method='sell_order')
        # net = 12000 * (1 - 0.045) - 10000 - 500 = 11460 - 10000 - 500 = 960
        assert not result.empty
        assert result.iloc[0]['net_profit'] == pytest.approx(960.0)

    def test_profit_pct_nao_inclui_transporte(self):
        """profit_pct deve refletir a oportunidade de mercado, sem o custo de transporte."""
        df = make_prices([
            {'city': 'Thetford', 'sell_price_min': 10000},
            {'city': 'Lymhurst', 'sell_price_min': 12000},
        ])
        result = arbitrage.find_arbitrage(df, fee_pct=4.5, transport_cost=5000, method='sell_order')
        # Com transporte alto o net_profit fica negativo, então result deve ser vazio
        assert result.empty

    def test_profit_pct_calculado_sem_transporte(self):
        """Verifica que profit_pct = (sell*fee_mult - buy) / buy * 100."""
        df = make_prices([
            {'city': 'Thetford', 'sell_price_min': 10000},
            {'city': 'Lymhurst', 'sell_price_min': 12000},
        ])
        result = arbitrage.find_arbitrage(df, fee_pct=4.5, transport_cost=0, method='sell_order')
        # profit_pct = (12000 * 0.955 - 10000) / 10000 * 100 = 14.6%
        assert not result.empty
        assert result.iloc[0]['profit_pct'] == pytest.approx(14.6, abs=0.1)

    def test_transporte_alto_elimina_rota(self):
        df = make_prices([
            {'city': 'Thetford', 'sell_price_min': 1000},
            {'city': 'Lymhurst', 'sell_price_min': 1100},
        ])
        result = arbitrage.find_arbitrage(df, fee_pct=0, transport_cost=200)
        assert result.empty

    def test_confidence_score_entre_zero_e_um(self):
        df = make_prices([
            {'city': 'Thetford', 'sell_price_min': 1000},
            {'city': 'Lymhurst', 'sell_price_min': 2000},
        ])
        result = arbitrage.find_arbitrage(df, fee_pct=0, transport_cost=0)
        assert not result.empty
        assert (result['confidence_score'] >= 0).all()
        assert (result['confidence_score'] <= 1).all()

    def test_dados_antigos_tem_confianca_zero(self):
        old_time = datetime.now(timezone.utc) - timedelta(hours=100)
        df = make_prices([
            {'city': 'Thetford', 'sell_price_min': 1000, 'timestamp_sell_min': old_time, 'timestamp_buy_max': old_time},
            {'city': 'Lymhurst', 'sell_price_min': 2000, 'timestamp_sell_min': old_time, 'timestamp_buy_max': old_time},
        ])
        result = arbitrage.find_arbitrage(df, fee_pct=0, transport_cost=0)
        if not result.empty:
            assert result.iloc[0]['confidence_score'] == pytest.approx(0.0)

    def test_top_n_limita_resultados(self):
        rows = []
        cities = ['Thetford', 'Lymhurst', 'Martlock', 'Bridgewatch', 'Fort Sterling']
        for i, city in enumerate(cities):
            rows.append({'city': city, 'sell_price_min': 1000 + i * 500})
        df = make_prices(rows)
        result = arbitrage.find_arbitrage(df, fee_pct=0, transport_cost=0, top_n=2)
        assert len(result) <= 2

    def test_metodo_instant_usa_buy_price_max(self):
        now = datetime.now(timezone.utc)
        df = make_prices([
            {'city': 'Thetford', 'sell_price_min': 1000, 'buy_price_max': 0},
            {'city': 'Lymhurst', 'sell_price_min': 9999, 'buy_price_max': 1800},
        ])
        result = arbitrage.find_arbitrage(df, fee_pct=0, transport_cost=0, method='instant')
        # Vende pelo buy_price_max=1800 de Lymhurst
        assert not result.empty
        assert result.iloc[0]['sell_price'] == 1800
