import pytest
import pandas as pd
import sqlite3
import tempfile
import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import store


@pytest.fixture
def tmp_db(tmp_path):
    """Banco temporário isolado por teste."""
    db = str(tmp_path / "test.db")
    store.init_db(db_file=db)
    return db


def make_df(overrides: list[dict] = None) -> pd.DataFrame:
    now = datetime.now(timezone.utc).isoformat()
    base = {
        'item_id': 'T4_ORE',
        'city': 'Thetford',
        'quality': 1,
        'sell_price_min': 1000,
        'timestamp_sell_min': now,
        'buy_price_max': 800,
        'timestamp_buy_max': now,
        'tier': 4,
    }
    rows = [base] if not overrides else [{**base, **o} for o in overrides]
    return pd.DataFrame(rows)


class TestInitDb:
    def test_cria_tabela(self, tmp_db):
        with sqlite3.connect(tmp_db) as con:
            cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='market_prices'")
            assert cur.fetchone() is not None

    def test_idempotente(self, tmp_db):
        store.init_db(db_file=tmp_db)
        store.init_db(db_file=tmp_db)


class TestInsertPrices:
    def test_insere_registro(self, tmp_db):
        count = store.insert_prices(make_df(), db_file=tmp_db)
        assert count == 1

    def test_df_vazio_retorna_zero(self, tmp_db):
        assert store.insert_prices(pd.DataFrame(), db_file=tmp_db) == 0

    def test_retorna_apenas_registros_novos(self, tmp_db):
        df = make_df()
        store.insert_prices(df, db_file=tmp_db)
        # Segunda inserção do mesmo item não é nova linha
        count = store.insert_prices(df, db_file=tmp_db)
        assert count == 0

    def test_nova_cidade_conta_como_novo(self, tmp_db):
        store.insert_prices(make_df(), db_file=tmp_db)
        df2 = make_df([{'city': 'Lymhurst'}])
        count = store.insert_prices(df2, db_file=tmp_db)
        assert count == 1

    def test_multiplos_itens(self, tmp_db):
        df = make_df([
            {'item_id': 'T4_ORE', 'city': 'Thetford'},
            {'item_id': 'T4_ORE', 'city': 'Lymhurst'},
            {'item_id': 'T5_ORE', 'city': 'Thetford'},
        ])
        count = store.insert_prices(df, db_file=tmp_db)
        assert count == 3


class TestGetPrices:
    def test_retorna_vazio_sem_banco(self, tmp_path):
        result = store.get_prices(db_file=str(tmp_path / "inexistente.db"))
        assert result.empty

    def test_retorna_dados_inseridos(self, tmp_db):
        store.insert_prices(make_df(), db_file=tmp_db)
        df = store.get_prices(db_file=tmp_db)
        assert len(df) == 1
        assert df.iloc[0]['item_id'] == 'T4_ORE'

    def test_colunas_de_tempo_sao_datetime(self, tmp_db):
        store.insert_prices(make_df(), db_file=tmp_db)
        df = store.get_prices(db_file=tmp_db)
        assert pd.api.types.is_datetime64_any_dtype(df['timestamp_sell_min'])
        assert pd.api.types.is_datetime64_any_dtype(df['timestamp_buy_max'])


class TestPurgeStale:
    def test_remove_registros_antigos(self, tmp_db):
        old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
        df = make_df([{'timestamp_sell_min': old_ts}])
        store.insert_prices(df, db_file=tmp_db)
        removed = store.purge_stale(hours=168, db_file=tmp_db)
        assert removed == 1
        assert store.get_prices(db_file=tmp_db).empty

    def test_preserva_registros_recentes(self, tmp_db):
        store.insert_prices(make_df(), db_file=tmp_db)
        removed = store.purge_stale(hours=168, db_file=tmp_db)
        assert removed == 0
        assert len(store.get_prices(db_file=tmp_db)) == 1

    def test_banco_inexistente_retorna_zero(self, tmp_path):
        result = store.purge_stale(db_file=str(tmp_path / "inexistente.db"))
        assert result == 0
