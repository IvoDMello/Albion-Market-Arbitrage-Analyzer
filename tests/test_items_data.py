import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import items_data


class TestGenerateItemList:

    def test_item_flat(self):
        result = items_data.generate_item_list({'Minério': 'ORE'}, [4], [0])
        assert result == ['T4_ORE']

    def test_item_encantado_formato_correto(self):
        """Formato da API: T4_ORE@1, não T4_ORE_LEVEL1@1."""
        result = items_data.generate_item_list({'Minério': 'ORE'}, [4], [1])
        assert result == ['T4_ORE@1']

    def test_multiplos_encantamentos(self):
        result = items_data.generate_item_list({'Minério': 'ORE'}, [4], [0, 1, 2])
        assert result == ['T4_ORE', 'T4_ORE@1', 'T4_ORE@2']

    def test_multiplos_tiers(self):
        result = items_data.generate_item_list({'Minério': 'ORE'}, [4, 5], [0])
        assert result == ['T4_ORE', 'T5_ORE']

    def test_multiplos_itens(self):
        result = items_data.generate_item_list({'Minério': 'ORE', 'Madeira': 'WOOD'}, [4], [0])
        assert 'T4_ORE' in result
        assert 'T4_WOOD' in result
        assert len(result) == 2

    def test_sem_encantamento_selecionado_usa_flat(self):
        result = items_data.generate_item_list({'Minério': 'ORE'}, [4], [])
        assert result == ['T4_ORE']

    def test_combinacao_completa(self):
        result = items_data.generate_item_list({'Minério': 'ORE'}, [4, 5], [0, 1])
        assert 'T4_ORE' in result
        assert 'T4_ORE@1' in result
        assert 'T5_ORE' in result
        assert 'T5_ORE@1' in result
        assert len(result) == 4

    def test_item_vazio_retorna_lista_vazia(self):
        result = items_data.generate_item_list({}, [4], [0])
        assert result == []

    def test_tier_vazio_retorna_lista_vazia(self):
        result = items_data.generate_item_list({'Minério': 'ORE'}, [], [0])
        assert result == []


class TestCategories:
    def test_categorias_existem(self):
        assert len(items_data.CATEGORIES) > 0

    def test_cada_categoria_tem_itens(self):
        for cat, items in items_data.CATEGORIES.items():
            assert len(items) > 0, f"Categoria '{cat}' está vazia"

    def test_valores_sao_strings_maiusculas(self):
        for cat, items in items_data.CATEGORIES.items():
            for name, suffix in items.items():
                assert suffix == suffix.upper(), f"Sufixo '{suffix}' em '{cat}' não está em maiúsculas"
