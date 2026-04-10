# Dicionário de Categorias e Itens Base
# Mapeia: "Nome Amigável" -> "SUFIXO_DO_ID"

CATEGORIES = {
    "Recursos (Coleta)": {
        "Minério (Ore)": "ORE",
        "Madeira (Wood)": "WOOD",
        "Pelego (Hide)": "HIDE",
        "Fibra (Fiber)": "FIBER",
        "Pedra (Rock)": "ROCK"
    },
    "Recursos Refinados": {
        "Barra de Metal": "METALBAR",
        "Tábua de Madeira": "PLANKS",
        "Couro": "LEATHER",
        "Tecido": "CLOTH",
        "Bloco de Pedra": "STONEBLOCK"
    },
    "Consumíveis": {
        "Poção de Cura": "POTION_HEAL",
        "Poção de Energia": "POTION_ENERGY",
        "Sopa (Vida)": "MEAL_SOUP",
        "Salada (Crafting)": "MEAL_SALAD",
        "Torta (Carga/Defesa)": "MEAL_PIE",
        "Omelete (Cooldown)": "MEAL_OMELETTE",
        "Ensopado (Dano)": "MEAL_STEW",
        "Sanduíche (HP Max)": "MEAL_SANDWICH"
    },
    "Equipamentos (Básico)": {
        "Bolsa": "BAG",
        "Capa": "CAPE"
    },
    "Montarias (Simples)": {
        "Cavalo de Montar": "MOUNT_HORSE",
        "Boi de Transporte": "MOUNT_OX"
    }
}

def generate_item_list(base_items_dict, selected_tiers, selected_enchants):
    """
    Gera a lista técnica de IDs baseada nas seleções do usuário.
    
    Exemplo:
    Input: base_items=['ORE'], tiers=[4], enchants=[0, 1]
    Output: ['T4_ORE', 'T4_ORE_LEVEL1@1']
    """
    final_ids = []
    
    # Se o usuário não selecionou encantamento, assumimos 0 (Flat)
    if not selected_enchants:
        selected_enchants = [0]

    for friendly_name, base_id in base_items_dict.items():
        for tier in selected_tiers:
            # Monta o ID base: T4_ORE
            tier_prefix = f"T{tier}_"
            full_base = tier_prefix + base_id
            
            for ench in selected_enchants:
                if ench == 0:
                    # Item Flat: T4_ORE
                    final_ids.append(full_base)
                else:
                    # Item Encantado: T4_ORE@1
                    # Formato oficial da Albion Data Project API para recursos encantados.
                    item_id_ench = f"{full_base}@{ench}"
                    final_ids.append(item_id_ench)
                    
    return final_ids