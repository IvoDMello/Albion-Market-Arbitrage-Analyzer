import streamlit as st
import pandas as pd
import os
import re
from datetime import datetime, timezone
import fetch_prices
import store
import arbitrage
import items_data

# Configuração da Página
st.set_page_config(
    layout="wide", 
    page_title="Albion Market Analyzer", 
    page_icon="📊"
)

# --- CABEÇALHO ---
st.title("📊 Albion Market Analyzer")

# --- Status do Banco de Dados ---
if not os.path.exists(store.DB_FILE):
    st.caption("⚠️ Banco de dados vazio. Configure os filtros ao lado e clique em 'Atualizar'.")
else:
    last_update = store.get_last_update()
    if last_update:
        age_hours = (datetime.now(timezone.utc) - last_update).total_seconds() / 3600
        age_str = f"{int(age_hours)}h atrás" if age_hours >= 1 else f"{int(age_hours * 60)}min atrás"
        freshness = "🟢" if age_hours < 6 else ("🟡" if age_hours < 24 else "🔴")
        st.caption(f"Status: Conectado {freshness} | Última atualização: {age_str}")
    else:
        st.caption("Status: Conectado | Sem dados ainda")

# --- Constantes Globais ---
CIDADES_REAIS = ["Thetford", "Fort Sterling", "Lymhurst", "Bridgewatch", "Martlock", "Caerleon", "Black Market"]
CIDADES_PORTAIS = ["Merlyn's Rest", "Arthur's Rest", "Morgana's Rest"]

# --- Helpers: Formatação Visual ---
def format_quality_name(q_id):
    try:
        q_id = int(q_id)
    except (ValueError, TypeError):
        pass
    mapping = {1: "Normal", 2: "Bom", 3: "Excepcional", 4: "Excelente", 5: "Obra-Prima"}
    return mapping.get(q_id, str(q_id))

def format_item_name_pt(item_id):
    if not isinstance(item_id, str): return str(item_id)
    translations = {
        'ORE': 'Minério', 'WOOD': 'Madeira', 'HIDE': 'Pelego', 'FIBER': 'Fibra', 'ROCK': 'Pedra',
        'BAR': 'Barra', 'PLANKS': 'Tábuas', 'LEATHER': 'Couro', 'CLOTH': 'Tecido', 'STONEBLOCK': 'Bloco Pedra',
        'METALBAR': 'Barra Metal', 'BAG': 'Bolsa', 'CAPE': 'Capa', 'MOUNT': 'Montaria', 
        'JOURNAL': 'Diário', 'POTION': 'Poção', 'MEAL': 'Comida'
    }
    parts = item_id.split('_')
    tier = ""
    enchant = ""
    name_parts = []
    for part in parts:
        if part.startswith('T') and part[1:].isdigit(): tier = f"Tier {part[1:]}"
        elif part.startswith('@') and part[1:].isdigit(): enchant = f".{part[1:]}"
        elif "LEVEL" in part: pass 
        elif part in translations: name_parts.append(translations[part])
        else: name_parts.append(part.title())
    base_name = " ".join(name_parts)
    final_name = f"{base_name} {tier}{enchant}".strip()
    return final_name if base_name else item_id

# ==========================================
# BARRA LATERAL (FILTROS E CONTROLES)
# ==========================================
st.sidebar.title("⚙️ Filtros & Ações")

# 1. Seleção de Itens
st.sidebar.subheader("1. O que você procura?")
tab_menu, tab_manual = st.sidebar.tabs(["📂 Categorias", "📝 Manual"])
final_items_list = []

with tab_menu:
    cat_options = list(items_data.CATEGORIES.keys())
    selected_cat = st.selectbox("Categoria", cat_options)
    sub_options = items_data.CATEGORIES[selected_cat] 
    selected_sub_names = st.multiselect("Itens", list(sub_options.keys()), default=list(sub_options.keys())[:1])
    
    col_tier, col_ench = st.columns(2)
    selected_tiers = col_tier.multiselect("Tiers", [3, 4, 5, 6, 7, 8], default=[4, 5])
    selected_enchants = col_ench.multiselect("Encant.", [0, 1, 2, 3, 4], default=[0], format_func=lambda x: f".{x}" if x>0 else "Flat")
    
    if selected_sub_names and selected_tiers:
        subset_dict = {k: v for k, v in sub_options.items() if k in selected_sub_names}
        generated_ids = items_data.generate_item_list(subset_dict, selected_tiers, selected_enchants)
        final_items_list = generated_ids

with tab_manual:
    manual_input = st.text_area("IDs (ex: T4_BAG)", "")
    if manual_input:
        final_items_list = [i.strip().upper() for i in re.split(r'[,\s\n]+', manual_input) if i.strip()]

# 2. Seleção de Cidades
st.sidebar.subheader("2. Onde?")
city_input = st.sidebar.multiselect("Cidades", CIDADES_REAIS + CIDADES_PORTAIS, default=CIDADES_REAIS)
quality_input = st.sidebar.multiselect("Qualidade", [1, 2, 3, 4, 5], default=[1], format_func=format_quality_name)

# 3. Botão de Ação
st.sidebar.markdown("---")
if st.sidebar.button("🔄 Atualizar Dados", type="primary"):
    if not final_items_list or not city_input:
        st.sidebar.error("Selecione itens e cidades.")
    else:
        with st.spinner(f"Buscando preços para {len(final_items_list)} itens..."):
            # Busca novos dados na API
            api_df = fetch_prices.fetch_prices_real(final_items_list, city_input, quality_input)
            
            # Atualiza o Banco de Dados
            store.init_db()
            count = store.insert_prices(api_df)
            
            if count > 0:
                st.sidebar.success(f"Atualizado! {count} preços novos.")
            else:
                st.sidebar.warning("API não retornou dados novos (talvez ninguém tenha escaneado esses itens recentemente).")

# 4. Parâmetros Econômicos
st.sidebar.markdown("---")
with st.sidebar.expander("💰 Taxas e Lucro", expanded=False):
    fee_pct = st.number_input("Taxa Mercado (%)", 0.0, 20.0, 4.5, step=0.5)
    transport_cost = st.number_input("Custo Transporte", 0, value=500, step=100)
    min_profit_pct = st.slider("Lucro Mínimo (ROI %)", 0, 200, 10)
    max_results = st.slider("Máx. Resultados", 10, 200, 50, step=10)
    arb_method = st.radio("Estratégia", ["Venda Lenta (Sell Order)", "Venda Imediata (Buy Order)"])
    method_code = 'sell_order' if "Lenta" in arb_method else 'instant'

# ==========================================
# ÁREA PRINCIPAL (MAIN)
# ==========================================

# 1. Carregar Tudo
try:
    all_prices_df = store.get_prices()
except Exception as e:
    st.error(f"Erro ao ler banco de dados: {e}")
    all_prices_df = pd.DataFrame()

# 2. FILTRAGEM VISUAL (A CORREÇÃO IMPORTANTE)
# Filtra o DataFrame global para mostrar APENAS o que o usuário selecionou na sidebar agora.
if not all_prices_df.empty and final_items_list:
    # Filtra por Itens Selecionados
    all_prices_df = all_prices_df[all_prices_df['item_id'].isin(final_items_list)]
    
    # Filtra por Cidades Selecionadas (apenas para garantir que não apareçam cidades indesejadas na tabela bruta)
    if city_input:
        all_prices_df = all_prices_df[all_prices_df['city'].isin(city_input)]

# 3. Processamento
if all_prices_df.empty:
    if not final_items_list:
        st.info("👈 Use a barra lateral para selecionar uma categoria (ex: Minério).")
    else:
        st.warning("Nenhum dado encontrado para os filtros atuais. Clique em 'Atualizar Dados' para buscar na API.")
else:
    # Calcula Arbitragem (apenas com os dados filtrados)
    opportunities_df = arbitrage.find_arbitrage(
        all_prices_df,
        fee_pct=fee_pct,
        transport_cost=transport_cost,
        top_n=300,
        method=method_code 
    )

    if opportunities_df.empty:
        st.info("Sem oportunidades de lucro para os itens selecionados.")
    else:
        # Filtro de ROI
        filtered_opps = opportunities_df[opportunities_df['profit_pct'] >= min_profit_pct].copy()
        
        # Ordenação
        filtered_opps = filtered_opps.sort_values(by='net_profit', ascending=False)
        final_view = filtered_opps.head(max_results)

        if final_view.empty:
            st.warning(f"Existem itens, mas nenhum atinge o ROI mínimo de {min_profit_pct}%. Tente baixar a margem.")
        else:
            # Volume de vendas por cidade de destino
            try:
                unique_destinations = final_view['sell_city'].unique()
                volume_map = {}
                for city in unique_destinations:
                    items_for_city = final_view[final_view['sell_city'] == city]['item_id_quality'].apply(
                        lambda x: x.split('_Q')[0]
                    ).unique().tolist()
                    vols = fetch_prices.fetch_sales_history(items_for_city, city)
                    volume_map.update(vols)

                final_view['Volume/Dia'] = final_view.apply(
                    lambda row: volume_map.get(row['item_id_quality'].split('_Q')[0], 0), axis=1
                )
                final_view['Liq.'] = final_view['Volume/Dia'].apply(
                    lambda x: "🟢" if x > 50 else ("🟡" if x > 10 else "🔴")
                )
            except Exception as e:
                print(f"AVISO volume: {e}")
                final_view['Volume/Dia'] = "N/A"
                final_view['Liq.'] = "⚪"

            # Métricas
            c1, c2, c3 = st.columns(3)
            c1.metric("Melhor Lucro", f"{final_view.iloc[0]['net_profit']:,.0f}")
            c2.metric("Melhor ROI", f"{final_view.iloc[0]['profit_pct']:.1f}%")
            c3.metric("Oportunidades", len(final_view))

            # Tabela Final
            display_df = final_view.copy()
            display_df['Lucro'] = display_df['net_profit'].map('{:,.0f}'.format)
            display_df['ROI'] = display_df['profit_pct'].map('{:.1f}%'.format)
            display_df['Compra'] = display_df['buy_price'].map('{:,.0f}'.format)
            display_df['Venda'] = display_df['sell_price'].map('{:,.0f}'.format)
            display_df['Item'] = display_df['item_id_quality'].apply(lambda x: format_item_name_pt(x.split('_Q')[0]))
            display_df['Conf.'] = display_df['confidence_score'].map('{:.0%}'.format)

            # Colunas dinâmicas (dependendo se volume existe ou não)
            cols = ['Item', 'buy_city', 'sell_city', 'Compra', 'Venda', 'Lucro', 'ROI', 'Conf.']
            if 'Volume/Dia' in display_df.columns:
                cols.extend(['Volume/Dia', 'Liq.'])
            
            st.dataframe(
                display_df[cols].rename(columns={'buy_city': 'Origem', 'sell_city': 'Destino'}),
                use_container_width=True,
                hide_index=True
            )