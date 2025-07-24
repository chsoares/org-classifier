#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Streamlit App - COP29 Insurance Classification Dashboard
"""

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path

# Configuração da página
st.set_page_config(
    page_title="org classifier",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.title("org classifier")

# Função para carregar dados
def load_data():
    """Carrega os dados dos CSVs"""
    try:
        orgs_path = Path("data/results/organizations.csv")
        people_path = Path("data/results/people.csv")
        
        if not orgs_path.exists() or not people_path.exists():
            st.error("❌ Arquivos de dados não encontrados. Execute primeiro o processamento completo.")
            return None, None
        
        orgs_df = pd.read_csv(orgs_path)
        people_df = pd.read_csv(people_path)
        
        return orgs_df, people_df
    
    except Exception as e:
        st.error(f"❌ Erro ao carregar dados: {str(e)}")
        return None, None

# Função para salvar correções
def save_correction(orgs_df, people_df, org_name, new_classification):
    """Salva correção manual nos arquivos"""
    try:
        # Atualizar organizations.csv
        orgs_df.loc[orgs_df['organization_name'] == org_name, 'is_insurance'] = new_classification
        orgs_df.loc[orgs_df['organization_name'] == org_name, 'processing_status'] = 'manual_correction'
        orgs_df.loc[orgs_df['organization_name'] == org_name, 'processed_at'] = datetime.now().isoformat()
        
        # Atualizar people.csv - usar a coluna correta baseada na estrutura dos dados
        if 'Home organization_normalized' in people_df.columns:
            people_df.loc[people_df['Home organization_normalized'] == org_name, 'is_insurance'] = new_classification
        elif 'Home organization' in people_df.columns:
            people_df.loc[people_df['Home organization'] == org_name, 'is_insurance'] = new_classification
        
        # Salvar arquivos
        orgs_df.to_csv("data/results/organizations.csv", index=False)
        people_df.to_csv("data/results/people.csv", index=False)
        
        return True
    except Exception as e:
        st.error(f"❌ Erro ao salvar correção: {str(e)}")
        return False

# Função para criar sunburst
def create_sunburst(orgs_df):
    """Cria gráfico sunburst com 3 níveis: Total > Processadas > Classificadas"""
    total_orgs = len(orgs_df)
    processed_orgs = len(orgs_df[orgs_df['processing_status'].isin(['completed', 'manual_correction'])])
    insurance_orgs = len(orgs_df[orgs_df['is_insurance'] == True])
    non_processed = total_orgs - processed_orgs
    non_insurance = processed_orgs - insurance_orgs
    
    fig = go.Figure(go.Sunburst(
        labels=['Total', 'Processadas', 'Não Processadas', 'Seguradoras', 'Não-Seguradoras'],
        parents=['', 'Total', 'Total', 'Processadas', 'Processadas'],
        values=[total_orgs, processed_orgs, non_processed, insurance_orgs, non_insurance],
        branchvalues="total",
        hovertemplate='<b>%{label}</b><br>Quantidade: %{value}<br>Percentual: %{percentParent}<extra></extra>',
        maxdepth=3,
    ))
    
    fig.update_layout(
        font_size=12,
        height=400,
        margin=dict(t=0, b=0, l=0, r=0)
    )
    
    return fig

# Carregar dados
orgs_df, people_df = load_data()

if orgs_df is not None and people_df is not None:
    
    # Tabs principais
    tab1, tab2 = st.tabs(["📊 Dashboard", "🗂️ Dataset"])
    
    # ==========================================
    # ABA 1: DASHBOARD
    # ==========================================
    with tab1:
        
        # Seção 1: Métricas
        st.subheader("Métricas")
        st.caption("Estatísticas gerais do processamento de organizações e pessoas.")
        
        # Calcular métricas
        total_orgs = len(orgs_df)
        processed_orgs = len(orgs_df[orgs_df['processing_status'].isin(['completed', 'manual_correction'])])
        insurance_orgs = len(orgs_df[orgs_df['is_insurance'] == True])
        
        total_people = len(people_df)
        classified_people = len(people_df[people_df['is_insurance'].notna()])
        insurance_people = len(people_df[people_df['is_insurance'] == True])
        
        # Primeira linha: Organizações
        st.write("**Organizações**")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total", f"{total_orgs:,}", border=True)
        with col2:
            st.metric("Processadas", f"{processed_orgs:,}", border=True)
        with col3:
            st.metric("Classificadas", f"{insurance_orgs:,}", border=True)
        
        # Segunda linha: Pessoas
        st.write("**Pessoas**")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total", f"{total_people:,}", border=True)
        with col2:
            st.metric("Processadas", f"{classified_people:,}", border=True)
        with col3:
            st.metric("Classificadas", f"{insurance_people:,}", border=True)
        
        # Seção 2: Lista de seguradoras + Sunburst
        st.subheader("Organizações")
        st.caption("Lista das organizações classificadas como seguradoras pelo programa")
        
        insurance_orgs_list = orgs_df[orgs_df['is_insurance'] == True]['organization_name'].sort_values()
        
        if len(insurance_orgs_list) > 0:
            # Criar DataFrame para exibição
            insurance_display = pd.DataFrame({
                'Organização': insurance_orgs_list.values
            })
            
            st.dataframe(
                insurance_display,
                use_container_width=True,
                height=500,
                hide_index=True
            )
            
            st.caption(f"Total: {len(insurance_orgs_list)} seguradoras identificadas")
        else:
            st.info("Nenhuma seguradora identificada ainda.")
        
        # Seção 3: Tabela de pessoas de seguradoras
        st.subheader("Pessoas de Seguradoras")
        st.caption("Participantes da COP29 que trabalham em organizações seguradoras.")
    
        insurance_people_df = people_df[people_df['is_insurance'] == True].copy()
        
        if len(insurance_people_df) > 0:
            # Remover colunas desnecessárias e renomear
            display_columns = [col for col in insurance_people_df.columns 
                             if col not in ['is_insurance', 'Home organization']]
            insurance_people_display = insurance_people_df[display_columns]
            
            # Renomear coluna home_organization_normalized para "Organização"
            if 'Home organization_normalized' in insurance_people_display.columns:
                insurance_people_display = insurance_people_display.rename(
                    columns={'Home organization_normalized': 'Home organization'}
                )
            
            st.dataframe(
                insurance_people_display,
                use_container_width=True,
                height=500
            )
            
            st.caption(f"Total: {len(insurance_people_df)} pessoas de seguradoras")
        else:
            st.info("Nenhuma pessoa de seguradora identificada ainda.")
    

    
    # ==========================================
    # ABA 2: DATASET
    # ==========================================
    with tab2:
        
        # Seção 1: Organizations.csv
        st.subheader("Organizações")
        st.caption("Visualização completa do arquivo organizations.csv com filtros.")
        
        # Filtros para organizações
        col1, col2 = st.columns(2)
        
        with col1:
            org_search = st.text_input("Buscar organização:")
        
        with col2:
            classification_filter = st.selectbox(
                "Filtrar por classificação:",
                ["Todos", "Seguradoras", "Não-Seguradoras", "Não Classificadas"]
            )
        
        # Aplicar filtros
        filtered_orgs = orgs_df.copy()
        
        if org_search:
            filtered_orgs = filtered_orgs[
                filtered_orgs['organization_name'].str.contains(org_search, case=False, na=False)
            ]
        
        if classification_filter == "Seguradoras":
            filtered_orgs = filtered_orgs[filtered_orgs['is_insurance'] == True]
        elif classification_filter == "Não-Seguradoras":
            filtered_orgs = filtered_orgs[filtered_orgs['is_insurance'] == False]
        elif classification_filter == "Não Classificadas":
            filtered_orgs = filtered_orgs[filtered_orgs['is_insurance'].isna()]
        
        st.dataframe(
            filtered_orgs,
            use_container_width=True,
            height=400
        )
        
        st.caption(f"Mostrando {len(filtered_orgs)} de {len(orgs_df)} organizações")
        
        # Seção 2: People.csv
        st.subheader("Pessoas")
        st.caption("Visualização completa do arquivo people.csv com filtros.")
        
        # Primeira linha: busca
        people_search = st.text_input("Buscar por nome ou organização:")
        
        # Segunda linha: filtros
        col1, col2, col3 = st.columns(3)
        
        with col1:
            type_filter = st.selectbox(
                "Filtrar por Type:",
                ["Todos"] + sorted(people_df['Type'].unique()) if 'Type' in people_df.columns else ["Todos"]
            )
        
        with col2:
            nominated_filter = st.selectbox(
                "Filtrar por Nominated by:",
                ["Todos"] + sorted(people_df['Nominated by'].unique()) if 'Nominated by' in people_df.columns else ["Todos"]
            )
        
        with col3:
            insurance_people_filter = st.selectbox(
                "Filtrar por is_insurance:",
                ["Todos", "Seguradoras", "Não-Seguradoras", "Não Classificadas"]
            )
        
        # Aplicar filtros
        filtered_people = people_df.copy()
        
        if people_search:
            search_columns = ['Name', 'Home organization_normalized']
            mask = False
            for col in search_columns:
                if col in filtered_people.columns:
                    mask |= filtered_people[col].astype(str).str.contains(people_search, case=False, na=False)
            filtered_people = filtered_people[mask]
        
        if type_filter != "Todos" and 'Type' in people_df.columns:
            filtered_people = filtered_people[filtered_people['Type'] == type_filter]
        
        if nominated_filter != "Todos" and 'Nominated by' in people_df.columns:
            filtered_people = filtered_people[filtered_people['Nominated by'] == nominated_filter]
        
        if insurance_people_filter == "Seguradoras":
            filtered_people = filtered_people[filtered_people['is_insurance'] == True]
        elif insurance_people_filter == "Não-Seguradoras":
            filtered_people = filtered_people[filtered_people['is_insurance'] == False]
        elif insurance_people_filter == "Não Classificadas":
            filtered_people = filtered_people[filtered_people['is_insurance'].isna()]
        
        # Remover coluna Home organization e renomear home_organization_normalized
        display_people = filtered_people.copy()
        if 'Home organization' in display_people.columns:
            display_people = display_people.drop(columns=['Home organization'])
        
        if 'Home organization_normalized' in display_people.columns:
            display_people = display_people.rename(
                columns={'Home organization_normalized': 'Home organization'}
            )
        
        st.dataframe(
            display_people,
            use_container_width=True,
            height=400
        )
        
        st.caption(f"Mostrando {len(filtered_people)} de {len(people_df)} pessoas")

        # Seção 4: Correção Manual
        st.subheader("Correção manual")
        st.caption("Corrigir classificações incorretas manualmente.")
    
        # Dropdown com todas as organizações
        org_options = sorted(orgs_df['organization_name'].dropna().unique())
        selected_org = st.selectbox(
            "Selecionar Organização:",
            [""] + org_options,
            help="Escolha a organização para corrigir"
        )
        if selected_org:
            # Mostrar classificação atual
            current_classification = orgs_df[orgs_df['organization_name'] == selected_org]['is_insurance'].iloc[0]
            if pd.isna(current_classification):
                current_text = "Não classificada"
            elif current_classification:
                current_text = "Seguradora"
            else:
                current_text = "Não-seguradora"
            
            st.info(f"**Atual:** {current_text}")
    
        if selected_org:
            # Dropdown para nova classificação
            new_classification = st.selectbox(
                "Nova Classificação:",
                ["", "Seguradora", "Não-seguradora"],
                help="Escolha a nova classificação"
            )
    
            # Botão para salvar
            if selected_org and new_classification:
                if st.button("💾 Salvar Correção", type="primary"):
                    # Converter para boolean
                    new_value = True if new_classification == "Seguradora" else False
                    
                    # Salvar correção
                    if save_correction(orgs_df, people_df, selected_org, new_value):
                        st.rerun()
                    else:
                        st.error("❌ Erro ao salvar correção")

else:
    st.error("❌ Não foi possível carregar os dados. Verifique se os arquivos existem em data/results/")
    st.info("💡 Execute primeiro o processamento completo com: `python run_full_dataset.py`")