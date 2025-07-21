import json
from collections import Counter
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from wordcloud import WordCloud

# Configuração da página
st.set_page_config(
    page_title="Atlas",
    page_icon="🗺️",
    layout="centered",
    initial_sidebar_state="collapsed"
)

st.title("🗺️ Atlas")
#st.caption("Visualização de acontecimentos mundiais de interesse")

# Carregar CSS
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Função para carregar dados
@st.cache_data
def load_data():
    try:
        with open('news_data.json', 'r', encoding='utf-8') as f:
            news_data = json.load(f)
        
        with open('country_emojis.json', 'r', encoding='utf-8') as f:
            country_emojis = json.load(f)
        
        # Converter para DataFrame
        df = pd.DataFrame(news_data)
        df['data'] = pd.to_datetime(df['data'])
        
        return df, country_emojis
    except FileNotFoundError:
        st.error("Arquivos de dados não encontrados. Execute primeiro o gerador de dados.")
        return None, None

# Função para criar tag cloud
def create_wordcloud(text_data):
    if text_data.empty:
        return None
    
    # Contar frequência das tags
    all_tags = []
    for tags in text_data:
        if isinstance(tags, list):
            all_tags.extend(tags)
        elif isinstance(tags, str):
            all_tags.extend(tags.split(', '))
    
    if not all_tags:
        return None
    
    tag_freq = Counter(all_tags)
    
    # Criar wordcloud
    wordcloud = WordCloud(
        width=1600, 
        height=800, 
        background_color='white',
        colormap='viridis',
        max_words=50
    ).generate_from_frequencies(tag_freq)
    
    return wordcloud

# Função para aplicar filtros
def apply_filters(df, start_date, end_date, selected_countries, selected_tags):
    filtered_df = df.copy()
    
    # Filtro de data
    if start_date and end_date:
        filtered_df = filtered_df[
            (filtered_df['data'] >= start_date) & 
            (filtered_df['data'] <= end_date)
        ]
    
    # Filtro de países
    if selected_countries:
        filtered_df = filtered_df[filtered_df['pais'].isin(selected_countries)]
    
    # Filtro de tags
    if selected_tags:
        # Filtrar notícias que contêm pelo menos uma das tags selecionadas
        mask = filtered_df['tags'].apply(lambda x: any(tag in x for tag in selected_tags))
        filtered_df = filtered_df[mask]
    
    return filtered_df

# Carregar dados
df, country_emojis = load_data()

if df is None:
    st.stop()

# Tabs principais
tab1, tab2 = st.tabs(["📋 Timeline", "📊 Dataviz"])

# Aba 1: Timeline
with tab1:
    
    st.subheader("Filtros")
    st.caption("Configure os filtros de data, países e tags para personalizar a visualização dos dados.")
    with st.container(border=True):
        # Filtros específicos da Timeline
        
        # Primeira linha: filtros de data
        col1, col2 = st.columns(2)
        
        with col1:
            # Filtros de data
            min_date = df['data'].min().date()
            max_date = df['data'].max().date()
            
            start_date = st.date_input(
                "Data inicial",
                value=min_date,  # Primeira data do dataset por padrão
                min_value=min_date,
                max_value=max_date,
                format="DD/MM/YYYY"
            )
        
        with col2:
            end_date = st.date_input(
                "Data final",
                value=max_date,  # Última data do dataset por padrão
                min_value=min_date,
                max_value=max_date,
                format="DD/MM/YYYY"
            )
        
        # Segunda linha: filtros de países e tags
        col1, col2 = st.columns(2)
        
        with col1:
            # Filtros de países
            all_countries = sorted(df['pais'].unique())
            selected_countries = st.multiselect(
                "Países",
                options=all_countries,
                default=[]  # Filtros em branco por padrão
            )
        
        with col2:
            # Filtros de tags
            all_tags = []
            for tags in df['tags']:
                all_tags.extend(tags)
            all_tags = sorted(list(set(all_tags)))
            
            selected_tags = st.multiselect(
                "Tags",
                options=all_tags,
                default=[]  # Filtros em branco por padrão
            )
        
        # Aplicar filtros (sempre há datas selecionadas agora)
        # Conversão para Timestamp para evitar erro de comparação
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)
        
        # Aplicar filtros
        filtered_df = apply_filters(df, start_date, end_date, selected_countries, selected_tags)
    
    # Estatísticas
    st.subheader("Métricas")
    st.caption("Resumo dos dados filtrados com estatísticas relevantes.")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Notícias", f"{len(filtered_df)} itens", border=True)
    with col2:
        st.metric("Países", f"{len(filtered_df['pais'].unique())} países", border=True)
    with col3:
        st.metric("Tags", f"{len(set([tag for tags in filtered_df['tags'] for tag in tags]))} tags", border=True)
    with col4:
        st.metric("Período", f"{(end_date.date() - start_date.date()).days + 1} dias", border=True)
    
   
    # Preparar dados para tabela com tags estilizadas
    display_df = filtered_df.copy()
    display_df['pais_emoji'] = display_df['pais'].map(lambda x: f"{country_emojis.get(x, {}).get('emoji', '🏳️')} {x}")
    
    # Criar tags estilizadas como ribbons
    def create_tag_ribbons(tags_list):
        if not tags_list:
            return ""
        ribbons = []
        for tag in tags_list:
            ribbons.append(f'<span style="background-color: #ffebee; color: #d32f2f; padding: 2px 8px; margin: 1px; border-radius: 12px; font-size: 0.8em; display: inline-block; white-space: nowrap;">{tag}</span>')
        return ' '.join(ribbons)
    
    display_df['tags_ribbons'] = display_df['tags'].apply(create_tag_ribbons)
    
    # Ordenar por data (mais recente primeiro)
    display_df = display_df.sort_values('data', ascending=False)
    
    # Exibir tabela com HTML customizado para tags
    st.markdown("""
    <style>
    .tag-container {
        display: flex;
        flex-wrap: wrap;
        gap: 2px;
        max-width: 100%;
    }
    .tag-ribbon {
        background-color: #ffebee;
        color: #d32f2f;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.8em;
        white-space: nowrap;
        display: inline-block;
        margin: 1px;
    }
    .date-header {
        background-color: #f0f2f6 ;
        padding: 6px 6px;
        border-radius: 8px;
        margin: 0px 0 16px 0;
        border-left: 4px solid #FF4B4B;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Criar timeline agrupada por data
    st.subheader("Timeline")
    st.caption("Visualização cronológica das notícias agrupadas por data, com países ordenados alfabeticamente.")
    
    # Agrupar por data (usando a data real, não a string)
    grouped_by_date = display_df.groupby('data')
    
    for date, group in grouped_by_date:
        # Formatar a data para exibição no formato "01 jan. 2025"
        date_str = date.strftime('%d %b. %Y').lower()
        
        # Cabeçalho da data
        st.markdown(f"""
        <div class="date-header">
            <h4>📅 {date_str}</h4>
        </div>
        """, unsafe_allow_html=True)
        
        # Ordenar países alfabeticamente dentro do dia
        group_sorted = group.sort_values('pais')
        
        # Notícias do dia
        with st.container(border=True):
            for idx, (row_idx, row) in enumerate(group_sorted.iterrows()):
                with st.container():
                    col1, col2 = st.columns([1, 4])
                    
                    with col1:
                        st.markdown(f"#### **{row['pais_emoji']}**")
                    
                    with col2:
                        st.markdown(f"{row['texto']}")
                        st.markdown(f"<div class='tag-container'>{row['tags_ribbons']}</div>", unsafe_allow_html=True)
                    
                    # Adicionar separador apenas se não for o último item do dia
                    if idx < len(group_sorted) - 1:
                        st.markdown("---")
                    else:
                        # Espaçamento para o último item
                        st.write("")
                        st.write("")
    
    
    # Botão de exportação
    csv = filtered_df.to_csv(index=False, encoding='utf-8-sig')
    st.download_button(
        label="📥 Exportar dados (.csv)",
        data=csv,
        file_name=f"atlas_diario_{start_date.date()}_{end_date.date()}.csv",
        mime="text/csv",
        type="tertiary"
    )
    
 
# Aba 2: Dataviz
with tab2:
    
    # Estatísticas do dataset completo
    st.subheader("Métricas")
    st.caption("Estatísticas gerais de todo o dataset de notícias.")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Notícias", f"{len(df)} itens", border=True)
    with col2:
        st.metric("Países", f"{len(df['pais'].unique())} países", border=True)
    with col3:
        st.metric("Tags", f"{len(set([tag for tags in df['tags'] for tag in tags]))} tags", border=True)
    with col4:
        st.metric("Período", f"{(df['data'].max().date() - df['data'].min().date()).days + 1} dias", border=True)
    
    # Gráfico de linha - Notícias por dia
    st.subheader("Notícias por dia")
    st.caption("Evolução temporal do número de notícias publicadas por dia, mostrando tendências e picos de atividade.")
    
    with st.container(border=True):
        # Preparar dados para o gráfico (todos os países em uma linha)
        daily_news = df.groupby('data').size().reset_index(name='count')
        
        # Calcular limite do eixo Y (20% maior que o valor máximo)
        max_count = daily_news['count'].max()
        y_max = max_count * 1.2
        
        # Formatar datas para o eixo X (1 jan. 25)
        daily_news['data_formatada'] = daily_news['data'].dt.strftime('%d %b. %y').str.lower()
        
        fig_line = px.line(
            daily_news,
            x='data_formatada',
            y='count',
            title='',
            labels={'data_formatada': '', 'count': ''},
            markers=True
        )
        
        fig_line.update_layout(
            showlegend=False,
            height=300,
            margin=dict(l=0, r=0, t=30, b=0),
            hovermode='x unified',
            plot_bgcolor="white",
            xaxis=dict(
                showgrid=False, 
                showspikes=False,
                title=None,
                tickangle=0,
                nticks=8  # Limitar o número de ticks para espaçamento adequado
            ),
            yaxis=dict(
                showspikes=False,
                showgrid=True,
                gridcolor="lightgray",
                gridwidth=0.5,
                rangemode="tozero",
                range=[0, y_max],
                title=None,
                nticks=6
            ),
        )
        
        # Atualizar a cor da linha para o vermelho do Streamlit e tooltip personalizado
        fig_line.update_traces(
            line=dict(color="#FF4B4B", width=3),
            marker=dict(color="#FF4B4B", size=6),
            hovertemplate="Notícias: %{y}<extra></extra>"
        )
        
        st.plotly_chart(fig_line, use_container_width=True)
    
    # Choropleth map - Distribuição por país
    st.subheader("Mapa de calor")
    st.caption("Mapa geográfico mostrando a distribuição de notícias por país, com intensidade baseada no número de notícias.")
    
    with st.container(border=True):
        # Filtro de data para este gráfico específico
        col1, col2 = st.columns(2)
        with col1:
            start_date_choropleth = st.date_input(
                "Data inicial para o mapa",
                value=df['data'].min().date(),
                min_value=df['data'].min().date(),
                max_value=df['data'].max().date(),
                format="DD/MM/YYYY"
            )
        with col2:
            end_date_choropleth = st.date_input(
                "Data final para o mapa",
                value=df['data'].max().date(),
                min_value=df['data'].min().date(),
                max_value=df['data'].max().date(),
                format="DD/MM/YYYY"
            )
        
        # Aplicar filtro de data para o choropleth
        start_date_choropleth = pd.Timestamp(start_date_choropleth)
        end_date_choropleth = pd.Timestamp(end_date_choropleth)
        
        filtered_df_choropleth = df[
            (df['data'] >= start_date_choropleth) & 
            (df['data'] <= end_date_choropleth)
        ]
        
        country_counts_choropleth = filtered_df_choropleth['pais'].value_counts().reset_index()
        country_counts_choropleth.columns = ['País', 'Notícias']
        
        # Adicionar códigos ISO do novo formato
        country_counts_choropleth['iso_alpha'] = country_counts_choropleth['País'].map(
            lambda x: country_emojis.get(x, {}).get('iso', '')
        )
        
        # Criar choropleth usando go.Figure como no projeto de ransomware
        fig_choropleth = go.Figure(
            go.Choropleth(
                locations=country_counts_choropleth['iso_alpha'],
                z=country_counts_choropleth['Notícias'],
                text="<b>" + country_counts_choropleth['País'] + "</b><br>Notícias: " + country_counts_choropleth['Notícias'].astype(str),
                colorscale="reds",
                marker_line_color="darkgray",
                marker_line_width=1,
                showscale=False,  # Remove a barra de cores
                hovertemplate="%{text}<extra></extra>",
            )
        )
        
        fig_choropleth.update_layout(
            geo=dict(
                showframe=False,
                showcoastlines=True,
                coastlinecolor="darkgray",
                coastlinewidth=1.25,
                projection_type="equirectangular",
            ),
            margin=dict(t=0, b=0, l=0, r=0),
            height=400,
        )
        
        st.plotly_chart(fig_choropleth, use_container_width=True)
    
    # Distribuição por tags
    st.subheader("Distribuição por tags")
    st.caption("Análise das tags mais frequentes no período selecionado, com visualização em nuvem de palavras e tabela de frequências.")
    
    with st.container(border=True):
        # Filtro de data para este gráfico específico
        col1, col2 = st.columns(2)
        with col1:
            start_date_tags = st.date_input(
                "Data inicial para tags",
                value=df['data'].min().date(),
                min_value=df['data'].min().date(),
                max_value=df['data'].max().date(),
                format="DD/MM/YYYY"
            )
        with col2:
            end_date_tags = st.date_input(
                "Data final para tags",
                value=df['data'].max().date(),
                min_value=df['data'].min().date(),
                max_value=df['data'].max().date(),
                format="DD/MM/YYYY"
            )
        
        # Aplicar filtro de data para tags
        start_date_tags = pd.Timestamp(start_date_tags)
        end_date_tags = pd.Timestamp(end_date_tags)
        
        filtered_df_tags = df[
            (df['data'] >= start_date_tags) & 
            (df['data'] <= end_date_tags)
        ]
        
        # Duas colunas: wordcloud e tabela
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Tag cloud**")
            wordcloud_viz = create_wordcloud(filtered_df_tags['tags'])
            
            if wordcloud_viz:
                fig2, ax2 = plt.subplots(figsize=(8, 6))
                ax2.imshow(wordcloud_viz, interpolation='bilinear')
                ax2.axis('off')
                st.pyplot(fig2)
            else:
                st.info("Não há dados suficientes para gerar a tag cloud.")
        
        with col2:
            st.write("**Tabela de frequências**")
            
            # Contar frequência das tags
            all_tags_filtered = []
            for tags in filtered_df_tags['tags']:
                all_tags_filtered.extend(tags)
            
            tag_counts = pd.Series(all_tags_filtered).value_counts().reset_index()
            tag_counts.columns = ['Tag', 'Frequência']
            
            # Exibir tabela
            st.dataframe(
                tag_counts,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Tag": st.column_config.TextColumn("Tag", width="medium"),
                    "Frequência": st.column_config.NumberColumn("Frequência", width="small")
                },
                height=300
            )
        
        # Nova visualização: Evolução temporal das tags
        # st.write("")
        
        # Filtro múltiplo de tags para esta visualização
        all_tags_filtered = []
        for tags in filtered_df_tags['tags']:
            all_tags_filtered.extend(tags)
        
        unique_tags = sorted(list(set(all_tags_filtered)))
        selected_tags_timeline = st.multiselect(
            "Selecione as tags para visualizar",
            options=unique_tags,
            default=unique_tags[:5] if len(unique_tags) >= 5 else unique_tags,  # Primeiras 5 por padrão
            help="Escolha até 5 tags para melhor visualização"
        )
        
        st.write("**Evolução temporal**")
        if selected_tags_timeline:
            # Criar dataframe completo com todas as datas do período
            all_dates = pd.date_range(
                start=filtered_df_tags['data'].min(),
                end=filtered_df_tags['data'].max(),
                freq='D'
            )
            
            # Criar dataframe base com todas as datas
            timeline_df = pd.DataFrame({'data': all_dates})
            
            # Para cada tag selecionada, contar frequência por data
            for tag in selected_tags_timeline:
                # Filtrar notícias que contêm a tag específica
                tag_news = filtered_df_tags[filtered_df_tags['tags'].apply(lambda x: tag in x)]
                
                # Contar notícias por dia para esta tag
                daily_tag_count = tag_news.groupby('data').size().reset_index(name=tag)
                
                # Fazer merge com o dataframe base para incluir todos os dias
                timeline_df = timeline_df.merge(daily_tag_count, on='data', how='left')
                
                # Preencher valores NaN com 0
                timeline_df[tag] = timeline_df[tag].fillna(0).astype(int)
            
            # Formatar datas para o eixo X
            timeline_df['data_formatada'] = timeline_df['data'].dt.strftime('%d %b. %y').str.lower()
            
            # Calcular limite do eixo Y (20% maior que o valor máximo)
            max_count = timeline_df[selected_tags_timeline].max().max()
            y_max = max_count * 1.2 if max_count > 0 else 5
            
            # Criar gráfico de linha
            fig_tag_timeline = go.Figure()
            
            # Cores para as tags
            colors = ['#FF4B4B', '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
            
            for i, tag in enumerate(selected_tags_timeline):
                color = colors[i % len(colors)]
                
                fig_tag_timeline.add_trace(
                    go.Scatter(
                        x=timeline_df['data_formatada'],
                        y=timeline_df[tag],
                        name=tag,
                        mode='lines+markers',
                        line=dict(color=color, width=2),
                        marker=dict(color=color, size=5),
                        hovertemplate=f"{tag}: %{{y}}<extra></extra>"
                    )
                )
            
            fig_tag_timeline.update_layout(
                showlegend=True,
                height=300,
                margin=dict(l=0, r=0, t=30, b=0),
                hovermode='x unified',
                plot_bgcolor="white",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="left",
                    x=0
                ),
                xaxis=dict(
                    showgrid=False, 
                    showspikes=False,
                    title=None,
                    tickangle=0,
                    nticks=8
                ),
                yaxis=dict(
                    showspikes=False,
                    showgrid=True,
                    gridcolor="lightgray",
                    gridwidth=0.5,
                    rangemode="tozero",
                    range=[0, y_max],
                    title=None,
                    nticks=6
                ),
            )
            
            st.plotly_chart(fig_tag_timeline, use_container_width=True)
        else:
            st.info("Selecione pelo menos uma tag para visualizar a evolução temporal.")

# Footer
# st.write("---")
with st.container(border=True):
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>🗺️ <strong>Atlas </strong>. Desenvolvido com ❤️ por ████████</p>
        </div>
        """,
        unsafe_allow_html=True
    ) 