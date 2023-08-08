import streamlit as st
import plotly.express as px
import pymysql


@st.cache_data
def fetch_data(table:str):
    
    pymysql.install_as_MySQLdb()
    conn = st.experimental_connection('mysql',type='sql')
    data = conn.query(f"SELECT * FROM {table}")

    return data


def ocorrencias(tab:st.tabs, plot_config:dict):
    
    aeronave_df = fetch_data('dim_aeronave')
    ocorrencia_df = fetch_data('fat_ocorrencia')
    ocorrencia_tipo_df = fetch_data('dim_ocorrencia_tipo')
    fato_contribuinte_df = fetch_data('dim_fator_contribuinte')
    ocorrencia_df['ano'] = ocorrencia_df['ocorrencia_dia'].dt.year

    with st.sidebar:
        st.header('Filtros')
        dateFilterStart, dateFilterEnd = st.date_input('Dia Ocorrencia', value=[ocorrencia_df['ocorrencia_dia'].min(),ocorrencia_df['ocorrencia_dia'].max()], disabled=False )
        uf = st.multiselect('UF', ocorrencia_df['ocorrencia_uf'].sort_values().unique(), default=ocorrencia_df['ocorrencia_uf'].sort_values().unique())
        classificacao = st.multiselect('Classificacao', ocorrencia_df['ocorrencia_classificacao'].sort_values().unique(), default=ocorrencia_df['ocorrencia_classificacao'].sort_values().unique())
        aeronave_tipo = st.multiselect('Tipo Aeronave', aeronave_df['aeronave_tipo_veiculo'].sort_values().unique(), default=aeronave_df['aeronave_tipo_veiculo'].sort_values().unique())
        aeronave_segmento = st.multiselect('Segmento Aeronave', aeronave_df['aeronave_registro_segmento'].sort_values().unique(), default=aeronave_df['aeronave_registro_segmento'].sort_values().unique())

    
    ocorrencia_df_select = ocorrencia_df[((ocorrencia_df['ocorrencia_dia'].dt.date >= dateFilterStart) & (ocorrencia_df['ocorrencia_dia'].dt.date <= dateFilterEnd)) &
                                (ocorrencia_df['ocorrencia_uf'].isin(uf)) &
                                (ocorrencia_df['ocorrencia_classificacao'].isin(classificacao) )
                                ]
    
    aeronave_df_select = aeronave_df[(aeronave_df['aeronave_tipo_veiculo'].isin(aeronave_tipo)) & (aeronave_df['aeronave_registro_segmento'].isin(aeronave_segmento))]


    merge_ocorrencia_df = ocorrencia_tipo_df.merge(ocorrencia_df_select, left_on='codigo_ocorrencia', right_on='codigo_ocorrencia_tipo')
    merge_aeronave_df = aeronave_df_select.merge(ocorrencia_df_select, left_on='codigo_ocorrencia', right_on='codigo_ocorrencia_aeronave')
    merge_fator_df = ocorrencia_df_select.merge(fato_contribuinte_df, left_on='codigo_ocorrencia', right_on='codigo_ocorrencia')

    with tab:

        metric_col1, metric_col2, metric_col3 = st.columns(3)

        with metric_col1:
            st.metric( 'Com Saída de Pista', len(ocorrencia_df[ocorrencia_df['ocorrencia_saida_pista'] == 'Sim']))
        with metric_col2:
            st.metric( 'Recomendações Emitidas', ocorrencia_df['total_recomendacoes'].sum() )
        with metric_col3:
            st.metric( 'Relatórios Publicados', len(ocorrencia_df[ocorrencia_df['divulgacao_relatorio_publicado'] == 'Sim']))

        st.divider()
        ocorre_col1, ocorre_col2, ocorre_col3 = st.columns(3)

        data_ocorre_ano = ocorrencia_df.groupby(['ocorrencia_classificacao', 'ano']).size().reset_index(name='total')
        grouped_df = merge_aeronave_df.groupby(['aeronave_registro_categoria', 'ocorrencia_classificacao']).size().reset_index(name='count')
        
        with ocorre_col1:
            st.subheader('Acidentes')
            st.bar_chart(data_ocorre_ano[data_ocorre_ano['ocorrencia_classificacao'] == 'Acidente'], x='ano', y='total')
            
            data = grouped_df[grouped_df['ocorrencia_classificacao'] == 'Acidente'][['aeronave_registro_categoria','count']].rename({"aeronave_registro_categoria":"Categoria"},axis=1)
            st.dataframe(data, hide_index=True, use_container_width=True, column_config={
                "count": st.column_config.ProgressColumn(
                "Acidentes",
                format="%f",
                min_value=0,
                max_value=10000,
                )
            } ) 

        with ocorre_col2:
            st.subheader('Incidentes')
            st.bar_chart(data_ocorre_ano[data_ocorre_ano['ocorrencia_classificacao'] == 'Incidente'], x='ano', y='total')

            data = grouped_df[grouped_df['ocorrencia_classificacao'] == 'Incidente'][['aeronave_registro_categoria','count']].rename({"aeronave_registro_categoria":"Categoria"},axis=1)
            st.dataframe( data , hide_index=True, use_container_width=True, column_config={
                "count": st.column_config.ProgressColumn(
                "Incidentes",
                format="%f",
                min_value=0,
                max_value=10000,
                )
            } )

        with ocorre_col3:
            st.subheader('Incidentes Graves')
            st.bar_chart(data_ocorre_ano[data_ocorre_ano['ocorrencia_classificacao'] == 'Incidente Grave'], x='ano', y='total')

            data = grouped_df[grouped_df['ocorrencia_classificacao'] == 'Incidente Grave'][['aeronave_registro_categoria','count']].rename({"aeronave_registro_categoria":"Categoria"},axis=1)
            st.dataframe(data, hide_index=True, use_container_width=True, column_config={
                "count": st.column_config.ProgressColumn(
                "Incidentes Graves",
                format="%f",
                min_value=0,
                max_value=10000,
                )
            } ) 

        st.divider()
        categorie_col1, categorie_col2 = st.columns(2)

        with categorie_col1:
            st.subheader('Categoria')
            data = aeronave_df.groupby(['aeronave_registro_categoria']).size().reset_index(name='total')
            fig = px.bar(data, y='aeronave_registro_categoria', x='total', text_auto=True, orientation='h')
            fig.update_layout( showlegend=False ,yaxis_title=None ,xaxis_title=None  )
            st.plotly_chart(fig, theme='streamlit',use_container_width=True, config=plot_config)
        with categorie_col2:
            st.subheader('Segmento')
            data_seg = aeronave_df.groupby(['aeronave_registro_segmento']).size().reset_index(name='total')
            fig = px.bar(data_seg, y='aeronave_registro_segmento', x='total',text_auto=True, orientation='h')
            fig.update_layout( showlegend=False ,yaxis_title=None ,xaxis_title=None  )
            st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=plot_config)

        st.divider()
        geo_col1, geo_col2 = st.columns(2)

        with geo_col1:
            st.subheader('Estado')
            data_ocorre_uf = merge_ocorrencia_df.groupby(['ocorrencia_uf','ocorrencia_classificacao']).size().reset_index(name='total')
            fig=px.bar(data_ocorre_uf, y='ocorrencia_uf', x='total', color='ocorrencia_classificacao', text_auto=True, orientation='h')
            fig.update_layout( showlegend=False ,yaxis_title=None ,xaxis_title=None  )
            st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=plot_config)

        with geo_col2:
            st.subheader('Aerodromo')
            data_ocorre_aerodromo = merge_ocorrencia_df.groupby(['ocorrencia_aerodromo','ocorrencia_classificacao']).size().reset_index(name='total')
            fig  = px.bar(data_ocorre_aerodromo, y='ocorrencia_aerodromo', x='total', color='ocorrencia_classificacao', text_auto=True, orientation='h')
            fig.update_layout( showlegend=False ,yaxis_title=None ,xaxis_title=None  )
            st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=plot_config)
        
        st.divider()
        types_col1, types_col2 = st.columns(2)

        with types_col1:
            st.subheader('Tipo Ocorrência')
            data = merge_ocorrencia_df.groupby(['ocorrencia_tipo','ocorrencia_classificacao']).size().reset_index(name='total')
            fig = px.bar(data, y='ocorrencia_tipo', x='total', color='ocorrencia_classificacao', text_auto=True, orientation='h')
            fig.update_layout( showlegend=True ,yaxis_title=None ,xaxis_title=None  )
            st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=plot_config)

        with types_col2:
            st.subheader('Fabricante')
            data_model_fab = merge_aeronave_df.groupby(['aeronave_fabricante', 'ocorrencia_classificacao']).size().reset_index(name='total')
            fig = px.bar(data_model_fab, y='aeronave_fabricante', x='total', color='ocorrencia_classificacao', text_auto=True, orientation='h')
            fig.update_layout( showlegend=True ,yaxis_title=None ,xaxis_title=None )
            st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=plot_config)

        st.divider()
        fator_col1 , fator_col2 = st.columns(2)

        with fator_col1:

            st.subheader('Área de Investigação')
            data_model_fator = merge_fator_df.groupby(['fator_area', 'ocorrencia_classificacao']).size().reset_index(name='total')
            fig = px.pie(data_model_fator, values='total', names='fator_area')
            fig.update_layout( showlegend=True ,yaxis_title=None ,xaxis_title=None )
            st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=plot_config)

        with fator_col2:
            st.subheader('Fatores Contribuintes')
            data_model_fator = merge_fator_df.groupby(['fator_nome', 'ocorrencia_classificacao']).size().reset_index(name='total')
            fig = px.bar(data_model_fator, y='fator_nome', x='total', color='ocorrencia_classificacao', text_auto=True, orientation='h')
            fig.update_layout( showlegend=True ,yaxis_title=None ,xaxis_title=None )
            st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=plot_config)


def recomendacoes(tab:st.tabs):

    with tab:
        st.header('Recomentações')
        ocorrencia_df = fetch_data('fat_ocorrencia')
        aeronave_df = fetch_data('dim_aeronave').drop(['criado_em'	,'criado_por' ,'atualizado_em', 'atualizado_por' ], axis=1)
        recomendacao_df = fetch_data('dim_recomendacao').drop(['criado_em'	,'criado_por' ,'atualizado_em', 'atualizado_por' ], axis=1)
        
        merge_df_recomendacao = ocorrencia_df.merge(recomendacao_df, left_on='codigo_ocorrencia_recomendacao', right_on='codigo_ocorrencia')
        merge_df = merge_df_recomendacao.merge(aeronave_df, left_on='codigo_ocorrencia_aeronave', right_on='codigo_ocorrencia')

        merge_df = merge_df[['ocorrencia_dia','aeronave_matricula', 'ocorrencia_classificacao','recomendacao_numero', 'recomendacao_destinatario', 'recomendacao_conteudo']]
        merge_df = merge_df.rename({'ocorrencia_dia':'Data Ocorrência','aeronave_matricula': 'Aeronave', 'ocorrencia_classificacao': 'Classificação','recomendacao_numero': "N Recomendação", 'recomendacao_destinatario':'Destinatario', 'recomendacao_conteudo':'Conteúdo Recomendação'}, axis=1) 
        st.dataframe(merge_df, hide_index=True)

def panorama(tab:st.tabs, plot_config:dict):

    ocorrencia_df = fetch_data('fat_ocorrencia')
    ocorrencia_tipo_df = fetch_data('dim_ocorrencia_tipo')
    ocorrencia_df['ano'] = ocorrencia_df['ocorrencia_dia'].dt.year
    merge_ocorrencia_df = ocorrencia_tipo_df.merge(ocorrencia_df, left_on='codigo_ocorrencia', right_on='codigo_ocorrencia_tipo')

    with tab:   

        st.header('Acidentes nos últimos 10 anos')
        data_model_fator = merge_ocorrencia_df.groupby(['ano', 'ocorrencia_classificacao']).size().reset_index(name='total')
        fig = px.bar(data_model_fator, x='ano', y='total', color='ocorrencia_classificacao', text_auto=True, barmode='group')
        fig.update_layout( showlegend=True ,yaxis_title=None ,xaxis_title=None )
        st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=plot_config)


def main():
    ## Config Web Page
    st.set_page_config(
        page_title="CENIPA Dashboard",
        page_icon=":airplane:",
        layout="wide",
        # menu_items={
        #     'About': """
        #         # It is a sample of real-time data streaming dashboard for cryptocurrency. 
        #         # 💰🤑
        #         # """
        # }
    )
    

    st.markdown(f'<img src="https://www.fab.mil.br/images/sistema/geral/gladio_80px.png"> <h1>Dashboard CENIPA - Ocorrências Aeronáuticas na Aviação Civil Brasileira</h1>',unsafe_allow_html=True)

    config = dict({"displayModeBar":'hover',"scrollZoom":False,"displaylogo":False,"responsive":False,"autosizable":True})
    tab1, tab2, tab3 = st.tabs(['Panorama','Ocorrências','Recomentações',])
    
    panorama(tab1, config)
    ocorrencias(tab2,config )
    recomendacoes(tab3)

if __name__=="__main__":
    main()