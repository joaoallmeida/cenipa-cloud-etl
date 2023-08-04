import streamlit as st
import plotly.express as px
import polars as pl
import boto3
import pymysql
from utils import Utils

aws_session = boto3.Session(region_name='us-east-1')

@st.cache_data
def fetch_data(table:str):
    
    pymysql.install_as_MySQLdb()

    url = Utils.get_glue_connection(aws_session, 'rds_mysql')
    conn = st.experimental_connection('rds_conn',type='sql',url=url)
    data = conn.query(f"SELECT * FROM {table}")

    return data

try:
    
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
    config = dict({"displayModeBar":'hover',"scrollZoom":False,"displaylogo":False,"responsive":False,"autosizable":True})

    st.title('Dashboard CENIPA - Ocorrências Aeronáuticas na Aviação Civil Brasileira ')

    ocorrencia_df = fetch_data('fat_ocorrencia')
    aeronave_df = fetch_data('dim_aeronave')
    ocorrencia_tipo_df = fetch_data('dim_ocorrencia_tipo')
    merge_ocorrencia_df = ocorrencia_tipo_df.merge(ocorrencia_df, left_on='codigo_ocorrencia', right_on='codigo_ocorrencia_tipo')
    merge_aeronave_df = aeronave_df.merge(ocorrencia_df, left_on='codigo_ocorrencia', right_on='codigo_ocorrencia_aeronave')

    with st.sidebar:
        st.header('Filtros')
        year = st.selectbox('Ano', range(2010, 2022))
        month = st.selectbox('Mês', range(1, 13))
        localidade = st.selectbox('Localidade', ocorrencia_df['ocorrencia_cidade'].sort_values().unique())
        uf = st.selectbox('UF', ocorrencia_df['ocorrencia_uf'].sort_values().unique())
        classificacao = st.selectbox('Classificacao', ocorrencia_df['ocorrencia_classificacao'].sort_values().unique())
        aeronave_tipo = st.selectbox('Tipo Aeronave', aeronave_df['aeronave_tipo_veiculo'].sort_values().unique())
        aeronave_segmento = st.selectbox('Segmento Aeronave', aeronave_df['aeronave_registro_segmento'].sort_values().unique())


    tab1, tab2, tab3 = st.tabs(['Ocorrências', 'Recomentações', 'Fator Contribuinte'])

    with tab1:

        ocorre_col1, ocorre_col2, ocorre_col3 = st.columns(3)


        ocorrencia_df['ano'] = ocorrencia_df['ocorrencia_dia'].dt.year
        data_ocorre_ano = ocorrencia_df.groupby(['ocorrencia_classificacao', 'ano']).size().reset_index(name='total')
        grouped_df = merge_aeronave_df.groupby(['aeronave_registro_categoria', 'ocorrencia_classificacao']).size().reset_index(name='count')
        
        with ocorre_col1:
            st.subheader('Acidentes')
            st.bar_chart(data_ocorre_ano[data_ocorre_ano['ocorrencia_classificacao'] == 'Acidente'], x='ano', y='total')
            st.dataframe(grouped_df[grouped_df['ocorrencia_classificacao'] == 'Acidente'][['aeronave_registro_categoria','count']], hide_index=True, use_container_width=True )
        with ocorre_col2:
            st.subheader('Incidentes')
            st.bar_chart(data_ocorre_ano[data_ocorre_ano['ocorrencia_classificacao'] == 'Incidente'], x='ano', y='total')
            st.dataframe( grouped_df[grouped_df['ocorrencia_classificacao'] == 'Incidente'][['aeronave_registro_categoria','count']], hide_index=True, use_container_width=True )
        with ocorre_col3:
            st.subheader('Incidentes Grave')
            st.bar_chart(data_ocorre_ano[data_ocorre_ano['ocorrencia_classificacao'] == 'Incidente Grave'], x='ano', y='total')
            st.dataframe(grouped_df[grouped_df['ocorrencia_classificacao'] == 'Incidente Grave'][['aeronave_registro_categoria','count']], hide_index=True, use_container_width=True )

        categorie_col1, categorie_col2 = st.columns(2)

        with categorie_col1:
            st.subheader('Categoria')
            data = aeronave_df.groupby(['aeronave_registro_categoria']).size().reset_index(name='total')
            fig = px.bar(data, y='aeronave_registro_categoria', x='total', text_auto=True, orientation='h')
            fig.update_layout( showlegend=False ,yaxis_title=None ,xaxis_title=None  )
            st.plotly_chart(fig, theme='streamlit',use_container_width=True, config=config)
        with categorie_col2:
            st.subheader('Segmento')
            data_seg = aeronave_df.groupby(['aeronave_registro_segmento']).size().reset_index(name='total')
            fig = px.bar(data_seg, y='aeronave_registro_segmento', x='total',text_auto=True, orientation='h')
            fig.update_layout( showlegend=False ,yaxis_title=None ,xaxis_title=None  )
            st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=config)

        geo_col1, geo_col2 = st.columns(2)

        with geo_col1:
            st.subheader('Estado')
            data_ocorre_uf = merge_ocorrencia_df.groupby(['ocorrencia_uf','ocorrencia_classificacao']).size().reset_index(name='total')
            fig=px.bar(data_ocorre_uf, y='ocorrencia_uf', x='total', color='ocorrencia_classificacao', text_auto=True, orientation='h')
            fig.update_layout( showlegend=False ,yaxis_title=None ,xaxis_title=None  )
            st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=config)

        with geo_col2:
            st.subheader('Aerodromo')
            data_ocorre_aerodromo = merge_ocorrencia_df.groupby(['ocorrencia_aerodromo','ocorrencia_classificacao']).size().reset_index(name='total')
            fig  = px.bar(data_ocorre_aerodromo, y='ocorrencia_aerodromo', x='total', color='ocorrencia_classificacao', text_auto=True, orientation='h')
            fig.update_layout( showlegend=False ,yaxis_title=None ,xaxis_title=None  )
            st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=config)
        

        st.subheader('Tipo Ocorrência')
        data = merge_ocorrencia_df.groupby(['ocorrencia_tipo','ocorrencia_classificacao']).size().reset_index(name='total')
        fig = px.bar(data, y='ocorrencia_tipo', x='total', color='ocorrencia_classificacao', text_auto=True, orientation='h')
        fig.update_layout( showlegend=True ,yaxis_title=None ,xaxis_title=None  )
        st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=config)
 
        st.subheader('Fabricante')
        data_model_fab = merge_aeronave_df.groupby(['aeronave_fabricante', 'ocorrencia_classificacao']).size().reset_index(name='total')
        fig = px.bar(data_model_fab, y='aeronave_fabricante', x='total', color='ocorrencia_classificacao', text_auto=True, orientation='h')
        fig.update_layout( showlegend=True ,yaxis_title=None ,xaxis_title=None )
        st.plotly_chart(fig, theme='streamlit', use_container_width=True, config=config)


    with tab2:
        st.header('Recomentações')
        recomendacao_df = fetch_data('dim_recomendacao')
        st.write(recomendacao_df)

    with tab3:
        st.header('Fator Contribuinte')
        fator_contribuitne_df = fetch_data('dim_fator_contribuinte')
        st.write(fator_contribuitne_df)

        
except Exception as e:
    raise e