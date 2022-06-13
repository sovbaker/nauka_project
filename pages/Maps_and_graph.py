import geopandas as GSPD
import folium
import requests
import streamlit as st
import pandas as pd
import psycopg2
from shapely.wkt import loads
from streamlit_folium import folium_static
"""# Здесь мы будем рисовать карту"""


st.set_page_config(page_title='Карты и графы', page_icon='🗺')
with st.echo(code_location='below'):
    @st.experimental_singleton
    def init_connection():
        return psycopg2.connect(**st.secrets["postgres"])


    conn = init_connection()


    # Perform query.
    # Uses st.experimental_memo to only rerun when the query changes or after 10 min.
    @st.experimental_memo(ttl=600)
    def run_query(query):
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()


    @st.experimental_singleton()
    def get_data(query):
        return pd.read_sql(query, conn)
    """Получим данные"""
    conn = init_connection()
    delivery_data = get_data("""select delivery2_user_id as user_id
                                , (case when delivery2_promocode is null then 0 else 1 end) as promo_flg
                                , delivery2_products as products, delivery2_vendor_name as vendor
                                , delivery2_price_client_rub as spent
                                , delivery2_latitude as lat
                                , delivery2_longitude as lon
                                from delivery_full""")

    """С помощью геопанадаса создадим геодатафрейм, а с помощью api osm-boundaries получим районы заказов"""
    geodata = GSPD.GeoDataFrame(delivery_data, geometry=GSPD.points_from_xy(delivery_data['lon'], delivery_data['lat']))

    districts_pd = pd.read_csv('mos_poly.csv')
    geodistricts = GSPD.GeoDataFrame(districts_pd, geometry=districts_pd['geometry'].apply(loads))

    geodata=geodata.sjoin(geodistricts[['geometry','local_name']], how='left')
    st.dataframe(geodata[:10].drop('geometry', axis=1))

    """
    Посмотрим в каких районах из каких ресторанов больше заказывают еду
    """
    col1, col2 = st.columns(2)
    with col1:
        options = st.selectbox('Выберете ресторан:', delivery_data['vendor'].unique())


    drow_products = geodata[geodata['vendor']==options].groupby('local_name', as_index=False)['user_id'].count()
    geojson='mos_districts.geojson'
    ## From (Дз 13)
    map = folium.Map(location=[55.753544, 37.621211], zoom_start=10)
    cho = folium.Choropleth(geo_data=geojson, data=drow_products, columns=['local_name', 'user_id']
                            , key_on='feature.properties.local_name'
                            , fill_color='YlOrRd'
                            , nan_fill_color="White"
                            , legend_name='Количество заказов'
                            ).add_to(map)

    folium_static(map, width=800)


    ##
    """Если вам это кажется знакомым, то вам не кажется, что-то похожее я делал в проекте по визуализации, 
    но там я почти не использовал геопандас, поэтому код отличается """




