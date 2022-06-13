import geopandas as GSPD
import folium
import requests
import streamlit as st
import pandas as pd

"""# Здесь мы будем рисвать карты"""



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
    st.dataframe(delivery_data[:100])






