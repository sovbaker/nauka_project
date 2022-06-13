import streamlit as st
import psycopg2
import pandas as pd

st.set_page_config(layout="wide")

st.markdown("""
# Загрузка и обработка данных
Здесь мы с помощью базы данных sql получим все что нам нужно для дальнейшей работы
""")
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

    """### Выгрузим из базы данных таблицу УИК и пользователями деливери"""

    vybory_df =get_data('''select * from delivery a
                 left join vybory b using (uik)''')
    st.dataframe(vybory_df.loc[:, 'delivery2_price_client_rub':])

