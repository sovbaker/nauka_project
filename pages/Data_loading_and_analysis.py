import streamlit as st
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import statsmodels.api as sm
import statsmodels.formula.api as smf
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

    vybory_df =get_data('''select uik, avg (delivery2_price_client_rub) as avg_spend
                            , avg(umg_flg::numeric) as umg_flg 
                            from(select * from delivery a
                                left join vybory b using (uik)) as a
                                group by 1''')
    """
    Так выглядит датафрейм для нашей первой модели 
    """
    vybory_df['umg_flg']=vybory_df['umg_flg'].fillna(0)
    st.dataframe(vybory_df)
    """
    В нашей модели зависимой переменной будет факт того, что люди проголосовали за кандидата умного голосования,
    а фичей средние траты тех, кто должен был голосовать в этом УИКе
    """
    figure=go.Figure()
    figure.add_trace(go.Scatter(x=vybory_df['avg_spend'], y=vybory_df['umg_flg'], mode='markers', opacity=0.5))
    figure.update_layout(title="Траты людей на еду и победа кандидата от УМГ", xaxis={'title':'Средние траты'}
                         , yaxis={'title':'Победа кандидата УМГ'})
    st.plotly_chart(figure)

    '''Построим простую логистическую регресию по этим данным'''
    model=smf.logit('umg_flg~avg_spend', data=vybory_df).fit()

    st.write(model.summary())

    """Как можно видеть наша модель показывыает хоть и положительный коэффициент при средних тратах
    , но очень маленький, практически нулевой - это означает, что мы не учитываем какие-то факторы
    , поэтому мы спустимся на уровень ниже посмотрим 
    на конкретных пользователей и добавим информации о них: из каких ресторанов человек чаще заказывал
    еду, марку его автомобиля, год его рождения, год выпуска автомобиля
    ,"""
    vybory_df_2=get_data('''select * from delivery a
                                left join vybory b using (uik)
                                group by 1''')