import streamlit as st
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import statsmodels.api as sm
import statsmodels.formula.api as smf
import networkx as nx
from scipy.optimize import curve_fit
from datetime import datetime

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
                            , avg(share::numeric) as umg_share 
                            from(select * from delivery a
                                left join vybory_ful b using (uik)
                                where b.umg_flg::numeric =1) as a
                                group by 1''').dropna()
    """
    Так выглядит датафрейм для нашей первой модели
    """
    st.dataframe(vybory_df)
    """
    В нашей модели зависимой переменной будет доля голосов за кандидата умного голосования,
    а фичей средние траты тех, кто должен был голосовать в этом УИКе
    
    Я **нетривиально** визуализирую данные с помощью plotly.graph_objects и с помощью
     **математического модуля scipy** найду функцию линейной регрессии и нарисую на графике
    
    """
    def linear_func(x,a,b):
        return a*x + b

    popt, pcov = curve_fit(linear_func, vybory_df['avg_spend'], vybory_df['umg_share'])


    figure=go.Figure()
    figure.add_trace(go.Scatter(x=vybory_df['avg_spend'], y=vybory_df['umg_share'], mode='markers', opacity=1, name='data'))
    figure.add_trace(go.Scatter(x=vybory_df['avg_spend'], y=linear_func(vybory_df['avg_spend'], *popt), mode='lines', name='fit'))
    figure.update_layout(title="Траты людей на еду и победа кандидата от УМГ", xaxis={'title':'Средние траты'}
                         , yaxis={'title':'Доля голосов кандидата УМГ'})
    st.plotly_chart(figure)

    '''Построим простую линейную регресию по этим данным'''
    model=smf.ols('umg_share~avg_spend', data=vybory_df).fit()

    st.write(model.summary())

    """
    
    Как можно видеть наша модель показывыает хоть и положительный коэффициент при средних тратах
    , но очень маленький, практически нулевой - это означает, что мы не учитываем какие-то факторы
    , поэтому мы спустимся на уровень ниже и посмотрим 
    на конкретных пользователей и добавим информации о них: из каких ресторанов человек чаще заказывал
    еду, как часто человек использовал промокоды, cколько у него машин, год его рождения, год выпуска автомобиля
    
    """

    vybory_df_2=get_data('''select * from delivery 
    left join (select phone_number, count(distinct gibdd2_car_model) as car_cnt, max(gibdd2_car_year) as car_year,gibdd2_dateofbirth::date as birth_day 
    from  gibdd group by 1, gibdd2_dateofbirth) as gibd using(phone_number)
    left join (select phone_number, sum(case when delivery2_promocode is null then 0 else 1 end) as promo_use_cnt from delivery_full group by 1) as s using(phone_number)
    left join (select distinct phone_number, delivery2_vendor_name as most_common_vendor
    from(select phone_number, delivery2_vendor_name, count(*) as freq from delivery_full group by 1,2 order by 3) as b) as v using (phone_number)
    ''')

    vybory_df_2['car_cnt'].fillna(0, inplace=True)
    vybory_df_2['birth_day']=pd.to_datetime(vybory_df_2['birth_day'])
    ##FROM (https://moonbooks.org/Articles/How-to-convert-a-dataframe-column-of-date-of-birth-DOB-to-column-of-age-with-pandas-in-python-/)
    def from_dob_to_age(born):
        today = datetime.today()
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

    vybory_df_2['age']=vybory_df_2['birth_day'].apply(from_dob_to_age)
    ##


    """
    #### Теперь создадим многофакторную модель. 
    
    Какие факторы в нее включать вы решаете сами, единственный фактор, который будет всегда это средние траты
    """

    options = st.multiselect('Выберете факторы для модели:', ['Любимый ресторан', 'Промокоды', 'Количество машин'
        , 'Год рождения', 'Год выпуска последнего автомобиля'])

    all_options = ['Любимый ресторан', 'Промокоды', 'Количество машин', 'Возраст', 'Год выпуска последнего автомобиля']

    all_factors = ['C(most_common_vendor)', 'C(promo_use_cnt)', 'C(car_cnt)', 'age', 'C(car_year)']
    all_factors_for_df = ['most_common_vendor', 'promo_use_cnt', 'car_cnt', 'age', 'car_year']

    chosen_options=[all_options.index(x) for x in options]
    chosen_factors=[all_factors[x] for x in chosen_options]
    chosen_factors_for_df=[all_factors_for_df[x] for x in chosen_options]
    uravnenie='share~delivery2_price_client_rub'
    for i in chosen_factors:
        uravnenie+='+'+i
    multi_factor_model=smf.ols(uravnenie, data=vybory_df_2[chosen_factors_for_df].dropna()).fit()
    st.write(multi_factor_model.summary())





