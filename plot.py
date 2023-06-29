import streamlit as st
import pandas as pd

from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.graph_objects as go 
import datetime

import dolphindb

# connect to database server
@st.cache_resource
def get_conn():
    db = dolphindb.session()
    db.connect("localhost", 8848, "admin", "123456")
    return db

# @st.cache_resource
# def get_conn():
#     return sqlite3.connect("stock.db", check_same_thread=False)

conn = get_conn()


config = {"editSelection":False, "displayModeBar": False, 
          "responsive":True, "editable":False, 
          "showAxisDragHandles":False, "showAxisRangeEntryBoxes":False}

@st.cache_data
def MA(S,N):              #求序列的N日简单移动平均值，返回序列                    
    return pd.Series(S).rolling(N).mean().values  


def plot_kline_fig(df, cur_date, height=500):
    """需要满足, df 中有 columns: data_dt, chg, open, close, high, low, amt"""
    dates = df['data_dt']
    
    cur_date = pd.to_datetime(cur_date, format="%Y.%m.%d")
    open = df['open']
    high = df['high']
    low = df['low']
    close = df['close']
    MA10 = MA(close, 10)
    MA5 = MA(close, 5)
    MA20 = MA(close, 20)
    amount = df['amt']
    pct_chg = df['chg']
    amount_color = df[['open','close']].apply(lambda x :'red' if x['close']>=x['open'] else 'green', axis=1)
    hover_text = list(map(lambda pct: f"涨跌幅:{pct:.2f}", pct_chg))

    fig = make_subplots(rows=5, cols=1,
        specs=[[{"rowspan": 4}],
                [{}],
                [{}],
                [{}],
                [{}],
            ])

    # 均线MA10
    line_ma = go.Scatter(x=dates, y=MA10, line=dict(color='#D2B48C',dash='solid', width=1),name='MA10', hoverinfo='skip')
    fig.add_trace(line_ma, row=1, col=1)

    # 均线MA5
    line_ma2 = go.Scatter(x=dates, y=MA5, line=dict(color='#FFD700',dash='solid', width=1),name='MA20', hoverinfo='skip')
    fig.add_trace(line_ma2, row=1, col=1)
    # 均线MA20
    line_ma20 = go.Scatter(x=dates, y=MA20, line=dict(color='#DDA0DD',dash='solid', width=1),name='MA20', hoverinfo='skip')
    fig.add_trace(line_ma20, row=1, col=1)

    # 蜡烛图
    candle_stick = go.Candlestick(x=dates, open=open, high=high, low=low, close=close,
        hovertext = hover_text, 
        increasing=dict(line=dict(color="#FF0000")),
        decreasing=dict(line=dict(color="#00FF00")),
        name=''
    )
    fig.add_trace(candle_stick, row=1, col=1)

    # 成交量图
    amount_bar = go.Bar(x=dates, y=amount, marker_color=amount_color,name='成交量')
    fig.add_trace(amount_bar, row=5, col=1)
    fig.update_yaxes(range=[amount.min(), amount.max()], row=5, col=1)
    fig.update_xaxes(dict(type="category"))
    
    # 垂直标记点线.
    max_high = high.max()
    min_low = low.min()
    line = go.Scatter(x=[cur_date, cur_date], y=[min_low, max_high],line=dict(color="gray", dash='dot'),hoverinfo='skip')
    fig.add_trace(line, row=1, col=1)

    # 成交量垂直标记线
    max_amount = amount.max()
    min_amount = 0 
    line2 = go.Scatter(x=[cur_date, cur_date],y=[min_amount,max_amount],line=dict(color="gray", dash='dot'),hoverinfo='skip')
    fig.add_trace(line2,row=5,col=1)

    fig.update_layout(
        xaxis=dict(showgrid=False, visible=False,type = "category"), 
        yaxis=dict(showgrid=False)
    )
    fig.update_layout(xaxis_rangeslider_visible=False)
    fig.update_layout(showlegend=False)
    fig.update_layout(xaxis_visible=False)
    fig.update_layout(yaxis_title=None, xaxis_title=None)
    fig.update_layout(height=height)
    fig.update_xaxes(tickfont=dict(size=1))
    fig.update_layout(autosize=True)
    st.plotly_chart(fig, use_container_width=False, config=config)


def read_kline_df(data_dt, ts_code, type="股票"):
    if type == "股票":
        script = f"""
        daily_adj = loadTable("dfs://stock", "daily_adj")
        cur_date = {data_dt}
        start_date = temporalAdd(cur_date, -20, "SSE") // 交易日偏移
        end_date = temporalAdd(cur_date, 20, "SSE") // 交易日偏移
        select data_dt, ts_code, chg, open, high, low, close, amt/100000 as amt 
            from daily_adj where data_dt between start_date:end_date and ts_code = `{ts_code}
        """
    
    elif type == "综合指数":
        script = f"""
        market_index = loadTable("dfs://stock", "market_index")
        cur_date = {data_dt}
        start_date = temporalAdd(cur_date, -20, "SSE") // 交易日偏移
        end_date = temporalAdd(cur_date, 20, "SSE") // 交易日偏移
        select data_dt, cum_open as open, cum_high as high, 
            cum_low as low, cum_close as close, amt, chg as chg
            from market_index where 
            data_dt between start_date:end_date order by data_dt asc
        """
        
    df = conn.run(script)
    return df



def plot_grid_fig(df: pd.DataFrame, type="股票" , height: int =500):
    options = GridOptionsBuilder.from_dataframe(
        df
    )
    options.configure_side_bar(filters_panel=False, columns_panel=False)
    options.configure_selection("single")
    update_mode = GridUpdateMode.SELECTION_CHANGED
    selection = AgGrid(
        df,
        gridOptions=options.build(),
        update_mode=update_mode,
        theme='material',
        height=height
    )
    if type == "股票":
        if selection and selection['selected_rows']:
            selected_stock = selection['selected_rows'][0]
            
            if 'ts_code' in selected_stock:
                ts_code = selected_stock['ts_code']
                st.session_state["ts_code"] = ts_code
            
        
    elif type == "题材":
        if selection and selection['selected_rows']:
            selected_concept = selection['selected_rows'][0]
            
            if 'concept' in selected_concept:
                concept = selected_concept['concept']
                st.session_state["concept"] = concept


def plot_concept_fig(df: pd.DataFrame, y_lable , cur_date , height: int =500):
    
    fig = px.line(df, x="data_dt", y=y_lable, color='concept',markers=True)
    max_amount = df[y_lable].max()
    min_amount = df[y_lable].min()
    # cur_date 转成 日期格式
    cur_date = cur_date.replace(".","-")
    line2 = go.Scatter(x=[cur_date, cur_date],y=[min_amount,max_amount],line=dict(color="gray", dash='dot'),hoverinfo='skip',mode='lines+markers')
    fig.add_trace(line2)
    fig.update_layout(xaxis_rangeslider_visible=False)
    fig.update_layout(showlegend=True)
    fig.update_layout(xaxis_visible=False)
    fig.update_layout(yaxis_title=None, xaxis_title=None)
    fig.update_layout(height=height)
    fig.update_xaxes(tickfont=dict(size=1))
    fig.update_layout(autosize=True)
    st.plotly_chart(fig, use_container_width=False, config=config)
    

@st.cache_data
def read_concept_df(data_dt, type, concept=None):
    conn.run(f"cur_date = {data_dt};t5 = temporalAdd(cur_date, -5, 'SSE') ;t_5 = temporalAdd(cur_date, 5, 'SSE');")
    conn.run("concept_ind = loadTable('dfs://stock', 'concept_ind')")
    if type == "中期大涨的概念":
        script = f"""
        select * from concept_ind where data_dt between t5:t_5 and rn_dl_desc < 10
        order by data_dt asc
        """
        y_label = "dl"

    elif type == "中期大跌的概念":
        script = f"""
        select * from concept_ind where data_dt between t5:t_5 and rn_dl_asc < 10
        order by data_dt asc
        """
        y_label = "dl"

    elif type == "当天大涨的概念":
        script = f"""
        select * from concept_ind where data_dt between t5:t_5 and rn_chg_desc < 10
        order by data_dt asc
        """
        y_label = "chg"


    if type == "当天大跌的概念":
        script = f"""
        select * from concept_ind where data_dt between t5:t_5 and rn_chg_asc < 10
        order by data_dt asc
        """
        y_label = "chg"
        
    elif type == "有潜力的概念":
        script = f"""
        select data_dt, concept, ql from concept_ind
        where data_dt between t5:t_5
        and rn_ql_desc < 10 order by data_dt asc
        """
        y_label = "ql"

    elif type == "概念K线" and concept is not None:
        script = f"""
        select data_dt, concept, dl from concept_ind
        where data_dt between t5:t_5 and concept like '%{concept}%'
        order by data_dt asc
        """ 
        y_label = "dl"

    df = conn.run(script)
    return df, y_label

   




