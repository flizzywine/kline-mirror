# Kline Mirror V4.0
# 第一版, 实现基本的可视化功能, 但过于冗杂, 且没有各类指标.
# 第二版, 引入了dolphindb, 做到了真正的时间序列分析,并优化了选股操作.
# 第三版, 用数学定义了大涨大跌. 热门题材.
# 第四版, 以概念题材为核心观察对象, 数据库完全切换到dolphindb

import streamlit as st
import pandas as pd
import sqlite3
import datetime
import numpy as np

from streamlit_option_menu import option_menu

st.set_page_config(layout="wide", page_title="Kline Mirror V4.0", page_icon=":shark:")

# from utils import *
from plot import *
from update import *


@st.cache_resource
def get_conn():
    db = dolphindb.session()
    db.connect("localhost", 8848, "admin", "123456")
    return db

conn = get_conn()

# 默认的配置 ,来自 session_state表,  field in ('tag', 'concept', 'date', 'ts_code')
def init_default_config():
    script = """   
    if (not existsTable("dfs://stock", `session_state)) {
        schema = table(1:0, `field`val`date, [STRING, STRING, DATE])
        db = database("dfs://stock")
        db.createTable(schema, `session_state)
        session_state = loadTable("dfs://stock", "session_state")
        data = table("tag"  as field, "中期大涨" as val, 9999.12.31 as date)
        session_state.append!(data)
        data = table("concept" as field, "整车" as val, 9999.12.31 as date)
        session_state.append!(data)
        data = table("date" as field, "2022.04.26" as val, 2022.04.26 as date)
        session_state.append!(data)
    } 
    """
    conn.run("session_state = loadTable('dfs://stock', 'session_state')")
    tag = conn.run("select val from session_state where field = 'tag'").val[0]
    concept = conn.run("select val from session_state where field = 'concept'").val[0]
    date = conn.run("select val from session_state where field = 'date'").val[0]
    st.session_state["tag"] = tag
    st.session_state["concept"] = concept
    st.session_state["date"] = date

    if "prefs" not in st.session_state:
        st.session_state["prefs"] = "高位"

    st.session_state['ts_code'] = None



# 时间控制, 时间的加减.

def date_控制():
    """最终还是把日期, 以字符串的形式表达出来了, 不然太难管控, 唯一的优点是,我不需要自己维护股票日历了"""
    cur_date = st.session_state["date"]
    date = st.text_input("输入日期","")
    next_date = st.button("--->")
    prev_date = st.button("<---")

    if next_date:
        new_date = conn.run(f"temporalAdd({cur_date}, 1, 'SSE')").astype("str").replace("-",".") 
        st.session_state["date"] = new_date
    elif prev_date:
        new_date = conn.run(f"temporalAdd({cur_date}, -1, 'SSE')").astype("str").replace("-",".")
        st.session_state["date"] = new_date

    cur_date = st.session_state["date"]
    date2 = st.date_input("挑选日期", datetime.datetime.strptime(cur_date, "%Y.%m.%d")).strftime("%Y.%m.%d")
    if not (next_date or prev_date):
        if date != "":
            new_date = date
        elif date == "" :
            new_date = date2 
        
    st.session_state["date"] = new_date
    conn.run(f"update session_state set val={new_date} where field='date'")
    st.write(new_date)

    if not conn.run(f"transFreq({new_date}, 'SSE') == {new_date}"): 
        #  最近的一天交易日不是当天
        st.warning(f"{new_date}非交易日, 请重新选择")

    
if not "date" in st.session_state:
    init_default_config()


# date_控制()

# 图表的展示, 参考app.py中的def 大盘情况_menu(date):
def 大盘情况_menu(date):
    
    st.write(f"当前日期: {date}")
    if date:
        st.write("大盘指数")
        df = read_kline_df(date, "", type="综合指数")
        plot_kline_fig(df, date, height=500)

        col1, col2 =  st.columns([1, 1])
        with col1:
            st.write("中期高度最高的个股")
            conn.run("stock_ind = loadTable('dfs://stock', 'stock_ind'); basic = loadTable('dfs://stock', 'basic')")
            
            df = conn.run(f"""select b.name, round(a.dl,2) as dl from stock_ind  a
                            left join basic  b on a.ts_code = b.ts_code
                            where a.data_dt={date} and a.rn_dl_desc < 5 and not isNull(a.rn_dl_desc)  order by a.dl desc""")
            st.dataframe(df)    
        with col2:
            st.write("当日涨幅最大的概念")
            conn.run("concept_ind = loadTable('dfs://stock', 'concept_ind')")
            df = conn.run(f"""select concept, round(chg,2) as chg from concept_ind where data_dt={date} and rn_chg_desc < 5 order by chg desc""")
            st.dataframe(df)


        st.write("中期大涨的概念")
        df, y_label = read_concept_df(date, "中期大涨的概念")
        plot_concept_fig(df, y_label, date, height=500)

        st.write("中期大跌的概念")
        df, y_label = read_concept_df(date, "中期大跌的概念")
        plot_concept_fig(df, y_label, date, height=500)
    
        with st.expander("当天概念情况", expanded=False):
            st.write("当天大涨的概念")
            df, y_label = read_concept_df(date, "当天大涨的概念")
            plot_concept_fig(df, y_label, date, height=500)

            st.write("当天大跌的概念")
            df, y_label = read_concept_df(date, "当天大跌的概念")
            plot_concept_fig(df, y_label, date, height=500)




@st.cache_data
def get_sql(data_dt, tag):
   
    conn.run("stock_ind = loadTable('dfs://stock', 'stock_ind'); daily_adj = loadTable('dfs://stock', 'daily_adj');")
    conn.run("concept_cons = loadTable('dfs://stock', 'concept_cons')")
    conn.run("basic = loadTable('dfs://stock', 'basic')")
    conn.run(f"t_5 = temporalAdd({data_dt}, 5, 'SSE') // 交易日偏移")
    if tag.startswith("概念:"):
        concept = st.session_state["concept"]
        sql = f"""select b.ts_code as ts_code from concept_cons a 
        left join basic b on a.代码 = b.symbol
        where a.概念名称 like '%{concept}%'"""
        return sql
    else:
        tag_sql_config = {
            "最近会涨":f"select ts_code from stock_ind where data_dt=t_5 and rn_zf5_desc < 200",
            "已经大涨": f"select ts_code from stock_ind where data_dt={data_dt} and rn_dl_desc <200",
            "最近会跌":f"select ts_code from stock_ind where data_dt=t_5 and rn_zf5_asc < 200",
            "已经大跌": f"select ts_code from stock_ind where data_dt={data_dt} and rn_dl_asc < 200",
            "当日大涨":f"select ts_code from daily_adj where data_dt={data_dt} and chg >=9.5",
            "当日大跌":f"select ts_code from daily_adj where data_dt={data_dt} and chg <= -6",
            "低位大涨": f"select ts_code from stock_ind where data_dt={data_dt} and zf5_10 < 115 and zf5>120 ",
            "均线重叠":f"select ts_code from stock_ind where data_dt={data_dt} and rn_bl_amt_asc < 100",
            "有潜力": f"select ts_code from stock_ind where data_dt={data_dt} and rn_ql_desc < 1000",
        }
        assert tag in tag_sql_config.keys()
        return tag_sql_config[tag]


@st.cache_data
def merge_set(tags , data_dt, prefs):
    """每个条件形成一个SQL,  然后intersect形成交集"""
    sql_list = []  
    for tag in tags:
        tag_sql = get_sql(data_dt, tag)
        sql_list.append(tag_sql)
    if len(sql_list) == 0:
        return None
    elif len(sql_list) == 1:
        final_sql = sql_list[0]
    else:
        final_sql = " INTERSECT ".join(sql_list)

    if st.session_state["prefs"]  == "低位": # 偏好低位
        order_by_col = "a.bl_amt asc"
    elif st.session_state["prefs"]  == "中位": # 偏好中位, 也即, 5日线靠近10日线
        order_by_col = "a.bl_mid asc"
    else: # 默认, 偏好高位
        order_by_col = "a.dl desc"

    conn.run("stock_ind = loadTable('dfs://stock', 'stock_ind'); basic = loadTable('dfs://stock', 'basic');")
    final_sql = f"""select b.name, a.ts_code from stock_ind a left join basic b on a.ts_code = b.ts_code 
    where a.ts_code in ({final_sql}) and a.data_dt={data_dt} order by {order_by_col}"""
        
    stock_list_df = conn.run(final_sql)
    st.write(f"{len(stock_list_df)}个股票")
    return stock_list_df
    

#个股情况先靠边, 先做sidebar ,把各种控制标签,做起来.
def 个股情况_menu(date):
    
    st.write(f"当前日期: {date}")
    # if "ts"
    # ts_code = st.session_state["ts_code"]

    left_col, right_col = st.columns([ 2, 5])
    with left_col:
        if date:
            tags = st.session_state["tags"]
            stock_list_df = merge_set(tags, date, st.session_state["prefs"])
            if stock_list_df is not None:
                plot_grid_fig(stock_list_df, type="股票")
            else:
                st.session_state['ts_code'] = None
            
            name = st.session_state["name"]
            if name != "" :
                conn.run("basic = loadTable('dfs://stock', 'basic')")
                ts_code = conn.run(f"select ts_code from basic where name like '%{name}%'").values[0][0]
                st.session_state['ts_code'] = ts_code

    with right_col:
        date = st.session_state['date']
        ts_code = st.session_state['ts_code']
        if date is not None and ts_code is not None:
            conn.run("concept_cons = loadTable('dfs://stock', 'concept_cons');bssic = loadTable('dfs://stock', 'basic') ")
            sql = f"select 概念名称 as concept from concept_cons where 代码='{ts_code[:6]}'"
            concepts = "||".join(list(conn.run(sql)['concept']))
            st.write(f"概念: {concepts}")
            st.write("----------")
            stock_name = conn.run(f"select name from basic where ts_code='{ts_code}'").values[0][0]
            st.write(stock_name)
            df = read_kline_df(date, ts_code, type='股票')
            plot_kline_fig(df, date)
                

def sidebar():
    date_控制()

    tag_list = [
            "最近会涨", 
            "已经大涨",
            "最近会跌", 
            "已经大跌",
            "当日大涨", 
            "当日大跌",
            "低位大涨",
            "均线重叠",
            "有潜力"
            ]
    name = st.text_input("选股:名称", "")
    st.session_state['name'] = name
    concept = st.text_input("选股:概念", "")
    st.session_state["concept"] = concept
    if concept != "" and concept != st.session_state['concept']:
        conn.run(f"update session_state set val='{concept}' where filed='concept'")

    st.write("选股:标签")
    checks = [st.checkbox(tag, value=True if tag==st.session_state['tag'] else False) for tag in tag_list]
    tags = []
    concept = st.session_state["concept"]
    if concept != "":
        tags.append(f"概念:{concept}")
    for check, tag in zip(checks, tag_list):
        if check:
            tags.append(tag)
    st.session_state["tags"] = tags

    st.write("选股:偏好")
    st.session_state["prefs"] = st.select_slider("偏好", options=["低位", "中位", "高位"], value="高位", key="偏好")
   
    
    if len(tags) == 1 and tags[0] != st.session_state['tag']:
        conn.run(f"update session_state set val='{tags[0]}' where field='tag'")

    if st.button("更新数据库", key="更新数据库"):
        update_all()

def 题材列表_menu_df(date, selected_menu):
    conn.run("concept_ind = loadTable('dfs://stock', 'concept_ind')")
    if selected_menu == "中期大涨":
        # 中期大涨的概念
        st.write("中期大涨的概念")
        df = conn.run(f"select concept, round(dl,2) as dl from concept_ind where  data_dt={date} and rn_dl_desc < 20 order by dl desc")
        
    elif selected_menu == "中期大跌":
        # 中期大跌的概念
        st.write("中期大跌的概念")
        df = conn.run(f"select concept, round(dl,2) as dl from concept_ind where  data_dt={date} and rn_dl_asc < 20 order by dl asc")
        
    elif selected_menu == "当日大涨":
        # 当天大涨的概念
        st.write("当天大涨的概念")
        df = conn.run(f"select concept, round(chg,2) as chg from concept_ind where  data_dt={date} and rn_chg_desc < 20 order by chg desc")
        
    elif selected_menu == "当日大跌":
        # 当天大跌的概念
        st.write("当天大跌的概念")
        df = conn.run(f"select concept, round(chg,2) as chg from concept_ind where  data_dt={date} and rn_chg_asc < 20 order by chg asc")

    return df
        

def 题材列表_menu(date):
    st.write(f"当前日期: {date}")
    
    left_col, right_col = st.columns([ 2, 5])
    with left_col:
        selected_menu = option_menu(None, ["中期大涨", "中期大跌", "当日大涨", "当日大跌"], 
            icons=['list-task',  "list-task", "list-task", "list-task"], 
            menu_icon="cast", default_index=0, orientation="horizontal")
        
        df = 题材列表_menu_df(date, selected_menu)
        if df is not None:
            plot_grid_fig(df, type="题材", height=400)

    with right_col:
        concept = st.session_state["concept"]
        if concept is not None and concept != "":
            df, y_label = read_concept_df(date, type="概念K线", concept=concept)
            plot_concept_fig(df, y_label, date)


with st.sidebar:
    sidebar()


def main():
    selected_menu = option_menu(None, ["大盘情况", "个股情况", "题材列表"], 
    icons=['house',  "list-task", "list-task"], 
    menu_icon="cast", default_index=0, orientation="horizontal")
    date = st.session_state["date"]
    if selected_menu == "大盘情况":
        大盘情况_menu(date)
    elif selected_menu == "个股情况":
        个股情况_menu(date)
    elif selected_menu == "题材列表":
        题材列表_menu(date)

main()