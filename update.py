import pandas as pd
import akshare as ak
import datetime
import tushare as ts
import dolphindb
import numpy as np
import time
import requests
def get_conn():
    db = dolphindb.session()
    db.connect("localhost", 8848, "admin", "123456")
    return db

conn = get_conn()

# 连接tushare
token = "f1ce53736e3b6777425d3df97c05e7460a55534db8ece60114c9e2a3"
ts.set_token(token)
pro = ts.pro_api()


# 更新daily_adj
def update_daily_adj(date):
    # 先下载 daily 数据
    # 判断是否已经有date的数据
    conn.run("daily_adj = loadTable('dfs://stock', `daily_adj)")
    if conn.run(f"select 1 from daily_adj where data_dt={date} limit 1").shape[0] == 0:
        print(f"已经存在{date}的daily_adj数据")
    else:
        # 下载数据
        df = pro.daily(start_date=date.replace(".",""), end_date=date.replace(".",""), adj='hfq')
        if df.shape[0] == 0:
            print(f"{date}没有数据")
            return
        df = df.query("not ts_code.str.contains('.BJ')", engine='python')
        # 把chg>20的刷成21, 把chg<-20的刷成-21
        df.pct_chg[df.pct_chg > 21] = 21
        df.pct_chg[df.pct_chg < -21] = -21      
        # to daily_adj
        new_order = ['trade_date', 'ts_code', 'pct_chg', 'open','high', 'low', 'close', 'amount']
        df = df.reindex(columns=new_order)
        df.rename(columns={'trade_date':'data_dt', 'amount':'amt', "pct_chg":"chg"}, inplace=True)

        # 日期从string转成datetime,然后上传.
        df['data_dt'] = pd.to_datetime(df['data_dt'], format='%Y%m%d').values.astype('datetime64[D]')
        df['ts_code'] = df['ts_code'].astype("object")
        conn.upload({'daily_adj_tmp':df})
        conn.run("tableInsert(daily_adj,(select * from daily_adj_tmp))")
        
        print(f"更新完成{date}的daily_adj数据")

# 批量更新daily_adj
def batch_update_daily_adj():
    conn.run("daily_adj = loadTable('dfs://stock', `daily_adj)")
    start_date = np.datetime_as_string(conn.run("select max(data_dt) from daily_adj").values[0][0], unit='D').replace("-",".")
    today = datetime.datetime.today().strftime("%Y.%m.%d")
    end_date = conn.run(f"transFreq({today}, 'SSE')").astype("str").replace("-",".") 
    print(start_date,"---", end_date)
    download_dates = conn.run(f"getMarketCalendar('SSE', {start_date}, {end_date})").tolist()
    
    for date in download_dates:
        date = date.strftime("%Y.%m.%d")
        print(date)
        while True:
            try:
                update_daily_adj(date)
                break
            except requests.exceptions.ConnectTimeout:
                print("出错了, 重试")
                time.sleep(5)
                continue
    print("更新完成批量daily_adj")
      


# # 更新股票列表
def update_basic():
    df = pro.stock_basic()
    df = df[(df['market'] != '北交所') & (df['market'] != 'CDR')] # 过滤北交所和CDR.
    df['ts_code'] = df['ts_code'].astype("object")
    df['symbol'] = df['ts_code'].str.slice(0,6)
    conn.upload({'basic_tmp':df})
    conn.run("""
    db = database("dfs://stock");
    if (existsTable("dfs://stock", 'basic')) db.dropTable('basic');
    basic = db.createTable(basic_tmp, 'basic');
    basic.tableInsert(select * from basic_tmp);""")
    print("更新完成basic")
# df.to_sql("basic",sdb._instance, index=False, if_exists="replace", method='multi')
# 要新增一个symbol 字段, 用来存储股票代码, 例如: 000001 
# 每次都删除重新创建一个.



# 更新概念与题材列表
# 每天更新,新增的概念. 每月进行一次全量的更新

# 增量更新概念
# 清洗概念,对于不想要的概念,进行清洗.
# 对于没有概念的股票,进行清洗.排除.


## 获取最新的概念列表
def update_concept_list():
    concept_list_df = ak.stock_board_concept_name_ths()
    concept_list_df['日期'] = pd.to_datetime(concept_list_df['日期'], format="%Y.%m.%d")
    conn.upload({"concept_list_df": concept_list_df})
    first_update_concept_flag = conn.run(" not existsTable('dfs://stock', 'concept_list_old')") 
    # 是否是第一次更新概念
    if first_update_concept_flag:
        # 如果是第一次更新概念, 则全量更新
        # // 日期, 概念名称, 成分股数量, 网址, 代码
        conn.run("""
        db = database("dfs://stock");
        concept_list_old = db.createTable(concept_list_df, 'concept_list_old');
        concept_list_old.append!(select * from concept_list_df);
        truncate('dfs://stock' , `concept_list_old);
        concept_list_new = db.createTable(concept_list_df, 'concept_list_new');
        concept_list_new.append!(select * from concept_list_df);
        """)
    else:
        # 比对old new ,找出成分股数量不同的概念, 这些概念就是新增的概念.
        # // 日期, 概念名称, 成分股数量, 网址, 代码
        conn.run("""
        db = database("dfs://stock");
        concept_list_new = loadTable("dfs://stock", 'concept_list_new');
        concept_list_old = loadTable("dfs://stock", 'concept_list_old');
        
        truncate('dfs://stock' , `concept_list_old); // 清空old
        concept_list_old.append!(select * from concept_list_new); // 原来 new 的值赋给 old
        truncate('dfs://stock' , `concept_list_new);// 清空new
        concept_list_new.append!(select * from concept_list_df);       
        """)
    print("更新完成concept_list")

def update_concept_cons(concept):
    conn.run("db = database('dfs://stock');")
    if  conn.run(f"existsTable('dfs://stock', 'concept_cons')"):
        conn.run("concept_cons = loadTable('dfs://stock', 'concept_cons');")
    
    # if conn.run(f"select * from  concept_cons where 概念名称 = '{concept}'").shape[0] > 0:
    #     print("已经存在", concept)
    #     return
    while True:
        try:
            print("开始下载", concept)
            df = ak.stock_board_concept_cons_ths(symbol=concept)
            df['概念名称'] = concept
            df = df[['代码', '名称', '概念名称']]
            conn.upload({"concept_cons_df": df})
            if not conn.run(f"existsTable('dfs://stock', 'concept_cons')"):
                conn.run("""
                t2 = select b.ts_code, c.* from basic b 
                    left join concept_cons_df c on b.symbol = c.代码 
                concept_cons = db.createTable(t2, 'concept_cons');
                concept_cons.append!(select * from t2);
                """)
            else:
                conn.run("concept_cons = loadTable('dfs://stock', 'concept_cons');")
                conn.run(f"""
                delete from concept_cons where 概念名称 = '{concept}';
                t2 = select b.ts_code, c.* from basic b 
                    left join concept_cons_df c on b.symbol = c.代码 
                concept_cons.append!(select * from t2);
                """)
            print("下载完成", concept)
            break
        except (requests.exceptions.ConnectTimeout,ValueError):
            print("出错了, 重试")
            time.sleep(5)
            continue


def batch_update_concept_cons():
    
    # 找到在 new中存在, 在old中不存在的概念
    # // 日期, 概念名称, 成分股数量, 网址, 代码
    conn.run("concept_list_new = loadTable('dfs://stock', 'concept_list_new');")
    conn.run("concept_list_old = loadTable('dfs://stock', 'concept_list_old');")
    concept_list = conn.run("""select 概念名称 as concept from concept_list_new new
          where not exists(select 1 from concept_list_old old 
                            where new.概念名称 = old.概念名称) 
          or new.概念名称 in (select 概念名称 from concept_list_old old 
                        where new.概念名称 = old.概念名称 and (new.成分股数量 <> old.成分股数量 or 
                                                            new.日期 <> old.日期))
          order by 日期 desc""")['concept'].values
    # concept_list = conn.run("""select 概念名称 as concept from concept_list_new
    #       order by 日期 desc""")['concept'].values
        
    for concept in concept_list:
        update_concept_cons(concept)
    print("更新完成concept_cons")



def update_ddb_tables():
    # 更新market_index
    # 更新stock_ma
    # 更新stock_ind
    # 更新concept_ind
    # 更新concept_cons
    conn.runFile("update_ddb_tables.dos")
    # 冷启动
    # conn.run("""
    # start_date = 2022.01.01
    # end_date = 2022.12.31
    # init_market_index(start_date, end_date)
    # init_stock_ma(start_date, end_date)
    # init_stock_ind(start_date, end_date)
    # //init_concept_ind(start_date, end_date)  // concept_ind 需要单独启动一遍, 因为换concept_cons了
    # """)

    # 热更新
    conn.run("""
    update_market_index()
    update_stock_ma()
    update_stock_ind()
    update_concept_ind()
    """)

    print("更新完成ddb_tables")
    

def update_all():
    update_basic()
    update_concept_list()
    batch_update_concept_cons()
    batch_update_daily_adj()
    update_ddb_tables()

