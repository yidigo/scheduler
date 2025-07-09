import pandas as pd
import numpy as np
from sklearn import ensemble
from collections import Counter
import os
from pathlib import Path
import sys
import argparse
from datetime import date, datetime
import requests
import logging
import time
from typing import Dict, Optional, Any

http_session: Optional[requests.Session] = requests.Session()

task_config: Optional[Dict[str, Any]] = {
    "clickhouse_url": "http://127.0.0.1:59011/", # 确保 URL 以 / 结尾
    "clickhouse_user": "default",
    "clickhouse_pass": ""
}

# 日志记录器
# 建议在您的应用中配置一个合适的 logger
task_logger = logging.getLogger(__name__)
# 为了能看到日志输出，这里做一个简单配置
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


# --- 自定义异常 ---
class ClickHouseRequestError(Exception):
    """自定义异常，用于表示 ClickHouse 请求失败。"""
    def __init__(self, message, status_code=None, response_body=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body

    def __str__(self):
        return f"{super().__str__()} (Status: {self.status_code}, Body: {self.response_body})"

def do_clickhouse_request(query: str, format: str = "JSON") -> bytes:

    if http_session is None:
        raise ValueError("HTTP session not initialized")
    if task_config is None:
        raise ValueError("Task configuration not initialized")

    # ClickHouse URL，确保以 / 结尾
    base_url = task_config.get("clickhouse_url")
    if not base_url:
        raise ValueError("clickhouse_url is not configured")

    # 使用 params 字典来构建查询参数，requests 库会自动处理 URL 编码
    params = {
        "add_http_cors_header": 1,
        "default_format": format,
        "max_result_rows": 10000,
        "max_result_bytes": 100000000,
        "result_overflow_mode": "break"
    }

    headers = {
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9", # 可选
        "Connection": "keep-alive",
        "Content-Type": "text/plain;charset=UTF-8"
        # 其他可选的头信息可以加在这里
        # "Origin": base_url,
        # "Referer": f"{base_url}play?user={task_config.get('clickhouse_user')}"
    }

    # Basic Authentication
    auth = None
    user = task_config.get("clickhouse_user")
    if user:
        # log.info("Using ClickHouse user: %s", user)
        auth = (user, task_config.get("clickhouse_pass", ""))

    start_time = time.monotonic() # 使用 monotonic 时钟来测量时间间隔更准确

    try:
        response = http_session.post(
            base_url,
            params=params,
            headers=headers,
            data=query.encode('utf-8'), # 将查询字符串编码为字节
            auth=auth,
            timeout=30 # 建议设置超时
        )

        duration = time.monotonic() - start_time
        task_logger.debug(
            f"ClickHouse query executed. Format: {format}, Duration: {duration:.4f}s, Status: {response.status_code}"
        )

        # 检查 HTTP 状态码，如果不是 2xx，requests.raise_for_status() 会抛出 HTTPError 异常
        response.raise_for_status()

        # 读取响应内容 (字节)
        return response.content

    except requests.exceptions.HTTPError as e:
        # 处理非 200 的状态码
        task_logger.error(
            f"ClickHouse request failed: status={e.response.status_code}, body={e.response.text}, query={query}"
        )
        raise ClickHouseRequestError(
            f"ClickHouse error (status {e.response.status_code})",
            status_code=e.response.status_code,
            response_body=e.response.text
        ) from e
    except requests.exceptions.RequestException as e:
        # 处理其他请求相关的错误（如网络问题、超时等）
        task_logger.error(f"Error making ClickHouse request: {e}")
        raise ClickHouseRequestError(f"Executing request failed: {e}") from e




def get_mode(desc_list):
    # 取时间段内占比最多的标签
    if len(list(desc_list)) == 0:
        return np.NaN
    frequency_counts = Counter(list(desc_list))
    max_count = max(frequency_counts.values())
    modes = [k for k, v in frequency_counts.items() if v == max_count]
    return max(modes)


def cal_theo_power_generation(history_df, model, cut_in_wind_speed, cut_out_wind_speed):
    # 根据拟合曲线预测得到理论发电量
    input_arr = history_df["风速"].values
    if model is not None:
        if not isinstance(input_arr, np.ndarray):
            print("please input correct wind data numpy array.")
            return None
        input_arr = np.maximum(input_arr, 0)
        input_arr = input_arr.reshape(-1, 1)
        # 将风速小于切入风速*0.8（小风修正系数）或风速大于切出风速对应的功率置零
        mask = (input_arr <= cut_in_wind_speed * 0.8) | (input_arr >= cut_out_wind_speed)
        output_arr = model.predict(input_arr)
        output_arr = output_arr.reshape(-1, 1)
        output_arr[mask] = 0
        output_arr = output_arr.reshape(-1)
        return output_arr
    return [np.nan] * len(history_df)


def adjust_curve(df_plot):
    # 随机森林拟合功率曲线得到模型

    model = ensemble.RandomForestRegressor(n_estimators=50,random_state=45)
    df = df_plot.copy()
    df = df.dropna()
    model.fit(df["wind"].values.reshape(-1, 1), df['power'].values.reshape(-1, 1))
    return model



def bj_new_cal(data, map_v):
    # 秒级计算，TBA需要根据工况标志位划分计算每天每个工况的累计时长（min）,PBA需要降采样到五分钟或十分钟分别计算每条的平均风速、应发电量、实发电量、秒数
    return_cols = ['类型', '日期', '标签', '时长', 'time', '标准简称', '风速', '实发电量', '应发电量', '秒数']
    if len(data) == 0:
        return pd.DataFrame([['', '', 1000, 1440] + [''] * (len(return_cols) - 4)], columns=return_cols)
    date_day = str(data['time'].iloc[0])[:10]
    if len(data[data['发电机气温'] == 0.0]) > 0:
        data = data[data['发电机气温'] != 0.0]
    if len(data) == 0:
        return pd.DataFrame([['TBA', date_day, 1000, 1440] + [''] * (len(return_cols) - 4)], columns=return_cols)
    # TODO 后期如果主控修改采集通道，则将该文件中“发电机气温”全部改为最新通道名称
    data.rename(columns={'发电机气温': '标签'}, inplace=True)
    # TBA，获取每个工况标签的累计时长
    data_tba = data.groupby(by=['标签'], as_index=False)['time'].count()
    data_tba.rename(columns={"time": '时长'}, inplace=True)
    data_tba['时长'] /= 60
    data_tba.loc['信息不可用-缺失数据'] = [1000, 1440 - data_tba['时长'].sum()]
    data_tba['日期'] = date_day
    data_tba['类型'] = 'TBA'
    data = data.set_index('time')
    float_columns = ['风速', '变流器有功功率']
    data[float_columns] = data[float_columns].astype(float)
    # PBA，获取每五分钟或每十分钟的风速、功率、秒数、工况标签，实发电量、应发电量
    model_theory = map_v['model_5min']
    cut_in = map_v['cut_in_speed']
    cut_out = map_v['cut_out_speed']
    # 菲律宾项目降采样到5min，计算实发电量、应发电量
    a = data.resample('5T', label='left').agg(
        {'风速': 'mean', '变流器有功功率': 'mean', '刹车号': 'count', '标签': get_mode})
    a.rename(columns={"刹车号": "秒数"}, inplace=True)
    a = a.dropna(subset=['风速'])
    a['实发电量'] = a['变流器有功功率'] * a['秒数'] / (60 * 60)
    a['应发电量'] = cal_theo_power_generation(a, model_theory, cut_in, cut_out) * a['秒数'] / (60 * 60)
    if 'model_5min_inter' in map_v:
        a['国际应发电量'] = cal_theo_power_generation(a, map_v['model_5min_inter'], cut_in, cut_out) * a['秒数'] / 3600
    else:
        a['国际应发电量'] = 0
    a['标准简称'] = 'sany'
    # 摩洛哥项目降采样到10min，计算实发电量、应发电量
    b = data.groupby(by=[pd.Grouper(freq='10T'), '标签']).agg(
        {'风速': 'mean', '变流器有功功率': 'mean', '刹车号': 'count'})
    b.rename(columns={"刹车号": "秒数"}, inplace=True)
    b = b.dropna(subset=['风速'])
    b['实发电量'] = b['变流器有功功率'] * b['秒数'] / (60 * 60)
    b['应发电量'] = cal_theo_power_generation(b, model_theory, cut_in, cut_out) * b['秒数'] / (60 * 60)
    if 'model_5min_inter' in map_v:
        b['国际应发电量'] = cal_theo_power_generation(b, map_v['model_5min_inter'], cut_in, cut_out) * b['秒数'] / 3600
    else:
        b['国际应发电量'] = 0
    b['标准简称'] = 'morocco'
    process_df = pd.concat([a.reset_index(drop=False), b.reset_index(drop=False)])
    process_df['日期'] = date_day
    process_df['类型'] = 'PBA'
    return_df = pd.concat([data_tba, process_df[
        ['类型', '日期', 'time', '标准简称', '标签', '风速', '实发电量', '应发电量', '秒数', '国际应发电量']]])
    return return_df


def primary_power(df, turbines_turple, name='潜在发电量'):
    # 计算潜在发电量，潜在发电量（基于理论功率曲线）、国际潜在发电量（基于国际功率曲线）
    df = df.reset_index(drop=True)
    df_c = df.copy()
    for base_t, primary_t in turbines_turple:
        if isinstance(primary_t, list):
            primary_df = df[(df['机组'].isin(primary_t)) & (df['标签'] == 120)][['time', '实发电量', '秒数']]
            primary_df = primary_df.groupby(['time'], as_index=False).mean()
        else:
            primary_df = df[(df['机组'] == primary_t) & (df['标签'] == 120)][['time', '实发电量', '秒数']]
            primary_df = primary_df.drop_duplicates(['time'])
        primary_df.rename(columns={'实发电量': name + '_new', '秒数': '原秒数'}, inplace=True)
        base_df = df[(df['机组'] == base_t) & (df['标签'] != 120)].merge(primary_df, on='time', how='left')
        base_df[name + '_new'].index = df[(df['机组'] == base_t) & (df['标签'] != 120)].index
        df_c.loc[(df_c['机组'] == base_t) & (df_c['标签'] != 120), name + '_new'] = (
                base_df[name + '_new'] * base_df['秒数'] / base_df['原秒数'])
    if name in df_c.columns:
        df_c.loc[df_c[name].isnull(), name] = df_c.loc[df_c[name].isnull(), name + '_new']
    else:
        df_c[name] = df_c[name + '_new']
    df_c.pop(name + '_new')
    return df_c



def _format_value(value):
    """
    Formats a single Python value for a SQL query, handling different types.
    - Strings are quoted and escaped.
    - Dates are quoted in 'YYYY-MM-DD' format.
    - Numbers are converted to strings.
    - None/NaN are converted to NULL.
    """
    if pd.isna(value):
        return "NULL"
    elif isinstance(value, (str)):
        # Escape single quotes for SQL
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"
    elif isinstance(value, (datetime, date)):
        # Format date/datetime objects
        return f"'{value.strftime('%Y-%m-%d')}'"
    elif isinstance(value, (int, float)):
        # Numbers don't need quotes
        return str(value)
    else:
        # Fallback for other types, treat as string
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"

def generate_clickhouse_insert_from_df(df: pd.DataFrame, table_name: str) -> str:
    """
    Generates a single ClickHouse SQL INSERT statement string from a Pandas DataFrame.
    This function dynamically uses the columns from the provided DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to convert. The column order in the DataFrame
                           will be used for the INSERT statement.
        table_name (str): The full name of the target database table (e.g., 'db.table').

    Returns:
        str: A single, multi-row SQL INSERT statement string.
    """
    if df.empty:
        return f"-- DataFrame is empty, no INSERT statement generated for {table_name}\n"

    # Get column names from the DataFrame, enclosed in backticks for safety
    columns_sql = ", ".join([f"`{col}`" for col in df.columns])

    # Process each row into a SQL values tuple string
    values_list = []
    # Use itertuples for efficient row iteration
    for row in df.itertuples(index=False, name=None):
        formatted_row = [_format_value(value) for value in row]
        values_list.append(f"({', '.join(formatted_row)})")

    # Join all row strings into a single string for the VALUES clause
    values_sql = ",\n".join(values_list)

    # Assemble the final SQL statement
    insert_sql = (
        f"INSERT INTO {table_name} ({columns_sql})\n"
        f"VALUES\n{values_sql};"
    )

    return insert_sql


# start_time = '2025-02-04'
# end_time = '2025-02-04'
#
# farm = 'WLTEQ'
#
# # 根据风场内部拼音获取内部风场名
# farm_ch = '乌拉特潇源'
# turbines = ['001', '002', '003', '004', '005', '006']


def get_inner_type_by_turbine(farm, turbine):
    if turbine=="006":
        return "SI214100"  # todo load from config
    else:
        return "SI19580"


def get_platform_by_turbine(farm, turbine):
    if turbine=="006":
        return "919"  # todo load from config
    else:
        return "915"


def get_files(data_dir, farm, turbine):
    matched_file_paths = []
    for path_object in Path(data_dir).iterdir():
        # iterdir() 会列出文件和文件夹，需要用 is_file() 过滤
        if path_object.is_file():
            if path_object.name.find(turbine) != -1:
                if not path_object.name.endswith(".parquet"):
                    continue
                matched_file_paths.append(os.path.join(data_dir,path_object.name))

    return matched_file_paths



def get_power_curve_by_turbine(farm, turbine):
    if turbine == "001":
        return pd.DataFrame({"wind":{"0":3.0,"1":3.5,"2":4.0,"3":4.5,"4":5.0,"5":5.5,"6":6.0,"7":6.5,"8":7.0,"9":7.5,"10":8.0,"11":8.5,"12":9.0,"13":9.5,"14":10.0,"15":10.5,"16":11.0,"17":11.5,"18":12.0,"19":12.5,"20":13.0,"21":13.5,"22":14.0,"23":14.5,"24":15.0,"25":15.5,"26":16.0,"27":16.5,"28":17.0,"29":17.5,"30":18.0,"31":18.5,"32":19.0,"33":19.5,"34":20.0,"35":20.5,"36":21.0,"37":21.5,"38":22.0,"39":22.5,"40":23.0,"41":23.5,"42":24.0,"43":24.5,"44":25.0},"power":{"0":105.7,"1":218.3,"2":360.7,"3":554.4,"4":801.8,"5":1104.4,"6":1461.1,"7":1869.3,"8":2339.4,"9":2874.6,"10":3485.7,"11":4183.9,"12":4929.7,"13":5709.0,"14":6466.5,"15":7033.0,"16":7403.3,"17":7627.4,"18":7761.6,"19":7843.3,"20":7894.2,"21":7928.1,"22":7954.3,"23":7981.2,"24":8000.0,"25":8000.0,"26":8000.0,"27":8000.0,"28":8000.0,"29":8000.0,"30":8000.0,"31":8000.0,"32":8000.0,"33":8000.0,"34":8000.0,"35":8000.0,"36":8000.0,"37":8000.0,"38":8000.0,"39":8000.0,"40":8000.0,"41":8000.0,"42":8000.0,"43":8000.0,"44":8000.0}}
                            )
    elif turbine == "002":
        return pd.DataFrame({"wind":{"0":3.0,"1":3.5,"2":4.0,"3":4.5,"4":5.0,"5":5.5,"6":6.0,"7":6.5,"8":7.0,"9":7.5,"10":8.0,"11":8.5,"12":9.0,"13":9.5,"14":10.0,"15":10.5,"16":11.0,"17":11.5,"18":12.0,"19":12.5,"20":13.0,"21":13.5,"22":14.0,"23":14.5,"24":15.0,"25":15.5,"26":16.0,"27":16.5,"28":17.0,"29":17.5,"30":18.0,"31":18.5,"32":19.0,"33":19.5,"34":20.0,"35":20.5,"36":21.0,"37":21.5,"38":22.0,"39":22.5,"40":23.0,"41":23.5,"42":24.0,"43":24.5,"44":25.0},"power":{"0":105.7,"1":218.3,"2":360.7,"3":554.4,"4":801.8,"5":1104.4,"6":1461.1,"7":1869.3,"8":2339.4,"9":2874.6,"10":3485.7,"11":4183.9,"12":4929.7,"13":5709.0,"14":6466.5,"15":7033.0,"16":7403.3,"17":7627.4,"18":7761.6,"19":7843.3,"20":7894.2,"21":7928.1,"22":7954.3,"23":7981.2,"24":8000.0,"25":8000.0,"26":8000.0,"27":8000.0,"28":8000.0,"29":8000.0,"30":8000.0,"31":8000.0,"32":8000.0,"33":8000.0,"34":8000.0,"35":8000.0,"36":8000.0,"37":8000.0,"38":8000.0,"39":8000.0,"40":8000.0,"41":8000.0,"42":8000.0,"43":8000.0,"44":8000.0}}
                            )

    elif turbine == "003":
        return pd.DataFrame({"wind":{"0":3.0,"1":3.5,"2":4.0,"3":4.5,"4":5.0,"5":5.5,"6":6.0,"7":6.5,"8":7.0,"9":7.5,"10":8.0,"11":8.5,"12":9.0,"13":9.5,"14":10.0,"15":10.5,"16":11.0,"17":11.5,"18":12.0,"19":12.5,"20":13.0,"21":13.5,"22":14.0,"23":14.5,"24":15.0,"25":15.5,"26":16.0,"27":16.5,"28":17.0,"29":17.5,"30":18.0,"31":18.5,"32":19.0,"33":19.5,"34":20.0,"35":20.5,"36":21.0,"37":21.5,"38":22.0,"39":22.5,"40":23.0,"41":23.5,"42":24.0,"43":24.5,"44":25.0},"power":{"0":105.7,"1":218.3,"2":360.7,"3":554.4,"4":801.8,"5":1104.4,"6":1461.1,"7":1869.3,"8":2339.4,"9":2874.6,"10":3485.7,"11":4183.9,"12":4929.7,"13":5709.0,"14":6466.5,"15":7033.0,"16":7403.3,"17":7627.4,"18":7761.6,"19":7843.3,"20":7894.2,"21":7928.1,"22":7954.3,"23":7981.2,"24":8000.0,"25":8000.0,"26":8000.0,"27":8000.0,"28":8000.0,"29":8000.0,"30":8000.0,"31":8000.0,"32":8000.0,"33":8000.0,"34":8000.0,"35":8000.0,"36":8000.0,"37":8000.0,"38":8000.0,"39":8000.0,"40":8000.0,"41":8000.0,"42":8000.0,"43":8000.0,"44":8000.0}}
                            )
    elif turbine == "004":
        return pd.DataFrame({"wind":{"0":3.0,"1":3.5,"2":4.0,"3":4.5,"4":5.0,"5":5.5,"6":6.0,"7":6.5,"8":7.0,"9":7.5,"10":8.0,"11":8.5,"12":9.0,"13":9.5,"14":10.0,"15":10.5,"16":11.0,"17":11.5,"18":12.0,"19":12.5,"20":13.0,"21":13.5,"22":14.0,"23":14.5,"24":15.0,"25":15.5,"26":16.0,"27":16.5,"28":17.0,"29":17.5,"30":18.0,"31":18.5,"32":19.0,"33":19.5,"34":20.0,"35":20.5,"36":21.0,"37":21.5,"38":22.0,"39":22.5,"40":23.0,"41":23.5,"42":24.0,"43":24.5,"44":25.0},"power":{"0":105.7,"1":218.3,"2":360.7,"3":554.4,"4":801.8,"5":1104.4,"6":1461.1,"7":1869.3,"8":2339.4,"9":2874.6,"10":3485.7,"11":4183.9,"12":4929.7,"13":5709.0,"14":6466.5,"15":7033.0,"16":7403.3,"17":7627.4,"18":7761.6,"19":7843.3,"20":7894.2,"21":7928.1,"22":7954.3,"23":7981.2,"24":8000.0,"25":8000.0,"26":8000.0,"27":8000.0,"28":8000.0,"29":8000.0,"30":8000.0,"31":8000.0,"32":8000.0,"33":8000.0,"34":8000.0,"35":8000.0,"36":8000.0,"37":8000.0,"38":8000.0,"39":8000.0,"40":8000.0,"41":8000.0,"42":8000.0,"43":8000.0,"44":8000.0}}
                            )
    elif turbine == "005":
        return pd.DataFrame({"wind":{"0":3.0,"1":3.5,"2":4.0,"3":4.5,"4":5.0,"5":5.5,"6":6.0,"7":6.5,"8":7.0,"9":7.5,"10":8.0,"11":8.5,"12":9.0,"13":9.5,"14":10.0,"15":10.5,"16":11.0,"17":11.5,"18":12.0,"19":12.5,"20":13.0,"21":13.5,"22":14.0,"23":14.5,"24":15.0,"25":15.5,"26":16.0,"27":16.5,"28":17.0,"29":17.5,"30":18.0,"31":18.5,"32":19.0,"33":19.5,"34":20.0,"35":20.5,"36":21.0,"37":21.5,"38":22.0,"39":22.5,"40":23.0,"41":23.5,"42":24.0,"43":24.5,"44":25.0},"power":{"0":105.7,"1":218.3,"2":360.7,"3":554.4,"4":801.8,"5":1104.4,"6":1461.1,"7":1869.3,"8":2339.4,"9":2874.6,"10":3485.7,"11":4183.9,"12":4929.7,"13":5709.0,"14":6466.5,"15":7033.0,"16":7403.3,"17":7627.4,"18":7761.6,"19":7843.3,"20":7894.2,"21":7928.1,"22":7954.3,"23":7981.2,"24":8000.0,"25":8000.0,"26":8000.0,"27":8000.0,"28":8000.0,"29":8000.0,"30":8000.0,"31":8000.0,"32":8000.0,"33":8000.0,"34":8000.0,"35":8000.0,"36":8000.0,"37":8000.0,"38":8000.0,"39":8000.0,"40":8000.0,"41":8000.0,"42":8000.0,"43":8000.0,"44":8000.0}}
                            )
    elif turbine == "006":
        return pd.DataFrame({"wind":{"0":3.0,"1":3.5,"2":4.0,"3":4.5,"4":5.0,"5":5.5,"6":6.0,"7":6.5,"8":7.0,"9":7.5,"10":8.0,"11":8.5,"12":9.0,"13":9.5,"14":10.0,"15":10.5,"16":11.0,"17":11.5,"18":12.0,"19":12.5,"20":13.0,"21":13.5,"22":14.0,"23":14.5,"24":15.0,"25":15.5,"26":16.0,"27":16.5,"28":17.0,"29":17.5,"30":18.0,"31":18.5,"32":19.0,"33":19.5,"34":20.0,"35":20.5,"36":21.0,"37":21.5,"38":22.0,"39":22.5,"40":23.0,"41":23.5,"42":24.0,"43":24.5,"44":25.0},"power":{"0":124.58848,"1":253.89064,"2":418.33487,"3":646.1822,"4":942.1466,"5":1308.9038,"6":1745.5771,"7":2249.0266,"8":2817.1658,"9":3461.3635,"10":4196.869,"11":5039.7974,"12":5954.933,"13":6907.8545,"14":7836.535,"15":8666.227,"16":9231.428,"17":9547.541,"18":9725.123,"19":9841.267,"20":9918.096,"21":9968.743,"22":10000.0,"23":10000.0,"24":10000.0,"25":10000.0,"26":10000.0,"27":10000.0,"28":10000.0,"29":10000.0,"30":10000.0,"31":10000.0,"32":10000.0,"33":10000.0,"34":10000.0,"35":10000.0,"36":10000.0,"37":10000.0,"38":10000.0,"39":10000.0,"40":10000.0,"41":10000.0,"42":10000.0,"43":10000.0,"44":10000.0}}
                            )


def get_pwr_curve(pwr_curve_type, pinyin_code, tur_type):
    return []


def run_calculation(args):

    iec_df = pd.read_csv('./condition_labels.txt', sep='	')
    iec_df['标签'] = iec_df['标签'].astype(int)
    en_to_ch_dict = dict(zip(iec_df['标签'], iec_df['工况标签']))
    label_cols = list(iec_df['标签'].unique())
    print(label_cols)

    choose_dict = {'240': 'choose_240_period', '月度': 'choose_month_period', '年度': 'choose_year_period',
                   '不划分': 'choose_none_period', '大小风季': 'choose_major_minor_wind_period'}
    df240 = pd.read_csv('./风场240时间.txt', sep=',')
    df240 = df240.dropna(subset=['240时间'])
    df240['并网时间'] = pd.to_datetime(df240['并网时间'])
    df240['240时间'] = pd.to_datetime(df240['240时间'])


    """主计算逻辑"""
    # 2. 从 args 中获取参数
    start_time = args.start_time
    end_time = args.end_time
    farm = args.farm
    farm_ch = args.farm_ch
    turbines = args.turbines
    data_dir = args.data_dir


    script_dir = Path(__file__).parent
    try:
        wf_df = pd.read_csv("./wf_df.csv")
    except FileNotFoundError:
        print(f"Error: wf_df.parquet not found in {script_dir}", file=sys.stderr)
        sys.exit(1)

    date_df = pd.DataFrame([[str(one)[:10] for one in pd.date_range(start_time, end_time)]], index=['日期']).T
    date_df['日期'] = pd.to_datetime(date_df['日期']).dt.date
    new_summary_tba_list = []
    new_summary_pba_list = []
    for turbine in turbines:
        type_name = get_inner_type_by_turbine(farm, turbine)
        plat_name = get_platform_by_turbine(farm, turbine)
        print(farm_ch, farm, turbine)
        # 获取秒级数据的文件名称
        files = get_files(data_dir+start_time,farm, turbine)
        if len(files) == 0:
            continue
        # 新算法-控制更新新算法-通过“发电机气温”
        # new_columns = ['time', '发电机气温', '风速', '变流器有功功率', '刹车号']
        new_columns = ['time', 'MC531', 'MC004', 'MC209', 'MC132']
        turbine_info_df = wf_df[(wf_df['pinyin_code'] == farm) & (wf_df['inner_turbine_name'] == turbine)]
        try:
            cut_in_speed = int(turbine_info_df['cut_in_speed'].values[0])
            cut_out_speed = int(turbine_info_df['cut_out_speed'].values[0])
        except Exception:
            cut_in_speed = 3
            cut_out_speed = 25
        # 用国际功率曲线\OPS理论功率曲线，用于计算国际\国内应发电量
        df_power_curve = get_power_curve_by_turbine(farm, turbine)
        df_power_curve.rename(columns={"Wind": "wind", "Power": "power"}, inplace=True)
        model_5min = adjust_curve(df_power_curve)

        # 获取国际功率曲线
        df_power_curve_intl = get_pwr_curve(pwr_curve_type="INTL", pinyin_code=farm, tur_type=type_name[2:])
        if len(df_power_curve_intl) > 0:
            model_5min_inter = adjust_curve(df_power_curve_intl)
            map_v = {'model_5min': model_5min, 'model_5min_inter': model_5min_inter,
                     'cut_in_speed': cut_in_speed, 'cut_out_speed': cut_out_speed}
        else:
            map_v = {'model_5min': model_5min, 'cut_in_speed': cut_in_speed, 'cut_out_speed': cut_out_speed}

        # 获取秒级数据
        df_tmp_list = list()
        for f in files:
            df_tmp = pd.read_parquet(f, columns=new_columns)
            df_tmp.rename(columns={'MC531': '发电机气温', 'MC004': '风速', 'MC209': '变流器有功功率', 'MC132': '刹车号'}, inplace=True)
            df_tmp_list.append(bj_new_cal(df_tmp, map_v))
        new_tt_df = pd.concat(df_tmp_list)
        # new_tt_df = get_data_process(files, columns=new_columns, map_fc=bj_new_cal, map_v=map_v)
        if isinstance(new_tt_df, pd.DataFrame) and len(new_tt_df) > 0:
            new_tt_df = new_tt_df.drop_duplicates()
            new_turbine_df = new_tt_df[new_tt_df['类型'] == 'TBA']  # TBA
            if len(new_turbine_df) > 0:
                new_turbine_df = new_turbine_df.groupby(by=['日期', '标签'], as_index=False)['时长'].sum()
                new_turbine_df['日期'] = pd.to_datetime(new_turbine_df['日期']).dt.date
                new_turbine_df['月份'] = pd.to_datetime(new_turbine_df['日期']).dt.to_period('M')
                new_turbine_df = date_df.merge(new_turbine_df, on='日期', how='left')
                new_turbine_df['时长'] = new_turbine_df['时长'].fillna(1440)
                new_turbine_df['标签'] = new_turbine_df['标签'].fillna(1000)
                new_turbine_df['机组'] = turbine  # +'【'+type_name+'/'+plat_name+'】'
                new_summary_tba_list.append(new_turbine_df)
            new_turbine_df = new_tt_df[new_tt_df['类型'] == 'PBA']  # PBA
            if len(new_turbine_df) > 0:
                new_turbine_df['机组'] = turbine  # +'【'+type_name+'/'+plat_name+'】'
                new_turbine_df['机型'] = type_name
                new_turbine_df['月份'] = pd.to_datetime(new_turbine_df['日期']).dt.to_period('M')
                if len(new_turbine_df[new_turbine_df.duplicated(subset=['日期', 'time', '标准简称', '机组', '机型', '标签'],
                                                                keep=False)]) > 0:
                    print('PBA重复行如下：')
                    print(new_turbine_df[
                              new_turbine_df.duplicated(subset=['日期', 'time', '标准简称', '机组', '机型', '标签'],
                                                        keep=False)])
                new_summary_pba_list.append(new_turbine_df)
    # new_summary_tba_df TBA的中间表，若有需要可以保存成中间数据，进行后续计算，数据量会比秒级少很多
    if len(new_summary_tba_list) > 0:
        new_summary_tba_df = pd.concat(new_summary_tba_list)
        grid_date = new_summary_tba_df[new_summary_tba_df['标签'] != 1000]['日期'].min()
        new_summary_tba_df = new_summary_tba_df[new_summary_tba_df['日期'] >= grid_date]
        # 有效月份
        months_list = new_summary_tba_df['月份']
    else:
        new_summary_tba_df = pd.DataFrame()
        months_list = []
    # new_summary_tba_df PBA的中间表，若有需要可以保存成中间数据，进行后续计算，数据量会比秒级少很多
    if len(new_summary_pba_list) > 0:
        new_summary_pba_df = pd.concat(new_summary_pba_list)
        # 潜在发电量的计算
        new_summary_pba_df_list = []
        for each_cal_method in new_summary_pba_df['标准简称'].unique():
            each_summary = new_summary_pba_df[new_summary_pba_df['标准简称'] == each_cal_method]
            each_summary['time'] = pd.to_datetime(each_summary['time'])
            each_summary['日期'] = each_summary['time'].dt.date
            # 平均风速对比找到临近机组
            primary_turbines_turple = []
            secondary_turbines_turple = []
            mean_wind_turbines = each_summary.groupby(['机型', '机组'], as_index=False)['风速'].mean()
            for i, row in mean_wind_turbines.iterrows():
                one = row['风速']
                d_turbine = row['机组']
                type_turbine = row['机型']
                df_copy = mean_wind_turbines[
                    (mean_wind_turbines['机组'] != d_turbine) & (mean_wind_turbines['机型'] == type_turbine)]
                if len(df_copy) > 1:
                    diff = abs(df_copy['风速'] - one)
                    nearest_row_indices = diff.argsort()[:2]
                    nearest_rows = df_copy.iloc[nearest_row_indices]
                    primary_turbines_turple.append((d_turbine, nearest_rows['机组'].to_list()[0]))
                    secondary_turbines_turple.append((d_turbine, nearest_rows['机组'].to_list()[1]))
                else:
                    primary_turbines_turple.append((d_turbine, ''))
                    secondary_turbines_turple.append((d_turbine, ''))
            types = [get_inner_type_by_turbine(farm, turbine) for turbine in turbines]
            type_turbine_dict = dict(zip(turbines, types))
            mean_turbines_turple = [
                (one, list(set([key for key, val in type_turbine_dict.items() if val == type_turbine_dict[one]]
                               ) - {one})) for one in turbines]
            print('主次临近机组，及全场同机型机组列表如下：', primary_turbines_turple, secondary_turbines_turple,
                  mean_turbines_turple)
            if each_cal_method == 'sany':
                each_summary = primary_power(each_summary, primary_turbines_turple)
                each_summary = primary_power(each_summary, secondary_turbines_turple)
                each_summary = primary_power(each_summary, mean_turbines_turple)
                each_summary.loc[each_summary['潜在发电量'].isnull(), '潜在发电量'] = each_summary.loc[
                    each_summary['潜在发电量'].isnull(), '应发电量']
                each_summary = primary_power(each_summary, primary_turbines_turple, name='国际潜在发电量')
                each_summary = primary_power(each_summary, secondary_turbines_turple, name='国际潜在发电量')
                each_summary = primary_power(each_summary, mean_turbines_turple, name='国际潜在发电量')
                each_summary.loc[each_summary['国际潜在发电量'].isnull(), '国际潜在发电量'] = each_summary.loc[
                    each_summary['国际潜在发电量'].isnull(), '国际应发电量']
            elif each_cal_method == 'morocco':
                each_summary = primary_power(each_summary, mean_turbines_turple)
                each_summary.loc[each_summary['潜在发电量'].isnull(), '潜在发电量'] = each_summary.loc[
                    each_summary['潜在发电量'].isnull(), '应发电量']
                each_summary = primary_power(each_summary, mean_turbines_turple)
                each_summary = primary_power(each_summary, mean_turbines_turple, name='国际潜在发电量')
                each_summary.loc[each_summary['国际潜在发电量'].isnull(), '国际潜在发电量'] = each_summary.loc[
                    each_summary['国际潜在发电量'].isnull(), '国际应发电量']
            each_summary['损失'] = each_summary['潜在发电量'] - each_summary['实发电量']
            each_summary.loc[each_summary['损失'] < 0, '损失'] = 0
            each_summary['国际损失'] = each_summary['国际潜在发电量'] - each_summary['实发电量']
            each_summary.loc[each_summary['国际损失'] < 0, '国际损失'] = 0
            each_summary = each_summary.groupby(['月份', '日期', '类型', '标准简称', '机组', '标签'], as_index=False).agg(
                {'风速': 'mean', '实发电量': 'sum', '应发电量': 'sum', '秒数': 'sum', '国际应发电量': 'sum',
                 '潜在发电量': 'sum', '国际潜在发电量': 'sum', '损失': 'sum', '国际损失': 'sum'})
            new_summary_pba_df_list.append(each_summary)
        new_summary_pba_df = pd.concat(new_summary_pba_df_list)
    else:
        new_summary_pba_df = pd.DataFrame()

    new_summary_tba_df.rename(columns={"日期":"record_date","标签":"label","时长":"duration","月份":"record_month","机组":"turbine"}, inplace=True)
    # new_summary_tba_df.insert(new_summary_tba_df.shape[1], "farm", farm)
    new_summary_tba_df.insert(new_summary_tba_df.shape[1], "farm", "farm")
    sql_insert_string = generate_clickhouse_insert_from_df(new_summary_tba_df, "db_report.summary_tba")
    print("===================")
    print(sql_insert_string)
    do_clickhouse_request(sql_insert_string)
    print("===================")


    new_summary_pba_df.rename(columns={
        '月份': 'record_month',
        '日期': 'record_date',
        '类型': 'record_type',
        '标准简称': 'standard_abbr',
        '机组': 'turbine',
        '标签': 'label',
        '风速': 'wind_speed',
        '实发电量': 'actual_generation',
        '应发电量': 'theoretical_generation',
        '秒数': 'seconds',
        '国际应发电量': 'intl_theoretical_generation',
        '潜在发电量': 'potential_generation',
        '国际潜在发电量': 'intl_potential_generation',
        '损失': 'loss_amount',
        '国际损失': 'intl_loss_amount'
    }, inplace=True)
    # new_summary_pba_df.insert(new_summary_pba_df.shape[1], "farm", farm)
    new_summary_pba_df.insert(new_summary_pba_df.shape[1], "farm", "farm")
    sql_insert_string = generate_clickhouse_insert_from_df(new_summary_pba_df, "db_report.summary_pba")
    print("===================")
    print(sql_insert_string)
    do_clickhouse_request(sql_insert_string)
    print("===================")



if __name__ == "__main__":
    # 定义命令行参数
    parser = argparse.ArgumentParser(description="Run wind farm data calculation.")
    parser.add_argument("--start-time", required=True, help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--end-time", required=True, help="End date in YYYY-MM-DD format.")
    parser.add_argument("--farm", required=True, help="Farm identifier (e.g., WLTEQ).")
    parser.add_argument("--farm-ch", required=True, help="Farm chinese name.")
    parser.add_argument("--turbines", required=True, nargs='+', help="List of turbine IDs.")
    parser.add_argument("--data-dir", required=True, help="Path to the root data directory.")

    args = parser.parse_args()

    run_calculation(args)