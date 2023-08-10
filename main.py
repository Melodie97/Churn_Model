import pyodbc
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import warnings
import pickle
import time
import churn_module as cm
from datetime import datetime, timedelta
from pandas.tseries.offsets import DateOffset
from dateutil import parser


warnings.filterwarnings("ignore")

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)

# Pipeline class for orchestrating pipeline run and its task dependencies.

from collections import deque

class DAG:
        def __init__(self):
            self.graph = {}
        def add(self, node, to=None):
            if not node in self.graph:
                self.graph[node] = []
            if to:
                if not to in self.graph:
                    self.graph[to] = []
                self.graph[node].append(to)
            if len(self.sort()) != len(self.graph):
                raise Exception

        def in_degrees(self):
            in_degrees = {}
            for node in self.graph:
                if node not in in_degrees:
                    in_degrees[node] = 0
                for pointed in self.graph[node]:
                    if pointed not in in_degrees:
                        in_degrees[pointed] = 0
                    in_degrees[pointed] += 1
            return in_degrees
        
        def sort(self):
            in_degrees = self.in_degrees()
            to_visit = deque()
            for node in self.graph:
                if in_degrees[node] == 0:
                    to_visit.append(node)
            
            searched = []
            while to_visit:
                node = to_visit.popleft()
                for pointer in self.graph[node]:
                    in_degrees[pointer] -= 1
                    if in_degrees[pointer] == 0:
                        to_visit.append(pointer)
                searched.append(node)
            return searched



class Pipeline:
    def __init__(self):
        self.tasks = DAG()
        
    def task(self, depends_on=None):
        def inner(f):
            self.tasks.add(f)
            if depends_on:
                self.tasks.add(depends_on, f)
            return f
        return inner
    
    def run(self):
        scheduled = self.tasks.sort()
        completed = {}
        
        for task in scheduled:
            for node, values in self.tasks.graph.items():
                if task in values:
                    completed[task] = task(completed[node])
            if task not in completed:
                completed[task] = task()
        return completed


pipeline = Pipeline()

init_date = datetime(2023, 9, 3, 22, 59)

with open('models/rf_under_2_1_pkl','rb') as f:
    model = pickle.load(f)

while True:

    init_date = init_date.strftime("%Y/%m/%d %H:%M")
    today_date = datetime.now()
    today_date = today_date.strftime("%Y/%m/%d %H:%M")

    init_date = parser.parse(init_date)
    today_date = parser.parse(today_date)

    if today_date == init_date:

        @pipeline.task()
        def date_extraction():
            year_extracted = init_date.year
            month_extracted = str(init_date.month)
            day_extracted = str(init_date.day)
            return year_extracted, month_extracted, day_extracted

        @pipeline.task(depends_on=date_extraction)
        def fetch_inference_data(calender):
            year = calender[0]
            month = calender[1]
            day = calender[2]
            conn_digital = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=XXXX;DATABASE=XXXX;UID=XXXX;PWD=XXXX')
            cursor = conn_digital.cursor()

            print("Database logged in")
            print(year, month, day)

            sql_query = f"""
                select *
                from customer_churn_by_customer_pred
                where customer_id is not null
            """

            df = pd.read_sql(sql_query, con=conn_digital)
            conn_digital.close()
            data = df
            return data

        @pipeline.task(depends_on=fetch_inference_data)
        def transform(data_extract):
            start = time.time()

            df, df_copy = cm.transform(data_extract)

            end = time.time()
            print('transformation time(mins):', (end - start)*0.0166667)

            return df, df_copy


        @pipeline.task(depends_on=transform)
        def predict(data):

            df = data[0]
            df_temp = data[1]

            start = time.time()

            pred = model.predict(df)

            end = time.time()
            print('Prediction for month complete..')
            print('prediction time(mins):', (end - start)*0.0166667)

            pred_df = pd.DataFrame(pred, index=df_temp.index, columns=['pred'])
            df_temp = pd.concat([df_temp, pred_df], axis=1)

            df_temp['pred'] = df_temp.pred.astype(int)

            return df_temp
            
        @pipeline.task(depends_on=predict)
        def load(inference_data):
            
            conn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=XXXX;DATABASE=XXXX;UID=XXXX;PWD=XXXX')
            cursor = conn.cursor()

            print("Load Database logged in")

            df_database = inference_data
            df_database = df_database[['customer_id', 'pred']]
            drop_table = """TRUNCATE TABLE basic_churn_prediction"""
            cursor.execute(drop_table)

            MY_TABLE = 'basic_churn_prediction'

            insert_to_tmp_tbl_stmt = f"INSERT INTO {MY_TABLE} VALUES (?,?)"
            cursor = conn.cursor()
            cursor.fast_executemany = True

            cursor.executemany(insert_to_tmp_tbl_stmt, df_database.values.tolist())
            cursor.commit()

            drop_visualization_table = """drop table if exists customer_churn_data_pred"""
            update_visualization = """
                select a.customer_id, isnull(customer_segment, 'Others') customer_segment, 
                isnull(datediff(year, date_of_birth, getdate()), -999) age, 
                isnull(fn_get_generation(year(date_of_birth)), 'Unknown') generation, 
                customer_status, region_name, 
                isnull(customer_occupation, 'OTHERS') customer_occupation, 
                isnull(sex, 'O') sex, 
                datediff(year, customer_opening_date, getdate()) years_with_bank,  datediff(month, customer_opening_date,
                getdate()) months_with_bank, 
                datediff(day, customer_opening_date, getdate()) days_with_bank, 
                isnull(account_officer_name, 'Unknown') account_officer_name, branch_name,
                zone_name,	
                case when a.monthly_churn = 1 then 'churn' else 'non-churn' end as monthly_churn
                into customer_churn_data_pred
                from basic_churn_prediction a
                join customer_accounts_table b
                on a.customer_id = b.customer_id
                left join customer_segmentation_table c
                on a.customer_id = c.customer_id
            """
            cursor.execute(drop_visualization_table)
            cursor.execute(update_visualization)
            cursor.commit()
            print('visualization data uploaded')

            cursor.close()
            conn.close()

            return f'{len(df_database)} rows inserted to the {MY_TABLE} table'

        test = pipeline.run()
        print(test[load])

        init_date = init_date + DateOffset(months=1)
        print(init_date)
