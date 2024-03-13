from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from amadeus import Client, ResponseError
import pandas as pd
import boto3
from io import StringIO
from airflow.hooks.base_hook import BaseHook
import time

aws_conn = BaseHook.get_connection("aws_conn_id")
aws_access_key_id = aws_conn.login
aws_secret_access_key = aws_conn.password

# Amadeus 클라이언트 생성
amadeus = Client(
    client_id='cjr9tknVO9UTkwsQhAGvg63aj6cL3Ob6',
    client_secret='XAAVmXtT9DH4yCtH'
)


# 일본 공항 리스트
airport = ['HND','NRT','KIX','FUK','NGO','CTS','OKA']

# 현재 연월일 받아오기
date_list = []
api_date = date.today() + timedelta(days=5)
dag_date = str(api_date)
for i in range(1,11):
    date_list.append(str(api_date + timedelta(days=i)))

# S3 연결 정보
s3_bucket = 'de-6-2-bucket'
s3_key = 'round_trip/'+dag_date+'.csv' # 현재 날짜에서 5일 뒤로 수정

#s3_key = 'round_trip/'+dag_date+'day2+'.csv' # 현재 날짜에서 5일 뒤로 수정



# DAG 기본 인수
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 21, 14, 30),  # 시작 시간을 오전 1시로 설정
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Amadeus API를 호출하여 데이터를 가져오는 함수
def fetch_flight_offers(airport, dag_date, date_list):
    response_list = []
    for port in airport:
        for dt in date_list:
            try:
                response = amadeus.shopping.flight_offers_search.get(
                    originLocationCode='ICN',
                    destinationLocationCode=port,
                    departureDate=dag_date,
                    returnDate=dt,
                    adults=1)
                response_list += response.data
                time.sleep(10)
            except ResponseError as error:
                print(error)
                return []
    return response_list

# 데이터 전처리 함수
def preprocess_data(data):
    round_df = pd.json_normalize(data)
    itineraries_df = round_df['itineraries']
    travelerPricings_df = round_df['travelerPricings']
    fees_df = round_df['price.fees']

    # fees 전처리
    data_list = []
    for i in range(len(fees_df)):
        data = fees_df[i]
        new_data = {f"{d['type']}_amount": d['amount'] for d in data}
        data_list.append(new_data)
    fees_df = pd.DataFrame(data_list)

    # itineraries 전처리
    def process_go_itinerary(df):
        data_list = []
        for i in range(len(df)):
            data = df[i][0]['segments'][0]
            new_data = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        new_key = f"{key}_{sub_key}"
                        new_data[new_key] = sub_value
                else:
                    new_data[key] = value
            data_list.append(new_data)
        return pd.DataFrame(data_list)
    
    def process_come_itinerary(df):
        data_list = []
        for i in range(len(df)):
            data = df[i][0]['segments'][0]
            new_data = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        new_key = f"{key}_{sub_key}"
                        new_data[new_key] = sub_value
                else:
                    new_data[key] = value
            data_list.append(new_data)
        return pd.DataFrame(data_list)

    iti_go_df = process_go_itinerary(itineraries_df)
    iti_go_df.columns = ['go_' + col for col in iti_go_df.columns]

    iti_come_df = process_come_itinerary(itineraries_df)
    iti_come_df.columns = ['come_' + col for col in iti_come_df.columns]

    # travelerPricings 전처리
    def process_traveler_pricing(df):
        data_list = []
        for i in range(len(df)):
            data = df[i][0]
            if 'price' in data:
                del data['price']
            new_data = {}
            for key, value in data.items():
                if isinstance(value, list):
                    for i, sub_data in enumerate(value):
                        for sub_key, sub_value in sub_data.items():
                            if isinstance(sub_value, dict):
                                for sub_sub_key, sub_sub_value in sub_value.items():
                                    if i == 0:
                                        new_key = f"go_{sub_key}_{sub_sub_key}"
                                        new_data[new_key] = sub_sub_value
                                    else:
                                        new_key = f"come_{sub_key}_{sub_sub_key}"
                                        new_data[new_key] = sub_sub_value
                            else:
                                if i == 0:
                                    new_key = f"go_{sub_key}"
                                else:
                                    new_key = f"come_{sub_key}"
                                new_data[new_key] = sub_value
                else:
                    new_data[key] = value
            data_list.append(new_data)
        return pd.DataFrame(data_list)

    travelerPricings_df = process_traveler_pricing(travelerPricings_df)
    travelerPricings_df.drop(columns=['go_amenities', 'come_amenities'], inplace=True)

    # 기존 데이터프레임에서 필요없는 열 삭제
    round_df.drop(columns=['itineraries', 'travelerPricings', 'price.fees'], inplace=True)

    # 데이터 병합
    round_df = pd.concat([round_df, fees_df, iti_go_df, iti_come_df, travelerPricings_df], axis=1)
    round_df = round_df.to_dict()
    round_df = json.dumps(round_df)
    return round_df

# S3에 저장하는 함수
def save_to_s3(data, s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key):
    result_all = json.loads(data)
    result_all = pd.DataFrame(result_all)
    s3 = boto3.client('s3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)
    s3.put_object(Body=result_all.to_csv(), Bucket=s3_bucket, Key=s3_key)


# Airflow DAG에 태스크 추가
with DAG('round_api_day4', default_args=default_args, description='Fetch flight offers from Amadeus API and save to S3', schedule_interval=timedelta(days=1)) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_flight_offers',
        python_callable=fetch_flight_offers,
        op_kwargs={'airport': airport, 'dag_date': dag_date, 'date_list': date_list},
    )

    preprocess_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
        op_args=[fetch_task.output],
    )

    save_task = PythonOperator(
        task_id='save_to_s3',
        python_callable=save_to_s3,
        op_args=[preprocess_task.output , s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key],
    )
fetch_task >> preprocess_task >> save_task
