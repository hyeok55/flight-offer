# flight-offer
DEV project
해당 DAG는 DEVCourse project에서 작성한 DAG입니다.

## DATA
Amadeous flight offer API
one - 편도 항공권
round - 왕복 항공권

## Pipeline
<img width="861" alt="image" src="https://github.com/hyeok55/flight-offer/assets/67605795/e37be87a-ce9d-47d1-aa5c-3e60564efb93">

## 개선할 점 및 계획
1. 이제 맛보기로 airflow를 배운 느낌이다.
2. airflow 내에서 데이터를 전처리하니 파이썬의 의존성이 너무 강하다
3. airflow를 단지 하나의 스케쥴러로만 생각하고 glue로 spark job을 하는 것으로 하면 어떨까 싶다.
4. full refresh로 진행했지만, airflow의 execution date를 이용해서 incremental update로 적재하면 어떻까 싶다.
5. 자동 트리거가 될 수 있도록 다양한 방법을 찾아보자
6. redshift의 데이터의 멱등성을 잘 지켰는지, 확인해보아야한다.
   
