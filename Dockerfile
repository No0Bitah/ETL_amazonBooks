FROM apache/airflow:3.0.0

WORKDIR /code

COPY requirements.txt  /code/
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

COPY . /code/