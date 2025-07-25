FROM apache/airflow:2.10.5-python3.10

# Copy toàn bộ code project vào image
COPY . /opt/airflow/

# Thiết lập biến môi trường PYTHONPATH
ENV PYTHONPATH="/opt/airflow"

# Cài đặt các thư viện cần thiết
RUN pip install -r /opt/airflow/requirements.txt
