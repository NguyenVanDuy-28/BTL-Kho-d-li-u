FROM apache/airflow:2.9.3

# Cài thư viện bằng bộ ràng buộc chính thức của Airflow để numpy/pandas luôn tương thích
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt" \
    openpyxl scikit-learn pyarrow