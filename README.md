# Retail Data Warehouse – Airflow + PostgreSQL Star Schema

## 0. Chuẩn bị (làm 1 lần)
1. Cài **Docker Desktop** (Windows: bật WSL2), cần trống khoảng 6–8 GB RAM cho Docker.
2. Tải `online_retail_II.xlsx` (hoặc `.csv`) từ Kaggle, đặt vào `data/raw/`.
3. (Khuyến nghị) Đổi xlsx sang csv để bước extract nhanh hơn (đọc xlsx ~ vài phút).
4. Linux/macOS: chạy `echo "AIRFLOW_UID=$(id -u)" > .env`.

## 1. Chạy hệ thống
```bash
docker compose up -d          # lần đầu mất vài phút (kéo image + cài thư viện)
```
- Airflow UI: http://localhost:8080 (admin / admin)
- Warehouse: host `localhost`, port `5433`, db `retail_dw`, user/pass `warehouse` / `warehouse`
  (trong Docker network: `warehouse-db:5432`)

Trên Airflow UI: bật DAG `retail_dw_etl` → bấm **Trigger DAG**.

Thứ tự task: `create_schema → extract → transform → load_dimensions → load_fact_sales → rfm_kmeans → validate`

## 2. Kiểm tra kết quả (pgAdmin / psql)
```sql
SELECT COUNT(*) FROM fact_sales;                 -- ~776,857 nếu khớp báo cáo
SELECT COUNT(*) FROM dim_customer;               -- ~5,853
SELECT * FROM vw_segment_summary;                -- số KH, % từng nhóm, R/F/M trung bình
SELECT * FROM silhouette_scores ORDER BY k;
SELECT * FROM vw_monthly_revenue ORDER BY 1;
```

## 3. Kết nối Superset (bước sau)
Database URI: `postgresql+psycopg2://warehouse:warehouse@<host>:5433/retail_dw`
(nếu Superset chạy chung Docker network: host = `warehouse-db`, port `5432`).
Dataset gợi ý: `vw_monthly_revenue`, `vw_segment_summary`, `rfm_segments`.

## Lưu ý
- Mỗi lần chạy DAG là **full refresh** (TRUNCATE rồi nạp lại) nên chạy lại không bị trùng.
- Hàm `transform` mô phỏng lại các bước trong báo cáo tiến độ. Nếu số dòng sạch khác 776,857,
  hãy đối chiếu với `scripts/02_clean_data.py` của nhóm và sửa `NON_PRODUCT_RE` / các bước lọc cho khớp.
- Tên cụm (VIP, Trung thành, ...) được gán tự động theo đặc điểm trung bình R/F/M của cụm.
- Dừng hệ thống: `docker compose down` (thêm `-v` để xóa cả dữ liệu).
