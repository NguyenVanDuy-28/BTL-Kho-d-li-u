-- ============================================================
-- STAR SCHEMA - Online Retail II
-- Grain của FactSales: 1 dòng = 1 dòng sản phẩm trong 1 hóa đơn
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key  INTEGER PRIMARY KEY,
    customer_id   INTEGER NOT NULL UNIQUE          -- mã KH gốc (Customer ID)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key   INTEGER PRIMARY KEY,
    stock_code    VARCHAR(30) NOT NULL UNIQUE,
    description   TEXT
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_key   INTEGER PRIMARY KEY,
    country_name  VARCHAR(60) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER PRIMARY KEY,             -- dạng yyyymmdd
    full_date     DATE NOT NULL UNIQUE,
    day_of_month  SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    month_name    VARCHAR(12) NOT NULL,
    quarter       SMALLINT NOT NULL,
    year          SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,               -- 1 = Thứ Hai
    day_name      VARCHAR(12) NOT NULL,
    is_weekend    BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key         BIGSERIAL PRIMARY KEY,
    invoice_no        VARCHAR(20) NOT NULL,        -- degenerate dimension
    customer_key      INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    product_key       INTEGER NOT NULL REFERENCES dim_product(product_key),
    date_key          INTEGER NOT NULL REFERENCES dim_date(date_key),
    country_key       INTEGER NOT NULL REFERENCES dim_country(country_key),
    invoice_datetime  TIMESTAMP NOT NULL,
    quantity          INTEGER NOT NULL,
    unit_price        NUMERIC(12,4) NOT NULL,
    revenue           NUMERIC(14,4) NOT NULL       -- quantity * unit_price
);
CREATE INDEX IF NOT EXISTS ix_fact_customer ON fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS ix_fact_product  ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS ix_fact_date     ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS ix_fact_country  ON fact_sales(country_key);
CREATE INDEX IF NOT EXISTS ix_fact_invoice  ON fact_sales(invoice_no);

-- ===== Kết quả khai phá dữ liệu (RFM + K-Means) =====
CREATE TABLE IF NOT EXISTS rfm_segments (
    customer_key  INTEGER PRIMARY KEY REFERENCES dim_customer(customer_key),
    customer_id   INTEGER NOT NULL,
    recency       INTEGER NOT NULL,
    frequency     INTEGER NOT NULL,
    monetary      NUMERIC(14,2) NOT NULL,
    cluster_k2    SMALLINT NOT NULL,
    cluster_k4    SMALLINT NOT NULL,
    segment_name  VARCHAR(40) NOT NULL
);

CREATE TABLE IF NOT EXISTS silhouette_scores (
    k      SMALLINT PRIMARY KEY,
    score  NUMERIC(8,4) NOT NULL
);

-- ===== View phục vụ Superset =====
CREATE OR REPLACE VIEW vw_monthly_revenue AS
SELECT make_date(d.year, d.month, 1)      AS month_start,
       SUM(f.revenue)                     AS revenue,
       COUNT(DISTINCT f.invoice_no)       AS orders,
       COUNT(DISTINCT f.customer_key)     AS customers
FROM fact_sales f
JOIN dim_date d USING (date_key)
GROUP BY make_date(d.year, d.month, 1);

CREATE OR REPLACE VIEW vw_segment_summary AS
SELECT segment_name,
       COUNT(*)                                          AS customers,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_customers,
       ROUND(AVG(recency), 1)                            AS avg_recency,
       ROUND(AVG(frequency), 1)                          AS avg_frequency,
       ROUND(AVG(monetary), 1)                           AS avg_monetary
FROM rfm_segments
GROUP BY segment_name;
