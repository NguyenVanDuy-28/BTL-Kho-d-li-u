"""Hàm ETL cho Online Retail II: extract -> transform -> load -> RFM/K-Means -> validate."""
import hashlib
import io
import json
import logging
import os
import shutil
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("ETL_DATA_DIR", "/opt/airflow/data"))
SQL_DIR = Path(os.environ.get("ETL_SQL_DIR", "/opt/airflow/sql"))
RAW_DIR = DATA_DIR / "raw"                  # vùng tiếp nhận (landing): nơi đặt file nguồn
LAKE = DATA_DIR / "lake"                    # DATA LAKE
LAKE_RAW = LAKE / "raw"                     #   tầng thô: giữ nguyên định dạng, chia theo nguồn/ngày nạp
LAKE_CLEANED = LAKE / "cleaned"             #   tầng đã làm sạch
SOURCE, DATASET = "kaggle", "online_retail_ii"
LANDING_FILES = ["online_retail_II.csv", "online_retail_II.xlsx"]
STAGING = DATA_DIR / "staging"              # file tạm giữa các task
RAW_PARQUET = STAGING / "raw.parquet"
CLEAN_DIR = LAKE_CLEANED / DATASET
CLEAN_PARQUET = CLEAN_DIR / "clean.parquet"

# Số liệu đã có trong báo cáo tiến độ, dùng để đối chiếu (chỉ cảnh báo)
EXPECTED_CLEAN_ROWS = 776_857
EXPECTED_CUSTOMERS = 5_853

COLUMN_MAP = {
    "Invoice": "invoice_no", "StockCode": "stock_code", "Description": "description",
    "Quantity": "quantity", "InvoiceDate": "invoice_datetime", "Price": "unit_price",
    "Customer ID": "customer_id", "Country": "country",
}
# Mã không phải sản phẩm thật (bưu phí, giảm giá, phí ngân hàng, ...)
NON_PRODUCT_RE = r"^(POST|DOT|D|M|BANK CHARGES|ADJUST\d*|S|B|AMAZONFEE|CRUK|GIFT.*|DCGS.*)$"

SEGMENT_VIP = "VIP"
SEGMENT_LOYAL = "Trung thành"
SEGMENT_NEW = "Mới / ít tương tác"
SEGMENT_RISK = "Có nguy cơ rời bỏ"


# ---------------------------------------------------------------- DB helpers
@contextmanager
def connect():
    import psycopg2  # import muộn để test được phần pandas khi không có psycopg2
    conn = psycopg2.connect(os.environ["WAREHOUSE_DSN"])
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute(sql):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql)


def query_df(sql):
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def copy_df(df, table):
    """Nạp DataFrame vào bảng bằng COPY (nhanh hơn INSERT nhiều lần)."""
    cols = ", ".join(df.columns)
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    with connect() as conn, conn.cursor() as cur:
        cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv)", buf)
    log.info("Đã nạp %s dòng vào %s", f"{len(df):,}", table)


# ------------------------------------------------------------------- tasks
def create_schema():
    execute((SQL_DIR / "01_schema.sql").read_text(encoding="utf-8"))


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def latest_raw_dir():
    parts = sorted((LAKE_RAW / SOURCE / DATASET).glob("ingest_date=*"))
    if not parts:
        raise FileNotFoundError(f"Data Lake chưa có dữ liệu thô tại {LAKE_RAW / SOURCE / DATASET}")
    return parts[-1]


def ingest_raw():
    """Nạp dữ liệu nguồn vào Data Lake (tầng raw), giữ nguyên file và ghi manifest."""
    found = [RAW_DIR / n for n in LANDING_FILES if (RAW_DIR / n).exists()]
    if not found:
        raise FileNotFoundError(f"Không thấy {' hoặc '.join(LANDING_FILES)} trong {RAW_DIR}")
    now = datetime.now(timezone.utc)
    dest = LAKE_RAW / SOURCE / DATASET / f"ingest_date={now.date().isoformat()}"
    dest.mkdir(parents=True, exist_ok=True)
    files = []
    for p in found:
        target = dest / p.name
        shutil.copy2(p, target)
        files.append({"file": p.name, "size_bytes": target.stat().st_size,
                      "sha256": _sha256(target)})
    manifest = {"source": SOURCE, "source_type": "file", "dataset": DATASET,
                "ingested_at_utc": now.isoformat(timespec="seconds"), "files": files}
    (dest / "_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2),
                                         encoding="utf-8")
    log.info("Đã nạp %d file vào %s", len(files), dest)
    return str(dest)


def extract():
    """Đọc dữ liệu thô từ Data Lake (phân vùng mới nhất) và lưu bản staging."""
    STAGING.mkdir(parents=True, exist_ok=True)
    src_dir = latest_raw_dir()
    csv_path = src_dir / "online_retail_II.csv"
    xlsx_path = src_dir / "online_retail_II.xlsx"
    dtypes = {"Invoice": str, "StockCode": str}
    if csv_path.exists():
        df = pd.read_csv(csv_path, dtype=dtypes, parse_dates=["InvoiceDate"],
                         encoding_errors="replace")
    elif xlsx_path.exists():
        sheets = pd.read_excel(xlsx_path, sheet_name=None, dtype=dtypes)  # chậm: vài phút
        df = pd.concat(sheets.values(), ignore_index=True)
    else:
        raise FileNotFoundError(f"Không thấy online_retail_II.xlsx/.csv trong {src_dir}")
    df.to_parquet(RAW_PARQUET, index=False)
    log.info("Extract: %s dòng", f"{len(df):,}")
    return len(df)


def transform():
    """Làm sạch theo đúng thứ tự trong báo cáo tiến độ."""
    df = pd.read_parquet(RAW_PARQUET).rename(columns=COLUMN_MAP)
    steps = [("Dữ liệu thô", len(df))]

    df = df.drop_duplicates()
    steps.append(("Bỏ trùng lặp", len(df)))

    df = df.dropna(subset=["customer_id"])
    steps.append(("Bỏ thiếu Customer ID", len(df)))

    df = df[~df["invoice_no"].astype(str).str.upper().str.startswith("C")]
    steps.append(("Bỏ đơn hủy", len(df)))

    df = df[(df["quantity"] > 0) & (df["unit_price"] > 0)]
    steps.append(("Bỏ Quantity/Price <= 0", len(df)))

    code = df["stock_code"].astype(str).str.strip().str.upper()
    df = df[~code.str.match(NON_PRODUCT_RE)]
    steps.append(("Bỏ mã phí/điều chỉnh", len(df)))

    df = df.copy()
    df["customer_id"] = df["customer_id"].astype(int)
    df["stock_code"] = df["stock_code"].astype(str).str.strip()
    df["description"] = df["description"].astype("string").str.strip()
    df["invoice_datetime"] = pd.to_datetime(df["invoice_datetime"])
    df["country"] = df["country"].astype(str).str.strip()
    df["quantity"] = df["quantity"].astype(int)
    df["revenue"] = (df["quantity"] * df["unit_price"]).round(4)

    for name, n in steps:
        log.info("%-25s %10s dòng", name, f"{n:,}")
    if len(df) != EXPECTED_CLEAN_ROWS:
        log.warning("Số dòng sạch %s khác báo cáo (%s) - hãy đối chiếu với 02_clean_data.py",
                    f"{len(df):,}", f"{EXPECTED_CLEAN_ROWS:,}")
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(CLEAN_PARQUET, index=False)
    (CLEAN_DIR / "_metadata.json").write_text(json.dumps({
        "dataset": DATASET, "source_partition": latest_raw_dir().name,
        "cleaned_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "steps": [{"step": n, "rows": int(r)} for n, r in steps]},
        ensure_ascii=False, indent=2), encoding="utf-8")
    return len(df)


def build_dims(clean):
    """Tạo 4 bảng chiều (khóa thay thế gán xác định, nên chạy lại vẫn ra cùng kết quả)."""
    countries = sorted(clean["country"].unique())
    dim_country = pd.DataFrame({"country_key": range(1, len(countries) + 1),
                                "country_name": countries})

    customers = sorted(clean["customer_id"].unique())
    dim_customer = pd.DataFrame({"customer_key": range(1, len(customers) + 1),
                                 "customer_id": customers})

    def mode_desc(s):
        s = s.dropna()
        return s.mode().iat[0] if not s.empty else None
    prod = (clean.groupby("stock_code")["description"].agg(mode_desc)
            .reset_index().sort_values("stock_code"))
    prod.insert(0, "product_key", range(1, len(prod) + 1))
    dim_product = prod[["product_key", "stock_code", "description"]]

    days = pd.date_range(clean["invoice_datetime"].min().normalize(),
                         clean["invoice_datetime"].max().normalize(), freq="D")
    dim_date = pd.DataFrame({
        "date_key": days.strftime("%Y%m%d").astype(int),
        "full_date": days.date,
        "day_of_month": days.day,
        "month": days.month,
        "month_name": days.strftime("%B"),
        "quarter": days.quarter,
        "year": days.year,
        "day_of_week": days.dayofweek + 1,
        "day_name": days.strftime("%A"),
        "is_weekend": days.dayofweek >= 5,
    })
    return {"dim_country": dim_country, "dim_customer": dim_customer,
            "dim_product": dim_product, "dim_date": dim_date}


def load_dimensions():
    clean = pd.read_parquet(CLEAN_PARQUET)
    dims = build_dims(clean)
    # Làm mới toàn bộ kho -> chạy lại DAG nhiều lần không bị trùng dữ liệu
    execute("TRUNCATE fact_sales, rfm_segments, silhouette_scores, "
            "dim_customer, dim_product, dim_date, dim_country RESTART IDENTITY;")
    for name, df in dims.items():
        copy_df(df, name)


def build_fact(clean, dims):
    cust = dict(zip(dims["dim_customer"]["customer_id"], dims["dim_customer"]["customer_key"]))
    prod = dict(zip(dims["dim_product"]["stock_code"], dims["dim_product"]["product_key"]))
    ctry = dict(zip(dims["dim_country"]["country_name"], dims["dim_country"]["country_key"]))
    fact = pd.DataFrame({
        "invoice_no": clean["invoice_no"].astype(str),
        "customer_key": clean["customer_id"].map(cust),
        "product_key": clean["stock_code"].map(prod),
        "date_key": clean["invoice_datetime"].dt.strftime("%Y%m%d").astype(int),
        "country_key": clean["country"].map(ctry),
        "invoice_datetime": clean["invoice_datetime"],
        "quantity": clean["quantity"],
        "unit_price": clean["unit_price"],
        "revenue": clean["revenue"],
    })
    if fact[["customer_key", "product_key", "country_key"]].isna().any().any():
        raise ValueError("Có khóa thay thế bị NULL khi map sang bảng chiều")
    return fact


def load_fact():
    clean = pd.read_parquet(CLEAN_PARQUET)
    copy_df(build_fact(clean, build_dims(clean)), "fact_sales")


def rfm_kmeans():
    """Tính RFM từ FactSales, thử K=2..10 (Silhouette), gán nhãn cụm K=2 và K=4."""
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    from sklearn.preprocessing import StandardScaler

    rfm = query_df("""
        WITH ref AS (SELECT MAX(invoice_datetime)::date + 1 AS d FROM fact_sales)
        SELECT f.customer_key, c.customer_id,
               ((SELECT d FROM ref) - MAX(f.invoice_datetime)::date)::int AS recency,
               COUNT(DISTINCT f.invoice_no)::int AS frequency,
               SUM(f.revenue)::float AS monetary
        FROM fact_sales f JOIN dim_customer c USING (customer_key)
        GROUP BY f.customer_key, c.customer_id
    """)
    X = np.column_stack([rfm["recency"], np.log1p(rfm["frequency"]), np.log1p(rfm["monetary"])])
    Xs = StandardScaler().fit_transform(X)

    scores = []
    for k in range(2, 11):
        labels = KMeans(n_clusters=k, n_init=10, random_state=42).fit_predict(Xs)
        scores.append((k, round(float(silhouette_score(Xs, labels)), 4)))
        log.info("K=%d  silhouette=%.4f", k, scores[-1][1])

    rfm["cluster_k2"] = KMeans(n_clusters=2, n_init=10, random_state=42).fit_predict(Xs)
    rfm["cluster_k4"] = KMeans(n_clusters=4, n_init=10, random_state=42).fit_predict(Xs)
    rfm["segment_name"] = name_segments(rfm)

    execute("TRUNCATE rfm_segments, silhouette_scores;")
    copy_df(rfm[["customer_key", "customer_id", "recency", "frequency", "monetary",
                 "cluster_k2", "cluster_k4", "segment_name"]].round({"monetary": 2}),
            "rfm_segments")
    copy_df(pd.DataFrame(scores, columns=["k", "score"]), "silhouette_scores")


def name_segments(rfm):
    """Đặt tên cụm K=4 theo đặc điểm trung bình (nhãn do nhóm diễn giải, không có sẵn trong dữ liệu)."""
    prof = rfm.groupby("cluster_k4").agg(R=("recency", "mean"), F=("frequency", "mean"),
                                         M=("monetary", "mean"))
    names = {}
    vip = prof["M"].idxmax(); names[vip] = SEGMENT_VIP
    rest = prof.drop(vip)
    risk = rest["R"].idxmax(); names[risk] = SEGMENT_RISK
    rest = rest.drop(risk)
    loyal = rest["F"].idxmax(); names[loyal] = SEGMENT_LOYAL
    names[rest.drop(loyal).index[0]] = SEGMENT_NEW
    log.info("Hồ sơ cụm K=4:\n%s\nTên cụm: %s", prof.round(1), names)
    return rfm["cluster_k4"].map(names)


def validate():
    clean = pd.read_parquet(CLEAN_PARQUET)
    q = lambda sql: query_df(sql).iat[0, 0]
    n_fact = int(q("SELECT COUNT(*) FROM fact_sales"))
    rev_fact = float(q("SELECT COALESCE(SUM(revenue),0)::float FROM fact_sales"))
    n_cust = int(q("SELECT COUNT(*) FROM dim_customer"))
    n_rfm = int(q("SELECT COUNT(*) FROM rfm_segments"))
    errors = []
    if n_fact != len(clean):
        errors.append(f"Số dòng fact {n_fact:,} != staging {len(clean):,}")
    if abs(rev_fact - float(clean["revenue"].sum())) > 0.5:
        errors.append(f"Tổng doanh thu fact {rev_fact:,.2f} != staging {clean['revenue'].sum():,.2f}")
    if n_cust != clean["customer_id"].nunique() or n_rfm != n_cust:
        errors.append(f"Số khách hàng lệch: dim={n_cust}, rfm={n_rfm}")
    if errors:
        raise ValueError("Validate thất bại: " + "; ".join(errors))
    if n_cust != EXPECTED_CUSTOMERS:
        log.warning("Số KH %d khác báo cáo (%d)", n_cust, EXPECTED_CUSTOMERS)
    log.info("OK - fact=%s dòng, doanh thu=%s, khách hàng=%s",
             f"{n_fact:,}", f"{rev_fact:,.2f}", f"{n_cust:,}")
