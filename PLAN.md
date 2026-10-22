# PLAN.md — E-commerce CDC Lakehouse Project

> **Mục tiêu cuối cùng:** Một project Data Engineer hoàn chỉnh chạy end-to-end, bỏ được vào CV/GitHub và demo được trong phỏng vấn:
> PostgreSQL OLTP (local, sau đó Amazon RDS) → CDC → Amazon S3 Bronze → Databricks trên AWS xử lý Silver/Gold bằng Delta Lake và dbt-databricks (SCD Type 2, Incremental) → dashboard, điều phối bằng Airflow, kiểm tra chất lượng bằng dbt tests + Great Expectations.
>
> **Quy tắc làm việc:** Làm xong MVP end-to-end ở scale nhỏ trước (100k orders), rồi mới scale lớn và nâng cấp CDC. Không làm giai đoạn sau khi giai đoạn trước chưa đạt tiêu chí "Done" của nó. Mỗi giai đoạn xong phải tick checklist, commit Git và ghi lại kết quả vào README.

- **Niche:** E-commerce / Retail
- **Stack chính:** Docker, PostgreSQL/Amazon RDS, Python, Amazon S3, Databricks trên AWS, Spark, Delta Lake, dbt-databricks, Airflow; công cụ dashboard cần chốt trước Giai đoạn 7
- **Region:** S3/RDS ở `ap-southeast-1`, Databricks Free Edition ở `us-east-2`; chấp nhận kết nối liên Region cho demo, theo dõi phí truyền dữ liệu và độ trễ. Không lưu account ID, credential hoặc token trong repo.
- **Scale mục tiêu:** 500k customers · 100k products · 10M orders · 25–35M order_items · 15–30M clickstream events
- **Thời gian ước tính:** 18–22 ngày, 3–4 giờ/ngày. Nếu chỉ có 2 giờ/ngày: 28–35 ngày

---

## Tổng quan các giai đoạn

| Giai đoạn | Tên | Thời gian | Kết quả chính |
|---|---|---|---|
| 0 | Chuẩn bị & Setup | Ngày 1 | Repo, Docker local và truy cập AWS được kiểm chứng |
| 1 | OLTP: Nạp dữ liệu thật + Gen phần mới | Ngày 2–4 | Postgres nạp từ Olist thật (initial snapshot) + simulator ngày mới gen trên nền dữ liệu thật |
| 2 | Ingestion & CDC Phase 1 → Bronze | Ngày 5–7 | Dữ liệu raw lên S3 đúng prefix/partition, có watermark |
| 3 | Silver — Clean, Dedup, MERGE CDC | Ngày 8–10 | Bảng Delta Silver sạch, idempotent, xử lý được update/delete khi nguồn cung cấp sự kiện tương ứng |
| 4 | dbt Core — Staging, Snapshot SCD2, Facts/Dims | Ngày 11–14 | Star schema Gold chạy incremental |
| 5 | Data Marts & Business Logic | Ngày 15–16 | RFM, Funnel, Product, Daily Sales |
| 6 | Orchestration & Data Quality | Ngày 17–18 | Airflow DAG tự chạy, GE checks, alert khi fail |
| 7 | Dashboard & Kiểm chứng số liệu | Ngày 19–20 | Dashboard 4 trang, số khớp với OLTP |
| 8 | Nâng cấp & Đóng gói Portfolio | Ngày 21–22+ (tuỳ chọn) | Debezium/Kafka, README, demo, CV |

---

## Giai đoạn 0 — Chuẩn bị & Setup (Ngày 1)

**Mục tiêu:** Môi trường chạy được từ một lệnh, không cài thủ công rời rạc.

### Việc cần làm
- [x] Dùng repo GitHub hiện có `datnndd/E-commerce-CDC`, cấu trúc thư mục:
  ```
  E-commerce-CDC/
  ├── compose.yaml              # Postgres local, Airflow local
  ├── .env.example              # mẫu biến môi trường, KHÔNG chứa key thật
  ├── Makefile                  # up / down / test; thêm seed / run ở giai đoạn tương ứng
  ├── ingestion/                # script CDC, API, logs
  ├── scripts/                  # seed_oltp.py, simulate_new_day.py
  ├── spark_jobs/               # job Silver (giai đoạn 3)
  ├── dbt_project/              # dbt (giai đoạn 4-5)
  ├── airflow/dags/
  ├── great_expectations/
  ├── docs/                     # workflow, plan và tài liệu kiến trúc khi triển khai
  └── README.md
  ```
- [x] Xác nhận AWS Console/CloudShell ở `ap-southeast-1` và billing bằng USD. AWS account đã có budget $1/tháng với email alert, thấp hơn mục tiêu khoảng 200.000đ; cảnh báo không tự dừng chi phí. Databricks Free Edition hiện không tính phí, có quota sử dụng riêng.
- [x] Tạo S3 bucket riêng cho project ở `ap-southeast-1`, bật Block Public Access, tắt ACL và dùng mã hoá mặc định SSE-S3. Dữ liệu Bronze sẽ dùng prefix `bronze/` khi ingestion chạy.
- [x] Chọn S3 bucket làm nơi lưu Bronze, và dùng Unity Catalog external location cho các prefix `silver/`, `gold/` khi tạo bảng Delta. Đặt lifecycle/xoá dữ liệu demo có chủ đích trước khi scale.
- [x] Tạo IAM role chỉ cho bucket project, trust Unity Catalog bằng external ID và self-assume; tạo storage credential và external location riêng. Không tải access key dài hạn vào repo hay `.env`. CLI local/credential tạm thời sẽ cấu hình trước Giai đoạn 2.
- [x] Xác nhận Databricks Free Edition metastore ở `us-east-2`, SQL warehouse và external location tới S3 `ap-southeast-1` hoạt động. Người dùng chọn giữ AWS ở Singapore và chấp nhận kết nối liên Region. Free Edition chỉ có serverless và không hỗ trợ custom workspace storage location; external location riêng đã được xác minh. RDS PostgreSQL được tạo ở Giai đoạn 1 khi DDL và lựa chọn VPC/chi phí đã rõ.
- [x] Viết `compose.yaml` cho Postgres 15 local (đặt `wal_level=logical`) + Airflow local.
- [x] Cài và kiểm tra Python 3.11, dbt-databricks, pyspark, faker; dùng AWS CLI có sẵn trong CloudShell cho kiểm tra giai đoạn 0. Cấu hình CLI local trước khi viết ingestion ở Giai đoạn 2.

### Done khi
- [x] `docker compose up -d` chạy được Postgres + Airflow, vào được Airflow UI
- [x] Ghi/đọc metadata/xoá 1 file test trong S3 bằng AWS CLI CloudShell; budget đã có trước khi tạo bucket.
- [x] Chạy `SELECT 1` trên Databricks SQL warehouse; kết quả là `1`, sau đó dừng warehouse.
- [x] Kiểm tra external location tới S3 project: các phép `READ`, `LIST`, `WRITE`, `DELETE` đều `PASS`; file events tắt vì chưa dùng ở giai đoạn này.
- [ ] Repo đã push commit đầu tiên, `.gitignore` đã chặn `.env`, key JSON, `target/`, `dbt_packages/`

### Rủi ro thường gặp
- Lộ AWS access key lên GitHub → vô hiệu hoá/thu hồi key ngay, kiểm tra quyền và xoá key khỏi lịch sử Git; ưu tiên credential tạm thời để tránh tình huống này.
- Quên budget hoặc để RDS/Databricks chạy ngoài giờ demo → tài nguyên tiếp tục phát sinh chi phí dù đã gửi cảnh báo.

---

## Giai đoạn 1 — OLTP: Nạp dữ liệu thật trước, gen phần mới sau (Ngày 2–4)

**Mục tiêu:** Có một "hệ thống bán hàng thật" để CDC bắt dữ liệu từ đó. Cách làm: **hybrid** — nạp dataset có sẵn làm dữ liệu lịch sử (initial snapshot), rồi mới gen thêm thay đổi mới dựa trên chính khách hàng/sản phẩm thật đó. Đây cũng đúng là vòng đời CDC ngoài đời thật: snapshot/backfill toàn bộ trước, sau đó mới chạy incremental CDC.

**Môi trường:** Phát triển và kiểm chứng DDL/seed trên Postgres Docker trước. Trước khi tạo Amazon RDS for PostgreSQL, chốt Region, VPC/private access, cỡ instance, lưu trữ, backup và thời gian bật để kiểm soát chi phí; sau đó nạp cùng dữ liệu vào RDS để trình diễn luồng cloud. Không public database chỉ để tiện kết nối.

**Vì sao không tự gen toàn bộ bằng Faker ngay từ đầu?**
- Tốn 2–3 ngày chỉ để chỉnh cho dữ liệu trông không giả, mà phân phối mua hàng, địa lý, category, nội dung review vẫn không thật bằng dữ liệu thật.
- Tự gen từ đầu chỉ còn cần thiết ở 2 chỗ dữ liệu thật không có: clickstream logs và các thay đổi *sau* thời điểm snapshot (đơn mới, đổi giá, đổi địa chỉ) — đó mới là phần gen có giá trị, vì nó tạo ra sự kiện cho CDC bắt.

### Việc cần làm

#### 1A. Nạp dữ liệu thật làm initial snapshot (Ngày 2)
- [ ] Tải **Brazilian E-Commerce (Olist)** từ Kaggle: 9 file CSV, ~100k orders giai đoạn 2016–2018, có sẵn customers, sellers, products, orders, order_items, payments, reviews + bảng dịch category sang tiếng Anh
- [ ] Thiết kế DDL Postgres cho 7 bảng theo schema Olist: `customers, sellers, products, orders, order_items, payments, reviews`
  - PK, FK, index trên các FK hay join
  - Map `orders.order_status` của Olist về tập dùng trong project: `created, paid, shipped, delivered, cancelled, refunded` (lưu cả status gốc nếu cần đối chiếu)
- [ ] Viết `scripts/load_olist_to_postgres.py`: nạp CSV bằng `COPY` (không insert từng dòng), kiểm tra FK integrity sau khi nạp — record mồ côi (order_item không có order, review không có order...) thì ghi log và loại/quarantine, không nạp bừa
- [ ] **Bổ sung cột mà Olist không có** — bước bắt buộc, thiếu là CDC không chạy được:
  - Thêm `created_at, updated_at` cho mọi bảng: khởi tạo `updated_at =` timestamp nghiệp vụ gần nhất của bản ghi đó (ví dụ orders lấy `order_purchase_timestamp`, products/customers lấy thời điểm snapshot)
  - Ghi rõ trong README: đây là mốc snapshot — mọi thay đổi có `updated_at` lớn hơn mốc này đều do simulator ở 1C tạo ra

#### 1B. Kiểm chứng dữ liệu thật (cuối Ngày 2)
- [ ] Chạy vài query nghiệp vụ bằng SQL thuần và giữ lại kết quả làm mốc đối soát về sau: doanh thu theo category/tháng, tỉ lệ huỷ, rating TB theo thời gian giao hàng, top products/sellers
- [ ] Vì là dữ liệu thật, các pattern này phải có sẵn mà không cần bạn "nặn": seasonality theo tháng, chênh lệch category, tương quan giao chậm ↔ rating thấp

#### 1C. Gen phần mới — bám trên dữ liệu thật (Ngày 3–4)
- [ ] Viết `scripts/simulate_new_day.py` — **script quan trọng nhất của giai đoạn này.** Khác với tự gen từ đầu: mọi bản ghi mới phải tham chiếu customers/products/sellers **có thật** trong DB:
  - Thêm orders mới (số lượng tuỳ scale đang chạy, xem 1D) cho khách cũ (để RFM/retention có ý nghĩa) + một phần khách mới bằng Faker
  - Update giá `order_items`/products: đổi giá 3% products thật, đổi `city/state` của 2% customers thật → tạo sự kiện cho SCD Type 2 ở Giai đoạn 4
  - Xoá (hard delete) 0.1% customers + soft-cancel một số orders → tạo sự kiện DELETE cho CDC ở Giai đoạn 2/8
  - Mọi thay đổi đều phải set `updated_at = NOW()`
- [ ] Viết `scripts/generate_clickstream.py`: sinh log JSON Lines, đủ các event `view_product, add_to_cart, begin_checkout, purchase`, có `session_id, device, utm_source` — và `product_id / customer_id` trong log **phải khớp với products/customers thật** để về sau join được với fact orders làm funnel

#### 1D. Scale lên Big Data — làm sau, khi pipeline đã đúng (cuối Giai đoạn 5 hoặc Giai đoạn 8)
- [ ] Olist gốc chỉ ~100k orders / ~112k order_items — đủ để build và kiểm chứng toàn bộ logic, **nhưng chưa đủ gọi là Big Data**. Đừng để việc scale làm chậm MVP.
- [ ] Khi logic đã đúng, khuếch đại (amplification) lên mục tiêu 10M orders / 25–35M order_items: sinh thêm orders bằng cách **sampling theo đúng phân phối thật** đã đo ở 1B (tỉ lệ category, region, payment method, seasonality, giỏ hàng TB) trên tập customers/products thật, thay vì phân phối random tự nghĩ ra
- [ ] Cách này giữ được câu chuyện trung thực khi phỏng vấn: "dữ liệu gốc là dữ liệu thật, phần scale-up được sinh theo đúng phân phối thống kê của dữ liệu thật"

### Done khi
- [ ] Đếm được số dòng từng bảng khớp số dòng Olist gốc (sau khi trừ record quarantine đã ghi log)
- [ ] Query ở 1B cho kết quả hợp lý và đã lưu làm mốc đối soát
- [ ] Chạy `simulate_new_day.py` 1 lần: số orders tăng, có products thật bị đổi giá (kiểm tra bằng `updated_at` mới nhất), có customers biến mất, và **không có order mới nào tham chiếu customer/product không tồn tại**

### Rủi ro thường gặp
- Quên bổ sung `updated_at` khi nạp Olist → Giai đoạn 2 không có mốc nào để CDC polling. Đây là việc đầu tiên phải làm sau khi nạp.
- Gen đơn mới bằng customer/product random không có trong DB → vỡ FK, funnel/RFM join ra rỗng. Simulator phải luôn lấy ID từ DB thật.
- Sa đà vào việc scale 10M dòng ngay ở giai đoạn này → mất nhiều ngày trước khi thấy được pipeline chạy. Snapshot thật ~100k dòng là đủ cho đến hết Giai đoạn 5.

---

## Giai đoạn 2 — Ingestion & CDC Phase 1 → Bronze (Ngày 5–7)

**Mục tiêu:** Dữ liệu từ 3 nguồn (DB, Logs, API) đều vào được S3 Bronze đúng chuẩn, chạy lại không bị trùng.

### Việc cần làm
- [ ] Tạo bảng control trong Postgres nguồn: `cdc_watermark(table_name, last_updated_at, last_run_at)`.
- [ ] Viết `ingestion/cdc_extract.py` (Phase 1 — timestamp CDC):
  - Với từng bảng: `SELECT * WHERE updated_at > :last_watermark ORDER BY updated_at`
  - Ghi ra Parquet, upload lên `s3://<bucket>/bronze/postgres/<table>/dt=YYYY-MM-DD/`
  - Chỉ cập nhật watermark **sau khi upload thành công** (upload fail mà watermark đã nhảy = mất dữ liệu vĩnh viễn)
  - Thêm cột `ingested_at, source_table` vào file Bronze
- [ ] Viết `ingestion/ingest_logs.py`: nén + upload clickstream `.json.gz` lên `bronze/clickstream/dt=.../hour=.../`
- [ ] Viết `ingestion/ingest_api.py`: gọi API tỷ giá + campaign (hoặc mock API), lưu JSON thô lên `bronze/api/<source>/dt=.../`, retry 3 lần khi API lỗi
- [ ] Chạy thử chu trình demo CDC hoàn chỉnh:
  1. Chạy ingestion lần 1 → ghi lại số file/số dòng Bronze
  2. Chạy `simulate_new_day.py`
  3. Chạy ingestion lần 2 → Bronze chỉ có thêm phần thay đổi, không load lại toàn bộ

### Done khi
- [ ] Cả 3 nguồn đều có dữ liệu trên S3, đúng cấu trúc prefix `dt=`
- [ ] Lần chạy ingestion thứ 2 (không có thay đổi mới ở nguồn) tạo ra **0 file mới / 0 dòng mới**
- [ ] Sau `simulate_new_day`, lần ingestion tiếp theo bắt đúng số orders mới (~50k, sai số nhỏ do update của bảng khác)
- [ ] Biết rõ và ghi lại trong README hạn chế của Phase 1: **không bắt được hard DELETE** — đây là lý do có Phase 2 ở Giai đoạn 8

### Rủi ro thường gặp
- Hai record có cùng `updated_at` ở biên watermark bị sót/trùng → dùng điều kiện `>= watermark` ở lần chạy sau và xử lý dedup ở Silver (Giai đoạn 3), đừng cố giải quyết triệt để ở Bronze.

---

## Giai đoạn 3 — Silver: Clean, Dedup, MERGE CDC (Ngày 8–10)

**Mục tiêu:** Biến Bronze raw thành bảng Silver sạch, tin cậy được — một dòng duy nhất cho mỗi PK, phản ánh trạng thái mới nhất ở OLTP.

### Việc cần làm
- [ ] Tạo schema/catalog Silver trong Databricks và lưu các bảng Delta trên S3 qua Unity Catalog/external location đã cấu hình.
- [ ] Viết Spark job `spark_jobs/silver_transform.py` cho 2 việc nặng nhất:
  - Flatten clickstream JSON lồng nhau → `slv_clickstream_events`
  - Clean + dedup `order_items` scale lớn
- [ ] Với các bảng còn lại, viết logic Silver bằng Databricks SQL hoặc Spark trên bảng Delta:
  - Chuẩn hoá: timestamp về UTC, `lower(trim(email))`, cast đúng type, chuẩn hoá `status`
  - Dedup: `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY updated_at DESC)` lấy bản mới nhất
  - Áp CDC bằng `MERGE INTO`: match → UPDATE, not matched → INSERT; record bị xoá ở nguồn (nếu bắt được) → soft-delete `is_deleted = true`
  - Loại/quarantine record lỗi (`quantity <= 0`, `unit_price < 0`, `customer_id IS NULL`) vào bảng `silver_quarantine` riêng, **không âm thầm bỏ qua**
- [ ] Xử lý late-arriving: record đến muộn nhưng `updated_at` cũ hơn bản hiện tại thì **không được ghi đè**
- [ ] Tạo đủ 7 bảng Silver: `slv_customers, slv_products, slv_orders, slv_order_items, slv_payments, slv_reviews, slv_clickstream_events`

### Done khi
- [ ] Mỗi bảng Silver không có PK trùng (query kiểm tra `GROUP BY pk HAVING COUNT(*) > 1` ra 0 dòng)
- [ ] **Test idempotency:** chạy lại toàn bộ job Silver 1 lần nữa với cùng dữ liệu Bronze → row count Silver không đổi
- [ ] **Test update:** đổi giá 1 product ở Postgres → chạy lại Giai đoạn 2 + 3 → Silver phản ánh giá mới
- [ ] Số dòng Silver khớp với số dòng OLTP (trừ record đã vào quarantine, phải giải thích được từng dòng chênh lệch)

### Rủi ro thường gặp
- Dedup chỉ theo PK mà quên so `updated_at` → bản cũ ghi đè bản mới. Đây là lỗi phỏng vấn rất hay bị hỏi xoáy.

---

## Giai đoạn 4 — dbt Core: Staging, Snapshot SCD2, Facts/Dims (Ngày 11–14)

**Mục tiêu:** Có star schema Gold đúng chuẩn data modeling, chạy incremental, lưu được lịch sử thay đổi của customer/product.

### Việc cần làm
- [ ] Khởi tạo dbt project, cấu hình profile Databricks SQL warehouse/catalog/schema và credential ngoài Git, cài `dbt_utils`
- [ ] **Staging** (`models/staging/`, materialized = view): `stg_customers, stg_products, stg_orders, stg_order_items, stg_payments, stg_reviews, stg_clickstream`
  - Chỉ rename, cast, chọn cột cần thiết — **không join, không aggregate ở tầng này**
- [ ] **Snapshots SCD Type 2** (`snapshots/`):
  - `customers_snapshot`: strategy = timestamp, `unique_key = customer_id`, `updated_at`
  - `products_snapshot`: strategy = timestamp, `unique_key = product_id`, `updated_at` (theo dõi thay đổi giá)
- [ ] **Intermediate** (`models/intermediate/`): `int_orders_enriched` (orders + payments đã aggregate theo order), `int_customer_orders` (lịch sử mua theo khách)
- [ ] **Core marts** (`models/marts/core/`):
  - Dimensions: `dim_customer` (từ snapshot SCD2), `dim_product` (từ snapshot SCD2), `dim_seller`, `dim_date`
  - Facts: `fct_orders`, `fct_order_items` — bảng Delta, `materialized = incremental, incremental_strategy = merge, unique_key = order_id / order_item key`, chỉ quét 3 ngày gần nhất để bắt late update
  - Chọn partition hoặc liquid clustering cho `fct_*` dựa trên kích thước dữ liệu và phép đo Databricks; tránh tạo quá nhiều partition/file nhỏ ở quy mô MVP
  - Join fact với `dim_product` theo đúng thời điểm bán: `order_date BETWEEN dim_product.dbt_valid_from AND COALESCE(dim_product.dbt_valid_to, '9999-12-31')`
- [ ] Viết dbt tests cho tầng này: `unique, not_null` trên mọi PK; `relationships` từ fact về dim; `accepted_values` cho `orders.status`; custom test: doanh thu không âm, `rating BETWEEN 1 AND 5`
- [ ] Chạy `dbt docs generate` và xem lineage graph — chụp lại graph này cho README

### Done khi
- [ ] `dbt run` + `dbt test` xanh toàn bộ ở lần chạy full-refresh và ở lần chạy incremental tiếp theo
- [ ] **Test SCD2:** đổi giá 1 product + đổi city 1 customer ở Postgres → chạy lại pipeline → snapshot có thêm 1 dòng lịch sử mới, dòng cũ bị đóng `dbt_valid_to`, fact cũ vẫn trỏ về giá/city tại thời điểm mua
- [ ] So sánh query profile trong Databricks khi có/không có filter `order_date`, ghi lại lượng dữ liệu đọc và thời gian thực thi để chứng minh cách bố trí dữ liệu đã chọn

### Rủi ro thường gặp
- Join fact–dim_product theo giá hiện tại thay vì SCD2 → toàn bộ doanh thu lịch sử sai khi giá đổi. Đây là lỗi nghiêm trọng nhất của project, kiểm tra kỹ ở bước test SCD2.

---

## Giai đoạn 5 — Data Marts & Business Logic (Ngày 15–16)

**Mục tiêu:** Biến star schema thành đúng 3–4 bảng mà dashboard/business dùng trực tiếp, không bắt dashboard tự join phức tạp.

### Việc cần làm
- [ ] `mart_daily_sales` (sales): doanh thu, số đơn, AOV, tỉ lệ huỷ theo ngày / category / region / payment method
- [ ] `mart_customer_rfm` (marketing): tính Recency (ngày từ lần mua delivered cuối), Frequency (số đơn delivered), Monetary (tổng tiền delivered) → chia quintile 1–5 → gán segment `Champions, Loyal, Potential, At Risk, Churned`
- [ ] `mart_funnel_daily` (marketing): theo ngày / device / utm_source — sessions → view → add_to_cart → begin_checkout → purchase + tỉ lệ chuyển đổi từng bước
- [ ] `mart_product_performance` (sales): doanh thu, số lượng, tỉ lệ refund, rating TB theo product / category / seller — dùng giá SCD2 tại thời điểm bán
- [ ] `mart_campaign_performance` (tuỳ chọn, nếu còn thời gian): join campaign API để tính doanh thu sau discount theo `utm_source`
- [ ] Thêm dbt tests riêng cho mart: tổng doanh thu trong `mart_daily_sales` của 1 ngày phải khớp tổng từ `fct_orders` của ngày đó (viết test đối soát hoặc query kiểm tra thủ công và ghi kết quả vào README)

### Done khi
- [ ] Cả 4 mart build được bằng `dbt run --select marts`
- [ ] Cộng doanh thu 1 ngày bất kỳ ở mart, ở fact và ở Postgres OLTP (chỉ tính đơn `delivered`) cho **ba con số khớp nhau** — đây là bước đối soát quan trọng nhất trước khi làm dashboard
- [ ] RFM: tổng số customers trong các segment cộng lại đúng bằng tổng số customers có ít nhất 1 đơn delivered, không khách nào bị tính 2 segment

---

## Giai đoạn 6 — Orchestration & Data Quality (Ngày 17–18)

**Mục tiêu:** Toàn bộ pipeline tự chạy theo lịch, fail thì biết ngay — không còn chạy tay từng script.

### Việc cần làm
- [ ] Viết Airflow DAG `ecommerce_cdc_daily` (+ bản rút gọn `ecommerce_cdc_hourly` cho CDC nếu muốn demo lag thấp):
  ```
  ingest_api → cdc_extract → load_silver → dbt_snapshot → dbt_run → dbt_test → ge_checkpoint → end
  generate_clickstream ─┘
  ```
- [ ] Cấu hình cho DAG: `retries = 2`, `retry_delay = 5m`, alert khi fail qua email hoặc Slack webhook (dùng webhook test cũng được)
- [ ] Great Expectations checkpoint trên Silver trước khi cho dbt chạy:
  - `orders.customer_id` not null, `payments.amount >= 0`
  - `clickstream.timestamp` không nằm trong tương lai
  - Row count Silver mỗi ngày không giảm bất thường (>20% so với trung bình 7 ngày = cảnh báo)
- [ ] Nếu GE fail → DAG dừng, **không** cho dữ liệu xấu chảy xuống Gold/dashboard
- [ ] Ghi log/run metadata mỗi lần chạy: ngày, số dòng từng tầng Bronze/Silver/Gold, thời gian chạy, trạng thái — lưu vào 1 bảng Delta `ops_pipeline_runs` (bảng này nuôi trang Data Quality của dashboard ở Giai đoạn 7)

### Done khi
- [ ] Tắt toàn bộ thao tác tay, để DAG tự chạy theo lịch 1 lần: dữ liệu "ngày mới" từ `simulate_new_day` tự chảy tới Gold mà không cần can thiệp
- [ ] Cố tình tạo 1 record lỗi ở nguồn (ví dụ order có `amount = -100`) → GE checkpoint fail, DAG dừng đúng chỗ, có alert, Gold không bị nhiễm dữ liệu lỗi
- [ ] Chạy lại DAG cùng 1 ngày 2 lần liên tiếp → không duplicate ở bất kỳ tầng nào

---

## Giai đoạn 7 — Dashboard & Kiểm chứng số liệu (Ngày 19–20)

**Mục tiêu:** Dashboard trả lời được câu hỏi nghiệp vụ và mọi con số trên đó đều đối soát được về nguồn.

### Quyết định mở
- Công cụ dashboard chưa được chọn. Trước khi bắt đầu giai đoạn này, chọn công cụ có cách kết nối được kiểm chứng với Databricks SQL hoặc với mart đã xuất bản, chi phí phù hợp và quyền chia sẻ bản demo rõ ràng.

### Việc cần làm
- [ ] Kết nối công cụ dashboard đã chọn với các bảng mart Gold (không nối trực tiếp vào fact/dim thô)
- [ ] **Trang 1 — Executive:** KPI doanh thu hôm nay/tháng này, số đơn, AOV, tỉ lệ huỷ; biểu đồ đường doanh thu 90 ngày; doanh thu theo category
- [ ] **Trang 2 — Customer/RFM:** phân bố segment RFM, bảng Top VIP, cohort retention theo tháng đăng ký
- [ ] **Trang 3 — Product & Funnel:** funnel chart 4 bước + tỉ lệ rớt, top/bottom product theo doanh thu và rating, tỉ lệ refund theo seller
- [ ] **Trang 4 — Data Quality/Ops:** số dòng Bronze/Silver/Gold theo ngày (từ `ops_pipeline_runs`), CDC lag, số record quarantine, lần chạy DAG gần nhất
- [ ] Kiểm chứng: chọn ngẫu nhiên 5 con số trên dashboard, truy ngược về mart → fact → OLTP bằng SQL, ghi lại quá trình đối soát này vào README/docs

### Done khi
- [ ] Người ngoài (bạn bè/mentor) mở dashboard tự hiểu được trang 1 mà không cần bạn giải thích
- [ ] 5/5 con số kiểm chứng khớp giữa dashboard và OLTP
- [ ] Dashboard có bộ lọc ngày/category hoạt động đúng, không có chart nào trống dữ liệu

---

## Giai đoạn 8 — Nâng cấp & Đóng gói Portfolio (Ngày 21–22+, tuỳ chọn nhưng rất ghi điểm)

**Mục tiêu:** Nâng project từ "Fresher khá" lên "Fresher nổi bật" và đóng gói để nộp CV.

### Việc cần làm — theo thứ tự ưu tiên
- [ ] **Debezium + Kafka (ưu tiên cao nhất):** Postgres local đã có `wal_level=logical` từ Giai đoạn 0; nếu dùng RDS, bật `rds.logical_replication` trong parameter group và kiểm tra quyền replication trước khi triển khai connector. Sự kiện CDC (`op, before, after, source_lsn`) chảy qua Kafka vào S3 Bronze → Silver Delta MERGE xử lý được hard DELETE thật. Giữ lại bản Phase 1 trong một nhánh/folder riêng để kể được câu chuyện nâng cấp từ polling lên log-based CDC.
- [ ] **Databricks Spark:** chuyển job flatten clickstream + clean `order_items` từ Spark local sang Databricks Jobs, dùng Delta MERGE và kiểm chứng Delta time travel.
- [ ] **CI đơn giản:** GitHub Actions chạy `dbt compile` + `sqlfluff` lint mỗi lần push
- [ ] **Terraform (tuỳ chọn):** khai báo S3 bucket, IAM và các tài nguyên AWS/Databricks đã dùng bằng Terraform thay vì click tay trên Console
- [ ] Hoàn thiện README theo đúng thứ tự người tuyển dụng đọc:
  1. Bài toán nghiệp vụ (3–4 dòng)
  2. Sơ đồ kiến trúc (`ecommerce-cdc-architecture.svg`)
  3. Stack + lý do chọn từng công nghệ
  4. Scale dữ liệu + ảnh chụp dashboard (4 trang)
  5. Những kỹ thuật khó: CDC, SCD2, Delta incremental merge, idempotency, partition/file layout — mỗi mục 2–3 dòng giải thích đánh đổi
  6. Cách chạy project bằng 1–2 lệnh (`make up && make seed && make run`)
  7. Hạn chế hiện tại + hướng phát triển (streaming thật, data contract...)
- [ ] Quay 1 video demo 3–5 phút: chạy `simulate_new_day` → Airflow DAG chạy → dashboard đổi số
- [ ] Viết dòng CV và chuẩn bị trả lời 5 câu hỏi phỏng vấn chắc chắn sẽ bị hỏi:
  1. CDC polling và log-based CDC khác nhau ở đâu? Vì sao polling bỏ sót DELETE?
  2. SCD Type 2 giải quyết vấn đề gì? Nếu không làm SCD2 thì báo cáo sai ở đâu?
  3. Làm sao pipeline của bạn idempotent?
  4. Vì sao chọn partition hoặc liquid clustering cho Delta theo `order_date`, và khi nào cần tối ưu file nhỏ?
  5. Dữ liệu ở OLTP và dashboard lệch nhau thì bạn debug từ tầng nào trước?

### Done khi
- [ ] Một người lạ clone repo về, đọc README, chạy được pipeline scale nhỏ mà không cần hỏi bạn
- [ ] Bạn tự trả lời được 5 câu hỏi trên trong dưới 2 phút mỗi câu mà không nhìn tài liệu

---

## Checklist tổng kết trước khi đưa vào CV

- [ ] Pipeline chạy end-to-end tự động bằng Airflow, không bước nào chạy tay
- [ ] Scale thật: Gold có ít nhất hàng chục triệu dòng fact, ghi rõ con số trong README
- [ ] Có đủ 3 nguồn dữ liệu khác loại: DB (CDC), Logs, API
- [ ] Có SCD Type 2 + incremental merge + idempotency — và bạn giải thích được cả 3
- [ ] Số liệu dashboard đối soát khớp với OLTP
- [ ] Không có credential/key nào trong lịch sử Git
- [ ] Có ảnh sơ đồ kiến trúc + ảnh dashboard + video demo trong README

## Nguyên tắc khi bị kẹt

1. Kẹt ở scale lớn → quay về scale nhỏ, làm cho đúng logic trước, scale chỉ là chạy lại seed với số lớn hơn.
2. Kẹt ở Debezium/Kafka quá 1 ngày → bỏ qua, quay lại Phase 1 polling để kịp tiến độ, Debezium để dành cho Giai đoạn 8.
3. Kẹt ở 1 mart → mart đó không được chặn các mart khác; làm mart dễ trước (Daily Sales → Product → RFM → Funnel).
4. Mỗi ngày kết thúc đều phải commit, kể cả code chưa xong — ghi rõ trong commit message phần nào đang dở.
