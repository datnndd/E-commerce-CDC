# PLAN.md — E-commerce CDC Lakehouse Project

> **Mục tiêu cuối cùng:** Một project Data Engineer hoàn chỉnh chạy end-to-end, bỏ được vào CV/GitHub và demo được trong phỏng vấn:
> PostgreSQL OLTP (local, sau đó Amazon RDS) → CDC → Amazon S3 Bronze → Databricks trên AWS xử lý Silver/Gold bằng Delta Lake và dbt-databricks (SCD Type 2, Incremental) → dashboard, điều phối bằng Airflow, kiểm tra chất lượng bằng dbt tests + Great Expectations.
>
> **Quy tắc làm việc:** Làm xong MVP end-to-end ở scale nhỏ trước (100k orders), rồi mới scale lớn và nâng cấp CDC. Theo lựa chọn hiện tại, Giai đoạn 4 và 5 được triển khai, đối soát local trước rồi kiểm tra cùng nhau trên Databricks. Chỉ đánh dấu tiêu chí "Done" cloud sau khi có kết quả chạy thực; mỗi phần hoàn tất cần tick checklist, commit Git và ghi kết quả vào README.

- **Niche:** E-commerce / Retail
- **Stack chính:** Docker, PostgreSQL/Amazon RDS, Python, Amazon S3, Databricks trên AWS, Spark, Delta Lake, dbt-databricks, Airflow, Power BI
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
| 5 | Data Marts & Business Logic | Ngày 15–16 | Daily Sales, Product, RFM, Funnel, Payment Mix, Order Experience |
| 6 | Orchestration & Data Quality | Ngày 17–18 | Airflow DAG tự chạy, GE checks, alert khi fail |
| 7 | Dashboard & Kiểm chứng số liệu | Ngày 19–20 | Dashboard 5 trang theo sáu business marts và vận hành, số khớp với OLTP |
| 8 | Nâng cấp & Đóng gói Portfolio | Ngày 21–22+ (tuỳ chọn) | Debezium/Kafka, README, demo, CV |

**Trạng thái 02/11/2026:** Giai đoạn 4–5 đã qua `dbt build` 122/122 và đối soát baseline. DAG Airflow local chạy thử thành công toàn tuyến trên database 1C, prefix S3 và schema Databricks riêng: Silver và Gold đều có 99.461 orders; 122/122 model/test qua, quarantine bằng 0, Ops ghi nhận `success`. Bài thử payment âm đã khiến Great Expectations fail 3 lần, webhook local nhận alert và Gold giữ nguyên. Sau khi trả payment về 99,33, Silver local và bước publish + dbt chạy riêng đã phục hồi, `dbt build` lại đạt 122/122 và Gold không có payment âm hay order trùng. Chủ dự án xác nhận đã thấy một lượt DAG thành công và chấp nhận chốt Giai đoạn 6; metadata Airflow local đọc trong lượt kiểm tra này vẫn có một run `running` và một run `queued`, nên lượt thành công mới chưa được đối chiếu bằng run ID. Bài thử hai lượt cùng logical date và lịch chạy không người giám sát vẫn chưa được xác minh. Giai đoạn 7 sẽ dùng Power BI.

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
  ├── scripts/                  # Harness: bootstrap/validation, không chứa mã pipeline
  ├── sources/                  # dữ liệu nguồn và bộ mô phỏng của project
  │   ├── oltp/                 # SQLAlchemy/Alembic, nạp Olist, mô phỏng thay đổi PostgreSQL
  │   └── clickstream/          # sinh event JSON Lines
  ├── spark_jobs/               # job Silver (giai đoạn 3)
  ├── dbt_project/              # dbt (giai đoạn 4-5)
  ├── airflow/dags/
  ├── great_expectations/
  ├── docs/                     # workflow, plan và tài liệu kiến trúc khi triển khai
  └── README.md
  ```
- [x] Xác nhận AWS Console/CloudShell ở `ap-southeast-1`; người dùng đã tự cấu hình budget. Databricks Free Edition có quota sử dụng riêng.
- [x] Tạo S3 bucket riêng cho project ở `ap-southeast-1`, bật Block Public Access, tắt ACL và dùng mã hoá mặc định SSE-S3. Dữ liệu Bronze sẽ dùng prefix `bronze/` khi ingestion chạy.
- [x] Chọn S3 bucket làm nơi lưu Bronze, và dùng Unity Catalog external location cho các prefix `silver/`, `gold/` khi tạo bảng Delta. Đặt lifecycle/xoá dữ liệu demo có chủ đích trước khi scale.
- [x] Tạo IAM role chỉ cho bucket project, trust Unity Catalog bằng external ID và self-assume; tạo storage credential và external location riêng. Không tải access key dài hạn vào repo hay `.env`. CLI local/credential tạm thời sẽ cấu hình trước Giai đoạn 2.
- [x] Xác nhận Databricks Free Edition metastore ở `us-east-2`, SQL warehouse và external location tới S3 `ap-southeast-1` hoạt động. Người dùng chọn giữ AWS ở Singapore và chấp nhận kết nối liên Region. Free Edition chỉ có serverless và không hỗ trợ custom workspace storage location; external location riêng đã được xác minh. RDS PostgreSQL được tạo ở Giai đoạn 1 khi DDL và lựa chọn VPC/chi phí đã rõ.
- [x] Viết `compose.yaml` cho Postgres 18.6 local (đặt `wal_level=logical`) + Airflow 3.3.2 local.
- [x] Cài và kiểm tra Python 3.13, dbt-databricks, pyspark, faker; dùng AWS CLI có sẵn trong CloudShell cho kiểm tra giai đoạn 0. Cấu hình CLI local trước khi viết ingestion ở Giai đoạn 2.

### Done khi
- [x] `docker compose up -d` chạy được Postgres + Airflow, vào được Airflow UI
- [x] Ghi/đọc metadata/xoá 1 file test trong S3 bằng AWS CLI CloudShell.
- [x] Chạy `SELECT 1` trên Databricks SQL warehouse; kết quả là `1`, sau đó dừng warehouse.
- [x] Kiểm tra external location tới S3 project: các phép `READ`, `LIST`, `WRITE`, `DELETE` đều `PASS`; file events tắt vì chưa dùng ở giai đoạn này.
- [x] Repo đã có commit cục bộ; `.gitignore` đã chặn `.env`, key JSON, `target/`, `dbt_packages/`. Các commit tiếp theo chỉ lưu cục bộ cho đến khi người dùng yêu cầu push.

### Rủi ro thường gặp
- Lộ AWS access key lên GitHub → vô hiệu hoá/thu hồi key ngay, kiểm tra quyền và xoá key khỏi lịch sử Git; ưu tiên credential tạm thời để tránh tình huống này.
- Để RDS/Databricks chạy ngoài giờ demo → tài nguyên có thể tiếp tục phát sinh chi phí.

---

## Giai đoạn 1 — OLTP: Nạp dữ liệu thật trước, gen phần mới sau (Ngày 2–4)

**Mục tiêu:** Có một "hệ thống bán hàng thật" để CDC bắt dữ liệu từ đó. Cách làm: **hybrid** — nạp dataset có sẵn làm dữ liệu lịch sử (initial snapshot), rồi mới gen thêm thay đổi mới dựa trên chính khách hàng/sản phẩm thật đó. Đây cũng đúng là vòng đời CDC ngoài đời thật: snapshot/backfill toàn bộ trước, sau đó mới chạy incremental CDC.

**Môi trường:** Phát triển và kiểm chứng DDL/seed trên Postgres Docker trước. Trước khi tạo Amazon RDS for PostgreSQL, chốt Region, VPC/private access, cỡ instance, lưu trữ, backup và thời gian bật để kiểm soát chi phí; sau đó nạp cùng dữ liệu vào RDS để trình diễn luồng cloud. Không public database chỉ để tiện kết nối.

**Vì sao không tự gen toàn bộ bằng Faker ngay từ đầu?**
- Tốn 2–3 ngày chỉ để chỉnh cho dữ liệu trông không giả, mà phân phối mua hàng, địa lý, category, nội dung review vẫn không thật bằng dữ liệu thật.
- Tự gen từ đầu chỉ còn cần thiết ở 2 chỗ dữ liệu thật không có: clickstream logs và các thay đổi *sau* thời điểm snapshot (đơn mới, đổi giá, đổi địa chỉ) — đó mới là phần gen có giá trị, vì nó tạo ra sự kiện cho CDC bắt.

### Việc cần làm

#### 1A. Nạp dữ liệu thật làm initial snapshot (Ngày 2)
- [x] Tải **Brazilian E-Commerce (Olist)** từ Kaggle: 9 file CSV, ~100k orders giai đoạn 2016–2018, có sẵn customers, sellers, products, orders, order_items, payments, reviews + bảng dịch category sang tiếng Anh
- [x] Thiết kế model SQLAlchemy và migration Alembic trong `sources/oltp/` cho các bảng theo schema Olist; migration đầu tiên đã được tạo và áp dụng vào PostgreSQL local
  - PK, FK, index trên các FK hay join
  - Map `orders.order_status` của Olist về tập dùng trong project: `created, paid, shipped, delivered, cancelled, refunded` (lưu cả status gốc nếu cần đối chiếu)
- [x] Viết `sources/oltp/load_olist.py`: nạp CSV bằng `COPY` (không insert từng dòng), kiểm tra FK integrity sau khi nạp — record mồ côi (order_item không có order, review không có order...) thì ghi log và loại/quarantine, không nạp bừa. Đã kiểm chứng trên DB tạm và nạp DB nguồn chính.
- [x] **Bổ sung cột mà Olist không có** — bước bắt buộc, thiếu là CDC không chạy được:
  - Thêm `created_at, updated_at` cho mọi bảng: khởi tạo `updated_at =` timestamp nghiệp vụ gần nhất của bản ghi đó (ví dụ orders lấy `order_purchase_timestamp`, products/customers lấy thời điểm snapshot)
  - Ghi rõ trong README: đây là mốc snapshot — mọi thay đổi có `updated_at` lớn hơn mốc này đều do simulator ở 1C tạo ra

#### 1B. Chốt mốc đối soát cho initial snapshot (cuối Ngày 2)
- [x] Sau khi nạp Olist vào DB nguồn chính, chưa chạy simulator trên DB này. Gán `snapshot_id` cho lần nạp; lưu danh sách/phiên bản CSV, thời điểm chốt, số dòng từng bảng và số dòng quarantine. Giữ nguồn đứng yên trong lần chốt mốc demo để các query đọc cùng một tập dữ liệu.
- [x] Lưu SQL thuần và kết quả độc lập với logic Silver/Gold cho một bộ chỉ số nhỏ: số dòng theo bảng, số đơn theo trạng thái, tổng `order_items.price` theo tháng và số bản ghi mồ côi. Mỗi kết quả gắn `snapshot_id`, định nghĩa phép tính, thời điểm chạy và múi giờ/đơn vị nếu có; tổng tiền ở cấp item, không join trực tiếp payments hoặc reviews vào phép cộng.
- [x] Giữ các phân tích doanh thu theo category, tỉ lệ huỷ, rating theo thời gian giao hàng, top products/sellers và seasonality làm khám phá dữ liệu để thiết kế mart/simulator; không coi mọi kết quả khám phá là tiêu chí bắt buộc phải khớp Gold.
- [ ] Khi Bronze → Silver → Gold đã xử lý xong initial snapshot, đối chiếu các chỉ số cùng định nghĩa và cùng `snapshot_id`; lưu chênh lệch và kiểm tra theo từng tầng nếu lệch. Việc này là tiêu chí kiểm chứng của các giai đoạn downstream, không chặn việc hoàn thành Giai đoạn 1.

#### 1C. Gen phần mới — bám trên dữ liệu thật (Ngày 3–4)
- [x] Phát triển và chạy thử simulator trên DB kiểm thử tách biệt, được nạp từ cùng snapshot. Chỉ chạy simulator trên DB nguồn chính sau khi initial snapshot đã đi qua Bronze/Silver/Gold và đối soát 1B đạt; sau đó đối soát thay đổi theo watermark, không quét toàn bộ OLTP sau mỗi lần chạy.
- [x] Viết `sources/oltp/simulate_new_day.py` — **script quan trọng nhất của giai đoạn này.** Khác với tự gen từ đầu: mọi bản ghi mới phải tham chiếu customers/products/sellers **có thật** trong DB:
  - Thêm orders mới (số lượng tuỳ scale đang chạy, xem 1D) cho khách cũ (để RFM/retention có ý nghĩa) + một phần khách mới bằng Faker
  - Đổi `products.current_price` của 3% products thật (không sửa giá order_items lịch sử), đổi địa chỉ của 2% customers thật → tạo sự kiện cho SCD Type 2 ở Giai đoạn 4
  - Đánh dấu xóa mềm 0.1% customers bằng `is_deleted/deleted_at` + tạo một số orders trạng thái `canceled`; CDC polling nhận các bản ghi này qua `updated_at` như UPDATE. DELETE vật lý chỉ xét khi có CDC đọc log ở Giai đoạn 8.
  - Mọi thay đổi đều phải set `updated_at = NOW()`
- [x] Viết `sources/clickstream/generate.py`: sinh log JSON Lines, đủ các event `view_product, add_to_cart, begin_checkout, purchase`, có `session_id, device, utm_source` — và `product_id / customer_id` trong log **phải khớp với products/customers thật** để về sau join được với fact orders làm funnel

#### 1D. Scale lên Big Data — làm sau, khi pipeline đã đúng (cuối Giai đoạn 5 hoặc Giai đoạn 8)
- [ ] Olist gốc chỉ ~100k orders / ~112k order_items — đủ để build và kiểm chứng toàn bộ logic, **nhưng chưa đủ gọi là Big Data**. Đừng để việc scale làm chậm MVP.
- [ ] Khi logic đã đúng, khuếch đại (amplification) lên mục tiêu 10M orders / 25–35M order_items: sinh thêm orders bằng cách **sampling theo đúng phân phối thật** đã đo ở 1B (tỉ lệ category, region, payment method, seasonality, giỏ hàng TB) trên tập customers/products thật, thay vì phân phối random tự nghĩ ra
- [ ] Cách này giữ được câu chuyện trung thực khi phỏng vấn: "dữ liệu gốc là dữ liệu thật, phần scale-up được sinh theo đúng phân phối thống kê của dữ liệu thật"

### Done khi
- [x] Đếm được số dòng từng bảng khớp số dòng Olist gốc (sau khi trừ record quarantine đã ghi log)
- [x] Đã lưu SQL, metadata và kết quả 1B theo `snapshot_id` để đối soát khi có Silver/Gold; DB nguồn chính chưa bị simulator thay đổi
- [x] Chạy `simulate_new_day.py` 1 lần **trên DB kiểm thử tách biệt**: số orders tăng, có products thật bị đổi giá (kiểm tra bằng `updated_at` mới nhất), hành vi xóa mềm khách hàng được kiểm chứng, và **không có order mới nào tham chiếu customer/product không tồn tại**

### Rủi ro thường gặp
- Quên bổ sung `updated_at` khi nạp Olist → Giai đoạn 2 không có mốc nào để CDC polling. Đây là việc đầu tiên phải làm sau khi nạp.
- Gen đơn mới bằng customer/product random không có trong DB → vỡ FK, funnel/RFM join ra rỗng. Simulator phải luôn lấy ID từ DB thật.
- Sa đà vào việc scale 10M dòng ngay ở giai đoạn này → mất nhiều ngày trước khi thấy được pipeline chạy. Snapshot thật ~100k dòng là đủ cho đến hết Giai đoạn 5.

---

## Giai đoạn 2 — Ingestion & CDC Phase 1 → Bronze (Ngày 5–7)

**Mục tiêu:** Dữ liệu từ 3 nguồn (DB, Logs, API) vào được S3 Bronze với run/manifest có thể kiểm tra và watermark an toàn. Polling PostgreSQL cố ý đọc lại 15 phút; việc loại trùng logic thuộc Silver ở Giai đoạn 3.

### Việc cần làm
- [x] Tạo bảng control trong Postgres nguồn: `cdc_watermark(table_name, last_updated_at, last_run_at)` (người dùng báo đã áp dụng `d79b3fd3ea12` trên RDS). Migration `6c4d9b7e2a11` bổ sung `last_boundary_hash` còn trong lịch sử nhưng không cần cho extractor overlap 15 phút.
- [x] Viết `ingestion/cdc_extract.py` (Phase 1 — timestamp CDC; đã kiểm thử với PostgreSQL tạm và S3 giả lập; người dùng báo đã upload `orders` từ EC2/RDS lên S3, chưa đối soát độc lập):
  - Lần đầu snapshot toàn bảng. Những lần sau lấy `updated_at > last_watermark - 15 phút` và `updated_at <= max(updated_at)`; đây là overlap tạm thời, có dòng lặp giữa các run.
  - Mỗi poll có dữ liệu tạo `s3://<bucket>/bronze/postgres_oltp/<table>/load_date=YYYY-MM-DD/run_id=YYYYMMDDTHHMMSSffffff/part-0000.parquet` (tách tiếp `part-0001.parquet`... mỗi 500,000 dòng), rồi `_manifest.json` sau cùng với `status=committed`. `load_date` là ngày UTC bắt đầu ingestion; manifest giữ cửa sổ nguồn đầy đủ. Silver chỉ đọc file được manifest committed tham chiếu.
  - Chỉ cập nhật watermark **sau khi manifest upload thành công**. Retry dùng run ID mới; nếu S3 thành công nhưng DB commit lỗi, Silver dedup các run lặp.
  - Thêm cột `ingested_at, source_table` vào file Bronze
- [ ] Đối chiếu revision checkout EC2 với repo hiện tại, giữ nguyên `.env` riêng trên EC2; xác nhận Alembic revision và IAM role.
- [ ] Người dùng báo đã chạy `--table orders` trên EC2/RDS; còn cần đối soát độc lập Parquet + manifest committed, `row_count`, cửa sổ nguồn, SHA-256 và `cdc_watermark`.
- [ ] Khi single-table đạt, chạy initial snapshot cho tám bảng OLTP còn lại và đối chiếu manifest với số dòng nguồn; `orders` chạy lại sẽ là delta overlap.
- [x] Viết `ingestion/ingest_logs.py`: nén + upload clickstream `.json.gz` lên `bronze/clickstream/dt=.../hour=.../run_id=.../`, rồi ghi manifest committed; đã kiểm thử 80 events trên bucket test.
- [x] Viết `ingestion/ingest_api.py`: lấy tỷ giá BRL/USD lịch sử từ Frankfurter và campaign mock qua HTTP, lưu JSON thô lên `bronze/api/<source>/dt=.../run_id=.../`, thử lại tối đa 3 lần khi lỗi tạm thời; đã kiểm thử hai nguồn trên bucket test.
- [x] Sinh 80 clickstream events từ 20 orders delivered trong snapshot RDS đã committed trên S3, không sửa DB; upload lên bucket dự án. Hai API cũng đã lên bucket dự án (777 tỷ giá, 3 campaign mock). Đã đọc lại cả ba manifest và đối chiếu SHA-256/kích thước; 20 `purchase` khớp `order_id`, `customer_id`, `product_id` và thời điểm mua trong snapshot.
- [ ] Chạy thử chu trình demo CDC hoàn chỉnh trên DB nguồn kiểm thử tách biệt, được nạp từ cùng snapshot; DB nguồn chính vẫn giữ initial snapshot đến khi đối soát Bronze/Silver/Gold đạt:
  1. Chạy ingestion lần 1 → ghi lại số file/số dòng Bronze
  2. Chạy `simulate_new_day.py` trên DB nguồn kiểm thử đó
  3. Chạy ingestion lần 2 → Bronze có phần thay đổi và overlap từ watermark lùi 15 phút; đánh dấu phần lặp để Silver xử lý ở Giai đoạn 3

### Done khi
- [x] Cả 3 nguồn đều có dữ liệu với manifest committed trên bucket dự án; PostgreSQL Bronze dùng `load_date=/run_id=/`.
- [ ] Mỗi initial snapshot PostgreSQL có `row_count` khớp nguồn, file được manifest tham chiếu và watermark đúng `max(updated_at)` sau upload thành công
- [ ] Trên nguồn `*_1c_test`, lần polling sau `simulate_new_day --orders 100` bắt đủ 100 orders mới theo run report, cộng với các dòng overlap; đối soát theo PK để tránh đếm trùng
- [ ] Biết rõ và ghi lại trong README hạn chế của Phase 1: **không bắt được hard DELETE** — đây là lý do có Phase 2 ở Giai đoạn 8

### Rủi ro thường gặp
- Dữ liệu đến muộn với `updated_at` cũ hơn `last_watermark - 15 phút` vẫn bị bỏ sót; các dòng trong overlap được Silver dedup ở Giai đoạn 3.
- Với 7 bảng Olist được gán cùng `updated_at = snapshot_at`, mỗi poll không đổi vẫn chọn và upload lại toàn bảng. Đã đo 1.352.257 dòng lặp trên local; cần xử lý trước khi chạy định kỳ.

---

## Giai đoạn 3 — Silver: Clean, Dedup, MERGE CDC (Ngày 8–10)

**Mục tiêu:** Biến Bronze raw thành bảng Silver sạch, tin cậy được — một dòng duy nhất cho mỗi PK, phản ánh trạng thái mới nhất ở OLTP.

### Việc cần làm
- [x] Tạo catalog `ecommerce_cdc` với Gold managed storage dưới `s3://<project-bucket>/gold/`; đăng ký 12 bảng Silver Delta external từ `silver/stage3_local_20261102/` trong schema `silver`. Đối chiếu số dòng từng bảng trên Databricks với local. Đây là snapshot Silver local được publish để kiểm chứng Gold, chưa phải kết quả chạy Spark job Silver trong Databricks.
- [x] Viết Spark job local `spark_jobs/silver_transform.py` đọc manifest committed, chuẩn hoá clickstream JSON hiện có (các event thực tế là JSON phẳng), clean + dedup `order_items` và ghi Delta. Đã chạy trên toàn bộ 9 bảng Olist local, clickstream và API; phần chạy trong Databricks vẫn chờ thử nghiệm cloud.
- [x] Viết logic Silver bằng Spark trên bảng Delta local cho các bảng còn lại:
  - Chuẩn hoá: timestamp về UTC, cast đúng type, chuẩn hoá `status`; nguồn Olist hiện không có cột `email`
  - Dedup: `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY updated_at DESC)` lấy bản mới nhất
  - Áp CDC bằng `MERGE INTO`: match → UPDATE, not matched → INSERT; record bị xoá ở nguồn (nếu bắt được) → soft-delete `is_deleted = true`
  - Loại/quarantine record lỗi vào bảng `silver_quarantine` riêng, **không âm thầm bỏ qua**; Olist `order_items` dùng `price` thay cho `unit_price` và không có cột `quantity`
- [x] Xử lý late-arriving ở local: record đến muộn nhưng `updated_at` cũ hơn bản hiện tại không ghi đè; kiểm thử bằng fixture Delta MERGE.
- [x] Tạo đủ 12 bảng Silver (9 bảng Olist + Clickstream + 2 bảng API) trên Delta local: `slv_customers, slv_products, slv_sellers, slv_category_translation, slv_geolocation, slv_orders, slv_order_items, slv_payments, slv_reviews, slv_clickstream_events, slv_exchange_rates, slv_campaigns` (cùng bảng cách ly `silver_quarantine`). Chưa tạo trong Databricks.

### Done khi
- [x] Local: cả 12 bảng Silver không có PK trùng hoặc PK null; `spark_jobs.verify_silver` đã kiểm tra trên Delta đầy đủ.
- [x] Local: chạy lại toàn bộ job Silver với cùng dữ liệu Bronze, số dòng cả 12 bảng không đổi.
- [x] Local fixture: hai run Bronze có dòng overlap chỉ còn một dòng hiện hành cho mỗi PK và không tăng row count vì replay.
- [x] **Test update local:** trên PostgreSQL tạm riêng, đổi giá 1 product từ 10 thành 20 → chạy lại extractor Bronze và Silver Delta MERGE → Silver phản ánh 20; watermark và hai manifest snapshot/delta đúng. Chưa thử chu trình này trên RDS/Databricks.
- [x] Local: số dòng 9 bảng Silver khớp 9 bảng PostgreSQL local; quarantine có 0 dòng trong snapshot sạch. Kiểm thử fixture chứng minh record lỗi được cách ly có lý do.

### Rủi ro thường gặp
- Dedup chỉ theo PK mà quên so `updated_at` → bản cũ ghi đè bản mới. Đây là lỗi phỏng vấn rất hay bị hỏi xoáy.

---

## Giai đoạn 4 — dbt Core: Staging, Snapshot SCD2, Facts/Dims (Ngày 11–14)

**Mục tiêu:** Có nhóm star schema Gold với grain rõ cho commerce, behavior và reference; lưu các phiên bản customer/product quan sát được bằng SCD2. Triển khai và đối soát local trước, sau đó mới chạy trên Databricks.

### Việc cần làm
- [x] Khởi tạo dbt project và profile mẫu Databricks OAuth không chứa credential; `dbt debug` đã kết nối warehouse thực, profile local hoàn chỉnh được Git bỏ qua. Chỉ thêm package khi model hoặc test thực sự cần.
- [x] **Staging** (`models/staging/`, materialized = view): `stg_customers, stg_products, stg_sellers, stg_category_translation, stg_geolocation, stg_orders, stg_order_items, stg_payments, stg_reviews, stg_clickstream_events, stg_exchange_rates, stg_campaigns`
  - Silver đã chuẩn hóa nên staging hiện là view chuyển tiếp, không join/aggregate; giữ cột mới của nguồn 1C khi chúng xuất hiện trong Silver.
- [x] **Snapshots SCD Type 2** đã định nghĩa (`snapshots/`) và chạy trong hai lần `dbt build` trên Databricks:
  - `customers_snapshot`: strategy = timestamp, `unique_key = customer_id`, `updated_at`
  - `products_snapshot`: strategy = timestamp, `unique_key = product_id`, `updated_at` (theo dõi thay đổi giá)
- [x] **Intermediate** (`models/intermediate/`): `int_orders_enriched` cộng item/payment **riêng từng bảng** về `order_id` trước khi join; `int_customer_orders` nhóm đơn đã giao theo `customer_unique_id` để phục vụ RFM.
- [x] **Core marts** (`models/marts/core/`) đã viết và chạy SQL trên Delta local, chia theo grain và nghiệp vụ thay vì một bảng rộng:
  - Conformed dimensions: `dim_date` (một ngày, dùng theo nhiều vai trò), `dim_customer` (một phiên bản `customer_id`, SCD2, giữ `customer_unique_id`), `dim_product` (một phiên bản `product_id`, SCD2), `dim_seller` (một seller), `dim_category` (một category), `dim_geography` (một cặp ZIP prefix/state đã chuẩn hóa), `dim_channel` (một `utm_source`). `dim_campaign` chỉ là dimension **synthetic** tùy chọn; không tự suy diễn campaign của đơn từ `utm_source`.
  - Commerce facts: `fct_orders` (một `order_id`), `fct_order_items` (một `(order_id, order_item_id)`), `fct_payments` (một `(order_id, payment_sequential)`), `fct_reviews` (một `review_row_id`). Order revenue và item revenue lấy từ `order_items.price`, không lấy `products.current_price`; payment là dòng tiền riêng. Review gắn với đơn, không gán cho từng product/seller khi đơn có nhiều item.
  - Behavior facts: `fct_clickstream_events` (một `event_id`) và `fct_sessions` (một `session_id`). Clickstream từ Olist là dữ liệu mô phỏng dựa trên đơn đã mua nên funnel không đại diện cho tỷ lệ chuyển đổi thật.
  - Reference fact: `fct_exchange_rates` (một `(date, base, quote)`); tỷ giá không cộng dồn và cần quy tắc lấy ngày gần nhất trước đó khi quy đổi cuối tuần.
  - Các fact có thay đổi dùng Delta `incremental_strategy = merge` với `unique_key` theo grain, chọn dòng nguồn bằng thời gian ingestion của Silver (kể cả cập nhật đơn cũ); `fct_sessions` nhỏ có thể rebuild toàn bộ. Chọn partition/liquid clustering sau khi đo kích thước và query thực tế.
  - Gắn fact với phiên bản SCD2 tại thời điểm sự kiện **khi có lịch sử quan sát**; đơn Olist trước lần snapshot đầu tiên gắn với phiên bản đầu quan sát được. Không coi phiên bản này là trạng thái lịch sử thực của 2016–2018. SCD2 không tạo lại lịch sử chưa từng được ghi nhận.
- [x] Viết dbt tests: `unique, not_null` theo grain, quan hệ fact → dimension, trạng thái đơn hợp lệ, giá item không âm, review score 1–5; test tổng item price và số dòng so với Silver/source baseline, test không fanout khi cộng payment/review. Cả 85 data tests (generic và singular) qua hai lần `dbt build` trên Databricks.
- [x] Đối soát SQL Gold trên Silver local: 99,441 orders, 112,650 items, 103,886 payments, 99,224 reviews, 80 click events, 777 rates; tổng `item_price_brl` = BRL 13,591,643.70 và cả 24 tháng mua hàng khớp baseline. Fixture hai phiên bản xác nhận đơn cũ giữ khóa phiên bản đầu và giá bán gốc.
- [x] Khi nối Product Mart ở Giai đoạn 5, test grain phát hiện `dim_category` có hai dòng khóa `__unknown__` do category NULL. Đã sửa để NULL ánh xạ tới một dòng; kiểm tra local xác nhận cả tám dimension keys đều unique/non-null và doanh thu product mart không bị nhân lên.
- [x] `dbt parse` nhận 35 models (29 model Giai đoạn 4 + 6 business marts), 2 snapshots, 85 data tests; hai lần `dbt build` cloud đều PASS=122, WARN=0, ERROR=0.
- [x] Chạy `dbt docs generate` trên profile cloud, xem lineage graph của `mart_daily_sales` và lưu ảnh tại `docs/images/dbt-lineage-sales.png` cho README.
- [x] Trên schema Silver kiểm thử tách biệt trong Databricks, đổi city của một customer và `current_price` của một product, chạy lại snapshots + dependent facts: mỗi đối tượng có hai phiên bản, phiên bản cũ đóng `dbt_valid_to`; fact của đơn Olist cũ giữ khóa phiên bản đầu và giá item BRL 44.99 dù giá catalog mới là BRL 54.99. Đây là kiểm thử dbt từ Silver cô lập, không thay cho chu trình nguồn 1C → Bronze → Silver.

### Done khi
- [x] SQL của 29 model Giai đoạn 4 chạy trên 12 bảng Silver Delta local; 5 singular SQL tests và fixture phát hiện duplicate item đã qua. Fixture hai phiên bản kiểm tra khóa SCD2 và giá bán, không thay thế lần chạy snapshot thật.
- [x] `dbt build` xanh trên Databricks ở lần chạy đầu và lần chạy incremental tiếp theo (122/122 mỗi lần); Delta history của `fct_orders` xác nhận lần hai có `MERGE`, số dòng và doanh thu không tăng trùng. Lần chạy đó đã cập nhật lại 99,441 order cùng mốc ingestion dù dữ liệu không đổi; follow-up Giai đoạn 6 bên dưới xử lý việc này.
- [x] Follow-up Giai đoạn 6: sáu fact incremental bỏ qua khóa đã có cùng `source_ingested_at`; full cloud `dbt build` tiếp theo đạt 122/122, Delta history cả sáu fact cho thấy 0 update/0 insert khi Silver không đổi.
- [ ] **Test SCD2:** trên nguồn kiểm thử 1C riêng, đổi giá 1 product + đổi city 1 customer → chạy lại pipeline → snapshot có thêm phiên bản và đóng `dbt_valid_to` cũ; fact cũ giữ `item_price` và khóa phiên bản phù hợp. Snapshot RDS gốc hiện không có `current_price`/soft-delete nên không thể dùng nó để chứng minh thử nghiệm này.
- [x] So sánh query history/profile trên `fct_orders` khi có/không có `purchase_date_key = 20180101`: đều đọc một file 10,111,873 byte từ cache; lần lượt 837 ms và 939 ms, số dòng đọc 74 và 99,441. Filter chưa prune file, nhưng ở kích thước hiện tại chưa có bằng chứng cần clustering/partition; đo lại khi scale.

### Rủi ro thường gặp
- Dùng `products.current_price` để tính doanh thu cũ hoặc ép mọi đơn 2016–2018 vào cửa sổ hiệu lực snapshot mới → báo cáo lịch sử sai. Dùng `order_items.price`; với thuộc tính product/customer trước lần quan sát đầu, ghi rõ giới hạn lịch sử.
- Join trực tiếp items × payments × reviews hoặc geolocation thô theo ZIP → nhân bản dòng và tiền. Mỗi fact giữ grain riêng; geolocation phải gom về một dòng cho mỗi cặp ZIP/state trước khi join.

---

## Giai đoạn 5 — Data Marts & Business Logic (Ngày 15–16)

**Mục tiêu:** Tạo sáu mart theo câu hỏi kinh doanh trên các fact đúng grain, để dashboard không phải tự join phức tạp. Đã hoàn thiện và đối soát local, sau đó chạy cả giai đoạn 4–5 trên Databricks.

### Việc cần làm
- [x] **Sales:** `mart_daily_sales` một dòng cho ngày mua × bang của khách, từ `fct_orders`; tổng đơn/hủy tính trên mọi status, doanh thu hàng, phí giao và AOV chỉ tính `delivered`. `mart_product_performance` một dòng cho product × seller × category quan sát lúc bán, từ `fct_order_items` đã giao. Không chia item revenue theo payment method khi chưa có quy tắc phân bổ.
- [x] **Customer:** `mart_customer_rfm` một dòng cho `customer_unique_id`, chỉ tính đơn `delivered`; Monetary = tổng `item_subtotal_brl`. `as_of_date` = một ngày sau ngày mua delivered cuối trong bộ dữ liệu, để Olist lịch sử không bị gán tất cả là churned theo ngày chạy hiện tại. Điểm R/F/M 1–5 dùng percentile rank có giữ tie: recency thấp tốt, frequency/monetary cao tốt. Gán nhãn theo thứ tự: Champions nếu R/F/M đều ≥4; Loyal nếu R≥3 và F≥4; Potential nếu R≥4; At Risk nếu R≥2; còn lại Churned.
- [x] **Behavior:** `mart_funnel_daily` một dòng cho ngày bắt đầu session × device × channel, đếm session qua bốn bước từ `fct_sessions`; clickstream hiện sinh từ đơn đã mua nên funnel là demo pipeline, không phải KPI chuyển đổi thật.
- [x] **Payment:** `mart_payment_mix_daily` một dòng cho **ngày mua của đơn** × `payment_type`, từ `fct_payments`; có số payment, số đơn phân biệt, tổng `payment_value_brl`, chỉ số trả góp. Nguồn không có timestamp thanh toán; tổng payment không được gọi là doanh thu hàng.
- [x] **Experience:** `mart_order_experience_daily` một dòng cho cohort ngày mua × bang khách, từ đơn `delivered`. Gom `fct_reviews` về một điểm trung bình mỗi order trước khi join; giao trễ so bằng ngày địa phương `America/Sao_Paulo` giữa actual và estimated delivery. Báo số đơn có delivery/review để hiểu mẫu số.
- [x] **Reference:** `fct_exchange_rates` vẫn là fact tham chiếu; chỉ thêm USD vào mart khi dashboard cần, lấy rate gần nhất với `date <= purchase_date`. `mart_campaign_performance` chưa làm vì campaign chỉ là mock và event không có `campaign_id`; không tính refund rate hay rating theo product vì nguồn không hỗ trợ.
- [x] Test mart trên Delta local và Databricks: grain duy nhất; sales/product/RFM/payment/experience/funnel đối soát với fact tương ứng; doanh thu delivered khớp baseline nguồn ở cả 12 tháng lịch (BRL 13,221,498.11); mỗi `customer_unique_id` có đúng một segment, các giá trị R/F/M bằng nhau cùng điểm; funnel mỗi bước không vượt số session. Các test generic/singular trong hai lần `dbt build` cloud đều pass.

**Kết quả local trên snapshot Olist hiện tại** (số dòng có thể đổi khi nạp thêm CDC):

| Business mart | Số dòng |
|---|---:|
| `mart_daily_sales` | 10,742 |
| `mart_product_performance` | 33,659 |
| `mart_customer_rfm` | 93,358 |
| `mart_funnel_daily` | 20 |
| `mart_payment_mix_daily` | 2,276 |
| `mart_order_experience_daily` | 10,608 |

Local verifier đã chạy 14 singular SQL tests cho business marts, phát hiện đúng một fixture sales bị trùng grain, và đối soát doanh thu delivered BRL 13,221,498.11 với baseline PostgreSQL trong cả 12 tháng lịch. `dbt ls --select marts.business --resource-type model` trả về đúng sáu mart.

### Done khi
- [x] Sáu model business và các SQL tests chạy được trên Gold/Silver Delta local; test grain có fixture vi phạm và phát hiện đúng.
- [x] Cả sáu mart và tests build thành công trong hai lần `dbt build` toàn bộ project trên Databricks; số dòng từng mart khớp local.
- [x] Tổng doanh thu delivered của mart khớp fact; cả 12 tháng lịch khớp baseline SQL từ Postgres OLTP. Riêng ngày 2018-01-01 (`America/Sao_Paulo`), PostgreSQL local và Databricks cùng có 81 items, BRL 6,948.21 giá item và BRL 6,818.31 doanh thu item đã giao.
- [x] RFM: tổng `frequency_orders` bằng số đơn delivered, một dòng và một segment cho mỗi `customer_unique_id` có đơn delivered

---

## Giai đoạn 6 — Orchestration & Data Quality (Ngày 17–18)

**Mục tiêu:** Airflow local điều phối nguồn → Silver → Gold/sáu business marts trên Databricks; lỗi dữ liệu phải dừng trước Gold và có dấu vết vận hành. Token Databricks cho DAG được cấu hình riêng, không dùng OAuth đăng nhập tương tác.

### Việc cần làm
- [x] Viết Airflow DAG `ecommerce_cdc_daily` theo thứ tự:
  ```
  cdc_extract → ingest_demo_clickstream ─┐
  ingest_api (rates + mock campaign) ────┴→ sync_committed_bronze → load_silver → silver_quality_gate
                                                                   ↓
                           publish_silver → dbt build (snapshots, facts, sáu marts, tests) → record_run
  ```
- [x] Airflow local chạy được Python/uv, Java/Spark, PostgreSQL, S3 và dbt Databricks bằng token cung cấp lúc chạy. DAG đặt `retries = 2`, `retry_delay = 5m`, callback webhook khi task fail; cấu hình token không tương tác và webhook thật vẫn thuộc bước bật lịch unattended.
- [x] Chạy thử 1C với prefix S3 và schema Databricks riêng (`PIPELINE_S3_PREFIX`, `DBT_SILVER_SCHEMA`, `DBT_TARGET_SCHEMA`), không trộn dữ liệu mô phỏng với snapshot RDS chính.
- [x] Great Expectations checkpoint trên **Silver trước khi publish/đọc vào Gold**: `slv_orders.customer_id` không null, `slv_payments.payment_value >= 0`, `slv_clickstream_events.event_timestamp` không nằm trong tương lai. Ops ghi quarantine và cảnh báo khi tổng dòng Silver giảm >20% so với trung bình 7 lần chạy thành công cùng namespace; cảnh báo volume không tự chặn DAG.
- [x] Khi checkpoint fail, `publish_silver` và `dbt_build` không chạy; Gold giữ 99.461 orders như trước lỗi. Lượt payment âm làm checkpoint fail cả ba lần, webhook local nhận alert. Bản ghi Ops của hai lượt bị gián đoạn đã được sửa về `failed`; vẫn cần chạy lại nhánh lỗi tuần tự để xác nhận trạng thái DagRun cuối.
- [x] Ghi metadata mỗi lần chạy vào Delta `ops_pipeline_runs`: run ID, thời gian, trạng thái, lỗi, số dòng Bronze/Silver/Gold và từng business mart, số quarantine. Bảng này nuôi trang Data Quality/Ops Giai đoạn 7.
- [x] Poll không đổi không upload lại: lần poll `orders` lặp trên nguồn 1C quét 0 và xuất bản 0 dòng. Sáu fact incremental bỏ qua cùng khóa/cùng ingestion time; lượt `dbt build` lại trên main có 0 MERGE updates/inserts. Chưa bật lịch unattended trước khi token và đường mạng được cấu hình.

### Done khi
- [x] Chạy DAG với nguồn 1C cô lập: Silver và Gold cùng 99.461 orders, sáu marts có tổng 150.672 dòng, `dbt build` 122/122. S3 và Databricks của lượt thử dùng prefix/schema riêng; bảng Silver chính vẫn trỏ về vị trí cũ.
- [x] Tạo một `order_payments.payment_value < 0` trong nguồn thử → checkpoint fail, DAG dừng đúng chỗ, có alert, Gold không đổi.
- [ ] Chạy lại cùng logical date hai lần → không trùng khóa nguồn, Silver, Gold hay marts; `ops_pipeline_runs` truy vết được cả kết quả chạy lại. Chủ dự án chấp nhận chốt Giai đoạn 6 trước khi xác minh tiêu chí này.

**Bằng chứng phục hồi từng bước:** sau khi sửa payment 1C về 99,33, Silver local có 103.906 payment, không có khóa trùng hay giá trị âm. Publish Silver lên schema thử thành công, `dbt build` đạt 122/122; Gold có 99.461 orders và 103.906 payments, payment thử là 99,33 và không còn payment âm. Hai lượt Airflow bị gián đoạn có Ops `failed`, không ghi nhầm `success`. Tiêu chí hai lượt Airflow thành công cùng logical date còn mở vì `dags test` đồng thời đã gây xung đột SQLite và Docker Desktop dừng trong lượt chạy tuần tự sau đó. Việc chốt Giai đoạn 6 là nghiệm thu của chủ dự án, không phải bằng chứng bổ sung cho tiêu chí này.

---

## Giai đoạn 7 — Dashboard & Kiểm chứng số liệu (Ngày 19–20)

**Mục tiêu:** Dashboard trả lời được câu hỏi nghiệp vụ và mọi con số trên đó đều đối soát được về nguồn.

### Công cụ dashboard
- Chủ dự án chọn Power BI và chế độ Import cho sáu mart cùng bảng Ops để tránh truy vấn Databricks theo từng visual. PBIP năm trang đã được dựng và kiểm tra cấu trúc ngày 03/11/2026, nhưng chưa có dữ liệu: lần Import đầu tiên bị SQL warehouse từ chối vì workspace không còn đủ điều kiện chạy Serverless Compute. Import vẫn cần warehouse hoạt động khi nạp và refresh. Kiểm chứng đọc `mart_daily_sales` cùng `ops_pipeline_runs`, cách refresh và quyền chia sẻ bản demo còn mở.

### Việc cần làm
- [ ] Kết nối Power BI với sáu business marts Gold; trang Ops đọc `ops_pipeline_runs` từ Giai đoạn 6. Không để dashboard tự join các fact/dim thô để tính KPI.
- [ ] **Trang 1 — Executive/Sales** (`mart_daily_sales`): doanh thu **item đã giao**, số đơn mọi status, số đơn đã giao, AOV item đã giao và tỉ lệ huỷ; đường doanh thu theo ngày, so sánh theo bang khách. Khi gộp nhiều ngày/bang, tính lại AOV = tổng `delivered_item_revenue_brl` / tổng `delivered_order_count` và tỉ lệ huỷ = tổng `canceled_order_count` / tổng `order_count`, không lấy trung bình các tỉ lệ có sẵn.
- [ ] **Trang 2 — Product** (`mart_product_performance`): Top sản phẩm, danh mục, seller và bang seller theo doanh thu item đã giao; số item bán. Mart hiện không có ngày bán hoặc bang khách, nên đây là tổng hợp toàn kỳ: không gắn bộ lọc ngày/bang khách từ trang Executive. Tổng `order_count` theo các product/category không phải số đơn duy nhất vì một đơn có thể chứa nhiều item.
- [ ] **Trang 3 — Customer/RFM** (`mart_customer_rfm`): phân bố số khách theo segment, tổng `monetary_brl` theo segment, bảng khách hàng giá trị cao. Hiển thị `as_of_date`; đây là một phân đoạn hiện tại theo ngày dữ liệu, không phải lịch sử segment theo tháng hay ngày đăng ký khách.
- [ ] **Trang 4 — Journey & Experience:** cơ cấu `payment_value_brl` theo phương thức từ `mart_payment_mix_daily`; tỉ lệ giao trễ, thời gian giao và review ở cấp đơn từ `mart_order_experience_daily`. Đặt funnel từ `mart_funnel_daily` trong khu vực riêng có nhãn **clickstream mô phỏng từ đơn hàng**, không diễn giải thành tỉ lệ chuyển đổi khách thật. Payment gắn với ngày mua của order, không phải ngày trả tiền; không cộng `paid_order_count` qua payment type để suy ra số đơn duy nhất. Khi gộp cohort, tính tỉ lệ giao trễ = tổng `late_delivery_order_count` / tổng `orders_with_delivery_date`, không lấy trung bình tỉ lệ ngày.
- [ ] **Trang 5 — Data Quality/Ops:** số dòng Bronze/Silver/Gold theo ngày, số record quarantine, lần chạy DAG gần nhất và trạng thái lỗi từ `ops_pipeline_runs`/kết quả Giai đoạn 6. Chỉ vẽ CDC lag sau khi Giai đoạn 6 ghi nhận được mốc nguồn và mốc xử lý để tính chỉ số này.
- [ ] Cấu hình bộ lọc theo grain từng trang: ngày mua/bang khách cho Executive; category/seller cho Product; segment cho Customer; ngày mua cho Payment/Experience và ngày session riêng cho Funnel; ngày chạy cho Ops. Mặc định hiển thị khoảng ngày thực sự có dữ liệu Olist, tránh khoảng ngày hiện tại rỗng. Chỉ dùng bộ lọc chung khi các dataset có cùng ý nghĩa trường lọc.
- [ ] Nếu cần biểu đồ **doanh thu category theo ngày** hoặc **khách mua lại theo tháng mua đầu tiên**, thiết kế và đối soát mart theo grain tương ứng trước khi đưa lên dashboard; sáu mart hiện tại chưa hỗ trợ hai biểu đồ đó. Không hiển thị refund theo seller hoặc rating theo product khi nguồn chưa hỗ trợ.
- [ ] Kiểm chứng: chọn ngẫu nhiên 5 con số trên dashboard, truy ngược về mart → fact → OLTP bằng SQL, ghi lại quá trình đối soát này vào README/docs

### Done khi
- [ ] Người ngoài (bạn bè/mentor) mở dashboard tự hiểu được trang 1 mà không cần bạn giải thích
- [ ] 5/5 con số kiểm chứng khớp giữa dashboard và OLTP
- [ ] Bộ lọc đúng theo grain của từng trang, không áp bộ lọc ngày lên Product/RFM hiện tại; không có chart nào trống vì mặc định chọn ngày ngoài phạm vi dữ liệu

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
