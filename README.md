# AdventureWorks Analytics Platform

AdventureWorks Analytics Platform là một nền tảng dữ liệu theo mô hình medallion, được xây dựng để đưa dữ liệu AdventureWorks từ hệ thống nguồn vào warehouse, làm sạch và chuẩn hóa dữ liệu, tạo các bảng phân tích, rồi phục vụ báo cáo doanh thu và hiệu suất bán hàng.

Điểm quan trọng của repository này không chỉ là các bảng Bronze, Silver và Gold. Sau các đợt refactor 4A, 4B, 4C và 4D, hệ thống còn có một lớp nền tảng chung để quản lý cấu hình, kết nối, identity, retry, audit, checkpoint, staging, quarantine, validation và publication. Nhờ vậy, pipeline có thể thất bại an toàn, chạy lại có kiểm soát và giữ nguyên phiên bản dữ liệu tốt gần nhất.

## Câu chuyện của hệ thống

Ban đầu, ứng dụng có một entry point mỏng và một số job gắn chặt với logic Sales. Khi phạm vi mở rộng, hệ thống được tách thành ba vùng rõ ràng:

```text
src/app/       orchestration và các gate của pipeline
src/features/  logic nghiệp vụ theo domain
src/shared/    hạ tầng ingestion dùng chung
src/core/      settings và các compatibility shell tối thiểu
```

Nguyên tắc sau refactor là: **feature sử dụng shared infrastructure; shared infrastructure không phụ thuộc vào feature cụ thể**. Vì vậy, Sales, Person và Production có thể sở hữu các job riêng, nhưng cùng dùng một bộ contract và cơ chế vận hành thống nhất.

### 4A - Đặt nền móng

Phase 4A xây dựng những quy tắc mà mọi layer về sau đều dùng:

- `pydantic-settings` tập trung hóa cấu hình và che giấu secret.
- `TableSpec` mô tả source, target, primary key, required columns và ordering key.
- Result model chuẩn hóa status, identity, row counts, timing và error.
- Retry, staging, audit, quarantine và checkpoint trở thành các service có thể inject.
- Domain ownership được tách thành Sales, Person và Production.

Từ đây, orchestration không còn phải biết chi tiết kết nối hay cách ghi từng batch. Nó chỉ điều phối các component theo contract.

### 4B - Bronze trở thành vùng landing có kiểm soát

Bronze nhận dữ liệu raw từ SQL Server AdventureWorks2012. Ba domain job cùng chạy theo một bộ cơ chế chung:

| Domain job | Bronze targets |
|---|---|
| Sales | `sales_order_header`, `sales_order_detail`, `customer`, `sales_territory`, `sales_person` |
| Person | `person` |
| Production | `product` |

Mỗi bảng được đọc theo stable ordering key và batch. Dữ liệu đi qua staging riêng cho run/load, được bổ sung lineage, ghi audit/checkpoint, quarantine các dòng bị loại và retry các lỗi transient. Chỉ sau khi toàn bộ staging pass validation thì dữ liệu mới được publish vào Bronze.

Nếu một batch hoặc một run thất bại, reconciliation và deterministic identity giúp hệ thống biết phần nào đã commit để không blind append. Bronze đang publish không bị xóa trước khi bản mới được kiểm tra xong.

### 4C - Silver biến raw data thành analytical source

Sau khi bảy Bronze target cùng thuộc về một `snapshot_id` và pass `BronzeSnapshotGate`, Silver chạy theo thứ tự dependency cố định:

```text
sales_order_header
sales_order_detail
customer
sales_territory
product
sales_person
```

Silver đọc Bronze theo chunk, kiểm tra schema, chuyển đổi kiểu dữ liệu, chạy các cleaner nghiệp vụ, quarantine lỗi chuyển đổi, kiểm tra grain và primary key, deduplicate toàn bảng, rồi validation trước publication. `bronze.person` là dependency bắt buộc để làm giàu `sales_person`.

Kết quả của Silver là sáu bảng chuẩn hóa:

```text
silver.sales_order_header_clean
silver.sales_order_detail_clean
silver.customer_clean
silver.sales_territory_clean
silver.product_clean
silver.sales_person_clean
```

Silver chỉ được coi là sẵn sàng cho Gold khi đủ sáu bảng đã publish và chia sẻ cùng `source_snapshot_id`.

### 4D - Gold trở thành lớp phân tích an toàn

Gold xây dựng một star schema gồm năm dimension và một fact:

```text
gold.dim_date
gold.dim_customer
gold.dim_product
gold.dim_territory
gold.dim_salesperson
gold.fact_sales
```

Các dimension nhỏ được đọc full-read. `fact_sales` được đọc theo stable-key batch trên `sales_order_detail_id`, bảo đảm grain một dòng cho mỗi sales order detail. Trước publication, Gold kiểm tra:

- schema, type và metadata;
- primary key, nullability và uniqueness;
- fact grain và orphan reference;
- công thức measure;
- KPI so với Silver baseline, tolerance mặc định 2%;
- PK/FK trên candidate schema.

Gold không ghi trực tiếp lên version đang phục vụ consumer. Candidate được xây dựng trong schema/version riêng, sau đó current pointer mới được cập nhật atomically. Nếu build, validation, constraint, KPI hoặc publication thất bại, Gold version cũ tiếp tục phục vụ dữ liệu.

## Workflow hiện tại

Đường đi canonical của ứng dụng là:

```text
main.py
  -> App.run()
  -> PipelineRunner.run(mode="full")
  -> ConnectionHealthService
  -> PlatformBootstrapJob
  -> BronzeToSilverPipeline
       -> SalesBronzeIngestionJob
       -> PersonBronzeJob
       -> ProductionBronzeJob
       -> BronzeSnapshotGate
       -> SalesSilverJob
  -> SalesGoldJob
```

Trong wiring hiện tại, `App` đăng ký `PostgresSilverPublishService` cho Silver và tạo `SalesGoldJob` production từ PostgreSQL Gold adapters. `PipelineRunner` nhận Gold job qua dependency injection và chỉ gọi Gold sau khi Bronze/Silver thành công. Vì vậy:

- `main.py` yêu cầu đủ ba stage: Bronze, Silver và Gold;
- Silver phải publish sáu target thật vào PostgreSQL trước khi Gold đọc;
- Gold kiểm tra `SilverSnapshotGate`, build candidate, validate rồi atomic publish;
- nếu Silver hoặc Gold thất bại, top-level result trả `status=FAILED` và `failed_stage` tương ứng;
- một live run Bronze -> Silver -> Gold cần SQL Server, PostgreSQL và schema/runtime adapters sẵn sàng.

Gold implementation và Gold stage hiện đã được nối vào application entry point. Trước publication, Gold vẫn giữ nguyên nguyên tắc fail-closed và không thay đổi version đang phục vụ nếu candidate không pass.

## Kết quả dữ liệu theo layer

| Layer | Vai trò | Kết quả |
|---|---|---|
| Bronze | Raw landing có audit và lineage | Dữ liệu nguồn, staging theo run/load, rejected-record evidence và retry-safe publication |
| Silver | Làm sạch và chuẩn hóa | Sáu analytical source tables cùng snapshot identity |
| Gold | Mô hình phân tích | Năm dimension, `fact_sales`, KPI và publication theo version |
| Dashboard | Consumption/reporting | Báo cáo hiệu suất bán hàng từ analytical output |

## Dashboard

Dashboard là lớp tiêu thụ dữ liệu ở cuối câu chuyện. Nó trình bày các chỉ số Sales Performance cho người dùng nghiệp vụ dựa trên các bảng analytical đã được kiểm tra ở Silver/Gold.

![Sales Performance Dashboard](Dashboard/SalesPerformanceDashboard.png)

## Cấu trúc repository

```text
src/
├── app/                 # orchestration, pipeline runner và snapshot gates
├── core/                # settings và compatibility shells
├── shared/              # connectors, ingestion contracts và services dùng chung
├── features/
│   ├── Sales_Performance/ # Sales Bronze/Silver/Gold và domain logic
│   ├── Person/            # Person Bronze ownership
│   └── Production/        # Product/Production Bronze ownership
└── utils/               # logging và helper utilities

scripts/
├── source/              # extraction/profiling từ source system
├── ingestion/           # operational ingestion wrappers
├── transformation/      # Silver transformations
└── warehouse/           # PostgreSQL schema, DDL và Gold adapters

tests/                   # contract, unit và integration-oriented tests
docs/                    # architecture, execution evidence và project notes
notebooks/               # exploratory và analytical notebooks
Dashboard/               # dashboard assets và preview image
docker-compose.yml       # PostgreSQL warehouse local
main.py                  # application entry point
requirements.txt         # Python dependencies
```

## Cấu hình và runtime

Settings nằm ở [src/core/settings.py](src/core/settings.py), dùng `pydantic-settings`, đọc `.env` và environment variables không phân biệt hoa thường. Các mặc định quan trọng:

```text
SQL Server: localhost:1433 / AdventureWorks2012 / Windows authentication
PostgreSQL: localhost:5432 / adventureworks_warehouse
batch_size: 10000
retry_max_attempts: 3
silver_rejected_threshold: 0
silver_transform_version: silver-v1
```

PostgreSQL được provision bởi `docker-compose.yml` và khởi tạo các schema `bronze`, `bronze_staging`, `silver`, `gold` cùng metadata tables. Docker Compose không provision SQL Server hoặc database AdventureWorks nguồn.

Một chi tiết vận hành quan trọng: một số PostgreSQL-backed service có thể khởi tạo ingestion schema ngay trong constructor. Vì vậy PostgreSQL cần sẵn sàng trước khi tạo default `App`.

## Chạy ứng dụng

Sử dụng repository-local Python environment:

```powershell
cd "A:\Workspace\DataEngineer\AdventureWorks Analytics Platform"
.\.venv\Scripts\Activate.ps1
```

Chạy application:

```powershell
python main.py
```

Prerequisites:

- Python 3.11 environment trong `.venv`;
- PostgreSQL warehouse đang chạy;
- SQL Server có database `AdventureWorks2012`;
- ODBC Driver 17 for SQL Server;
- cấu hình `.env` phù hợp với máy chạy.

## Kiểm thử

Chạy toàn bộ regression suite:

```powershell
python -m pytest -q
```

Các nhóm test chính bao phủ:

- settings và architecture contracts;
- Bronze extraction, staging, audit, quarantine, retry, checkpoint và publish;
- Silver transformation, rejection, deduplication và publication gate;
- Gold builders, fact grain, validation, KPI, constraint, retry, rerun và publish preservation;
- connector và PostgreSQL integration behavior khi môi trường database sẵn sàng.

## Trạng thái xác nhận

- Phase 4A foundation: đã triển khai.
- Phase 4B Bronze runtime: đã triển khai với persistent audit/quarantine, staging, retry/reconciliation và atomic publish.
- Phase 4C Silver runtime: đã triển khai với chunked deterministic processing, validation, deduplication, checkpoint và publication gate.
- Phase 4D Gold runtime: đã triển khai với injectable job, Silver snapshot gate, candidate staging, integrity/KPI validation, constraint verification và atomic publication.
- Gold focused tests: 52 tests theo evidence hiện có.
- Repository regression: 183 tests theo lần validation gần nhất.

## Tài liệu liên quan

- [Architecture refactor summary](ARCHITECTURE_REFACTOR_SUMMARY.md)
- [Phase 4A execution plan](docs/project/PHASE_4A_FOUNDATION_EXECUTION_VI.md)
- [Phase 4B Bronze execution spec](docs/internal/phase4b_bronzelayer_execution_spec.md)
- [Phase 4C Silver execution spec](docs/internal/phase4c_silverlayer_execution_vi.md)
- [Phase 4D Gold execution spec](docs/internal/phase4d_goldlayer_execution_vi.md)
- [Working standards](docs/internal/WORKING_STANDARDS.md)

## Ghi chú

Repository này độc lập với thư mục Python legacy ở cấp workspace. Các compatibility shell còn lại chỉ phục vụ import/entry point cũ; source of truth hiện tại nằm trong `src/app`, `src/shared` và `src/features`.
