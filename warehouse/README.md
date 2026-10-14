# Data Warehouse cháy rừng

## 1. Mục tiêu và vị trí của nhánh

Thư mục `warehouse/` triển khai kho dữ liệu phân tích cho bộ dữ liệu dự báo cháy rừng tại Mỹ. Thiết kế dùng MySQL 8.4 theo mô hình star schema; PySpark đảm nhiệm đọc, kiểm tra và chuẩn hóa các batch CSV do pipeline crawling tạo ra.

Nhánh `feature/data-crawling` đã hoàn thành phần thu thập ban đầu và được merge vào `main`. Crawler lấy CSV từ Kaggle, chia dữ liệu thành các lô (mặc định tối đa 1.000.000 dòng/lô), tải các lô lên OCI Object Storage bucket `raw-zone`, đồng thời lưu trạng thái con trỏ ở bucket `system-state`. Nhánh Data Warehouse tiếp tục từ `main` sau phần merge đó: nó tiêu thụ các object trong `raw-zone`, không thay thế hoặc chạy lại crawler.

```text
Kaggle
  └─ crawler (crawl-data/, nhánh data-crawling đã merge vào main)
       └─ OCI Object Storage: raw-zone/wildfire/...
            └─ PySpark ETL (warehouse/scripts/etl.py)
                 ├─ kiểm tra và chuẩn hóa
                 ├─ MySQL staging và audit/reject
                 └─ dim_date + dim_location + fact_wildfire
```

ETL chỉ chạy được khi crawler đã tải object, máy chạy ETL có quyền đọc bucket, MySQL đã được khởi tạo và cấu hình Spark/JDBC phù hợp. Trạng thái một lần chạy crawler thành công không tự chứng minh toàn bộ dataset đã được tải hoặc ETL đã nạp xong.

## 2. Phần việc Data Warehouse đã triển khai

- Hoàn thiện schema MySQL cho `dim_date`, `dim_location` và `fact_wildfire`; thêm khóa ngoại, khóa duy nhất cho tọa độ và các ràng buộc nhãn/toạ độ/ngày.
- Thêm lineage vào fact qua `source_record_id`, `source_sha256`, `source_object_key`, để truy ngược observation về batch CSV nguồn.
- Thêm `etl_batch` lưu trạng thái, thời gian, số dòng đọc/đạt/loại, số fact mới và lỗi của từng object.
- Thêm staging `stg_wildfire`, staging reject `stg_wildfire_reject` và bảng lưu dòng lỗi `etl_reject`.
- Xây dựng ETL PySpark theo batch, có kiểm tra metadata, cấu trúc CSV, số dòng, dữ liệu ngày/nhãn/toạ độ và các trường số.
- Hỗ trợ bỏ qua batch đã thành công, chạy lại batch lỗi và chủ động chạy lại batch thành công bằng `--reprocess`. Khóa định danh nguồn giúp chạy lại không nhân đôi fact.
- Bổ sung migration chạy lại an toàn cho database đã tạo bằng schema warehouse ban đầu, file cấu hình mẫu, danh sách thư viện và hướng dẫn cài đặt/chạy/đối soát.
- Bổ sung truy vấn SQL phân tích cơ bản và các kiểm tra chất lượng dữ liệu.

## Báo cáo sơ bộ Data Warehouse cho các thành viên

### Cơ sở dữ liệu hiện có những bảng nào?

Theo DDL hiện tại, database `wildfire_dw` có **7 bảng với tổng cộng 79 cột**. Số này đếm cột dữ liệu, không tính index, primary key, foreign key hay các ràng buộc `CHECK`.

| Bảng | Số cột | Dùng để làm gì? |
| --- | ---: | --- |
| `dim_date` | 5 | Lưu ngày, năm, tháng, ngày trong tháng và `date_key` dạng `YYYYMMDD`. |
| `dim_location` | 3 | Lưu khóa vị trí và latitude/longitude duy nhất. |
| `fact_wildfire` | 23 | Bảng phân tích chính: một dòng cho mỗi observation hợp lệ, có ngày, vị trí, nhãn cháy, 15 biến thời tiết/chỉ số và lineage nguồn. |
| `etl_batch` | 13 | Theo dõi từng file nguồn: khoảng dòng, trạng thái, số dòng đọc/đạt/loại, số fact mới, thời gian và thông báo lỗi. |
| `stg_wildfire` | 23 | Vùng đệm tạm cho dòng hợp lệ trước khi ghi vào dimension và fact. |
| `stg_wildfire_reject` | 6 | Vùng đệm tạm cho dòng không hợp lệ, kèm lý do và payload để chẩn đoán. |
| `etl_reject` | 6 | Lưu reject sau khi batch hoàn tất để nhóm tra cứu về sau. |
| **Tổng** | **79** | **3 bảng mô hình phân tích, 1 bảng audit batch, 2 bảng staging và 1 bảng reject lâu dài.** |

Trong `fact_wildfire`, 15 biến dữ liệu là `pr`, `rmax`, `rmin`, `sph`, `srad`, `tmmn`, `tmmx`, `vs`, `bi`, `fm100`, `fm1000`, `erc`, `etr`, `pet`, `vpd`. Ba cột lineage là `source_record_id`, `source_sha256`, `source_object_key`. Các cột còn lại lưu khóa fact, khóa ngày/vị trí, thời điểm và nhãn `wildfire`.

**Lưu ý trạng thái:** schema và migration đã được viết trong repository. Môi trường làm việc chưa kết nối MySQL, nên chưa xác nhận các bảng đã được tạo trong một MySQL server thật hoặc đã có dữ liệu thật bên trong.

### ETL chạy theo từng bước như thế nào?

1. **Extract – lấy dữ liệu nguồn:** ETL liệt kê các object `wildfire/...` trong OCI bucket `raw-zone`. Mỗi object phải có tên chứa source hash và khoảng dòng, ví dụ `rows_1_1000000.csv`; metadata object phải khớp với tên file.
2. **Kiểm tra tập batch trước khi đọc:** các file phải thuộc cùng một source hash, bắt đầu từ dòng 1 và có khoảng dòng liên tục, không chồng lấn. Thiếu batch hoặc trộn hai phiên bản nguồn làm ETL dừng trước khi ghi dữ liệu.
3. **Đọc CSV và chuẩn hóa tên cột:** Spark đọc CSV có header; tên được trim và chuyển về chữ thường để so khớp. Thiếu cột bắt buộc hoặc có hai cột trùng tên sau chuẩn hóa thì batch bị dừng. Cột bổ sung được giữ trong fingerprint của dòng để lineage ổn định, nhưng không thêm thành cột fact.
4. **Transform – chuẩn hóa kiểu dữ liệu:**
   - Thử parse `datetime` theo các định dạng đã hỗ trợ; thời gian có timezone được quy về UTC, thời gian không có timezone được hiểu là UTC.
   - Chuẩn hóa `Wildfire` thành `1` hoặc `0` từ `1/0`, `1.0/0.0`, `true/false`, `yes/no`.
   - Parse latitude, longitude và 15 biến thời tiết thành số. Tọa độ được chuyển sang `DECIMAL(9,6)`; các biến thời tiết giữ kiểu số thực và được phép NULL.
   - Tạo `source_record_id` ổn định từ object nguồn, nội dung dòng và thứ tự của các dòng trùng hệt nhau. Điều này giữ được số lượng duplicate trong nguồn mà vẫn giúp nạp lại an toàn.
5. **Kiểm tra từng dòng:** dòng hợp lệ đi tiếp; dòng lỗi được gắn một hoặc nhiều lý do, ví dụ `invalid_datetime`, `latitude_out_of_range` hoặc `invalid_wildfire_label`, rồi chuyển vào luồng reject.
6. **Load – ghi vào MySQL staging:** các dòng hợp lệ được JDBC ghi vào `stg_wildfire`; dòng reject được ghi vào `stg_wildfire_reject`. ETL so sánh số dòng Spark đọc với số dòng khai báo trong tên object; lệch số lượng làm batch thất bại.
7. **Ghi dimension và fact trong transaction:** MySQL thêm ngày/vị trí chưa có, sau đó insert fact có `source_record_id` chưa tồn tại. Reject được chuyển sang `etl_reject`, trạng thái và số liệu batch được cập nhật, rồi staging được dọn. Nếu transaction lỗi, các thay đổi ở bước cuối rollback; nếu retry thất bại, reject của lần thành công trước vẫn còn.

### Có kiểm tra ràng buộc dữ liệu không?

**Có, ở ba lớp.** Cần phân biệt kiểm tra cấu trúc của cả batch với kiểm tra chất lượng từng dòng:

| Lớp kiểm tra | Điều kiện đang kiểm tra | Khi không đạt |
| --- | --- | --- |
| File/batch nguồn | Đường dẫn và metadata có cùng hash/range; một phiên bản nguồn; khoảng dòng liên tục từ 1; số dòng CSV khớp khoảng khai báo. | Dừng batch hoặc toàn lượt trước khi nạp; kiểm tra file/pointer crawler. |
| Từng dòng trong Transform | Ngày parse được; latitude/longitude có giá trị, chuyển được sang `DECIMAL(9,6)` và nằm trong phạm vi; nhãn chuyển được về 0/1; biến thời tiết nếu có giá trị phải parse được và hữu hạn. | Ghi dòng và lý do vào reject; các dòng hợp lệ còn lại trong batch vẫn có thể được nạp. |
| Ràng buộc database và đối soát sau nạp | Khóa chính/duy nhất, tọa độ duy nhất, khóa ngoại fact tới date/location, nhãn 0/1, ngày và tọa độ đúng phạm vi; tổng read bằng accepted + rejected. | MySQL từ chối thao tác vi phạm; truy vấn `quality_checks.sql` giúp tìm sai lệch sau khi nạp. |

Ở bản hiện tại, ETL **chưa áp đặt ngưỡng nghiệp vụ riêng cho từng biến thời tiết** (ví dụ `rmax` trong 0–100), vì cần đối chiếu tài liệu/header và đơn vị của dataset thật trước khi đặt ngưỡng để tránh loại nhầm dữ liệu. Các giá trị thời tiết rỗng/NULL được giữ là NULL; chuỗi không phải số, NaN và vô cực bị reject. Chất lượng thời tiết còn được xem qua tỷ lệ NULL và truy vấn phân tích.

Sau khi load, `warehouse/sql/quality_checks.sql` kiểm tra trạng thái batch, lý do reject, cân bằng số dòng, fact thiếu source identity, tọa độ sai và chênh ngày giữa `fact_wildfire` với `dim_date`. Các truy vấn phân tích mẫu nằm trong `warehouse/sql/queries.sql`.

## 3. Cấu trúc thư mục

```text
warehouse/
├── .env.example                    # Cấu hình mẫu, không chứa credentials thật
├── README.md                       # Tài liệu này
├── requirements.txt                # Thư viện Python cho ETL
├── scripts/
│   └── etl.py                      # Đọc OCI, kiểm tra và nạp theo batch
└── sql/
    ├── schema.sql                  # Tạo mới database và toàn bộ bảng
    ├── migration_001_etl_support.sql # Nâng cấp schema warehouse cũ
    ├── test_data.sql                # Dữ liệu giả lập để kiểm tra thủ công
    ├── queries.sql                  # Truy vấn phân tích mẫu
    └── quality_checks.sql           # Đối soát số liệu và chất lượng
```

Ở repo còn có `crawl-data/` và hướng dẫn OCI của nhóm crawler. Phần đó là upstream tạo dữ liệu nguồn; không đặt credentials trong README hay commit file `.env`.

## 4. Mô hình dữ liệu và grain

Database là `wildfire_dw`.

| Bảng | Vai trò và khóa chính |
| --- | --- |
| `dim_date` | Một dòng cho mỗi ngày; `date_key` theo dạng `YYYYMMDD`, `full_date` là duy nhất. |
| `dim_location` | Một dòng cho mỗi cặp tọa độ; `location_key` tự tăng, cặp latitude/longitude là duy nhất. |
| `fact_wildfire` | Một dòng cho mỗi observation nguồn hợp lệ; liên kết ngày và tọa độ qua khóa ngoại. |
| `etl_batch` | Một dòng trạng thái cho mỗi object CSV nguồn. |
| `stg_wildfire` | Vùng đệm các dòng hợp lệ trong batch đang chạy; được dọn sau khi batch hoàn tất. |
| `stg_wildfire_reject` | Vùng đệm các dòng không hợp lệ trước khi ghi vào bảng reject lâu dài. |
| `etl_reject` | Lưu nội dung và lý do loại để có thể điều tra sau ETL. |

Grain của fact là **một observation trong nguồn**, không phải một dòng cho mỗi ngày hoặc tọa độ. Vì vậy hai observation có cùng thời gian và vị trí vẫn có thể là hai dòng fact riêng nếu chúng là hai dòng nguồn riêng. Khóa `fact_id` là khóa thay thế; `source_record_id` là khóa lineage/idempotency do ETL tạo. Các fact cũ hoặc dữ liệu giả lập có thể có `source_record_id = NULL`.

Fact chứa nhãn `wildfire` và 15 biến môi trường/chỉ số: `pr`, `rmax`, `rmin`, `sph`, `srad`, `tmmn`, `tmmx`, `vs`, `bi`, `fm100`, `fm1000`, `erc`, `etr`, `pet`, `vpd`.

## 5. Hợp đồng dữ liệu đầu vào

ETL tìm object theo mẫu:

```text
wildfire/source_sha256=<64 ký tự hex>/rows_<dòng đầu>_<dòng cuối>.csv
```

Metadata object phải có `source-sha256`, `row-start`, `row-end` tương ứng với đường dẫn. Tập object được chấp nhận phải thuộc cùng một source hash, bắt đầu từ dòng 1, không thiếu hoặc chồng lấn khoảng dòng. Mỗi CSV phải có đủ số dòng theo khoảng khai báo. Nếu không thỏa các điều kiện này, ETL dừng trước khi nạp để tránh trộn phiên bản hoặc tạo báo cáo thiếu dữ liệu.

Các cột bắt buộc là:

```text
latitude, longitude, datetime, Wildfire,
pr, rmax, rmin, sph, srad, tmmn, tmmx, vs,
bi, fm100, fm1000, erc, etr, pet, vpd
```

Tên cột được chuẩn hóa không phân biệt hoa/thường. Cột nguồn bổ sung được đưa vào fingerprint của dòng để định danh nguồn ổn định.

- `datetime`: hỗ trợ các định dạng ngày phổ biến, ISO và một số định dạng `MM/dd/yyyy`; datetime có timezone được quy đổi theo UTC, datetime không timezone được hiểu là UTC.
- `Wildfire`: chấp nhận `0`, `1`, `0.0`, `1.0`, `false`, `true`, `no`, `yes`.
- Tọa độ là bắt buộc; latitude phải trong `[-90, 90]`, longitude trong `[-180, 180]` và có thể lưu với độ chính xác 6 chữ số thập phân.
- Các biến thời tiết có thể NULL; chuỗi số không parse được hoặc giá trị vô cực/NaN bị đưa vào reject.
- Dòng sai ngày, tọa độ hoặc nhãn bị từ chối với lý do cụ thể; ETL vẫn ghi nhận số dòng đọc và số dòng bị loại.

`source_record_id` là SHA-256 từ object key, fingerprint payload dòng và ordinal của các dòng trùng hoàn toàn trong object. Ordinal giữ nguyên số lượng các dòng trùng mà không dựa vào Spark partition ID; nạp lại cùng object tạo lại cùng tập ID.

> Cần xác minh header và quy ước timezone với object thật trước khi chốt kết quả phân tích. Repository hiện không lưu CSV dữ liệu thật hoặc credentials OCI.

## 6. Cài đặt và cấu hình

### Thành phần cần có

- MySQL Server 8.4 và một tài khoản ETL có quyền đọc/ghi database `wildfire_dw`.
- Python 3.10 trở lên; các thư viện Python trong `requirements.txt`.
- Java và một bản Spark tương thích với Java/Python trên máy chạy ETL.
- Quyền đọc object trong OCI bucket `raw-zone`.
- Kết nối mạng từ máy Spark tới OCI Object Storage và MySQL.
- Thư viện JVM `hadoop-aws` khớp đúng phiên bản Hadoop đi kèm Spark và MySQL Connector/J trên Spark classpath.

### Tạo database

Với database mới, chạy `warehouse/sql/schema.sql` bằng MySQL CLI:

```powershell
mysql -u root -p
```

Sau khi vào dấu nhắc MySQL:

```sql
SOURCE D:/duong-dan-repo/warehouse/sql/schema.sql;
```

Với database đã được tạo bằng schema warehouse ban đầu, chạy migration một lần:

```sql
SOURCE D:/duong-dan-repo/warehouse/sql/migration_001_etl_support.sql;
```

Migration kiểm tra cột/index lineage trước khi thêm và tạo các bảng ETL nếu chưa tồn tại; có thể chạy lại. Database `wildfire_dw` phải tồn tại trước khi chạy migration. Không chạy `test_data.sql` trên database dùng để nạp dữ liệu thật.

### Python và cấu hình cục bộ

Từ thư mục gốc repository:

```powershell
Copy-Item warehouse/.env.example warehouse/.env
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r warehouse/requirements.txt
```

Điền `warehouse/.env` bằng thông tin riêng của môi trường:

| Biến | Ý nghĩa |
| --- | --- |
| `S3_ENDPOINT` | OCI S3-compatible endpoint của namespace/region. |
| `S3_REGION` | Region ký request, ví dụ `ap-singapore-1`. |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | Customer Secret Key OCI có quyền đọc bucket. |
| `DATA_BUCKET` | Bucket CSV, mặc định `raw-zone`. |
| `MYSQL_HOST`, `MYSQL_PORT` | Địa chỉ MySQL và cổng, mặc định `3306`. |
| `MYSQL_DATABASE` | Database đích, mặc định `wildfire_dw`. |
| `MYSQL_USER`, `MYSQL_PASSWORD` | Tài khoản ETL MySQL. |
| `MYSQL_USE_SSL` | Bật/tắt SSL JDBC, mặc định tắt cho MySQL local. |
| `SPARK_MASTER` | Spark master, mặc định `local[*]`. |
| `SPARK_JARS_PACKAGES` | Maven coordinates cho `hadoop-aws` và MySQL Connector/J. |
| `ETL_JDBC_BATCH_SIZE` | Số dòng mỗi JDBC batch, mặc định `5000`. |
| `ETL_JDBC_PARTITIONS` | Số partition ghi JDBC, mặc định `2`. |

`.env` bị Git ignore. Chỉ commit `.env.example` không có secrets. Không ghi credentials trong command, log chia sẻ, README hoặc báo cáo.

Ví dụ cấu hình Maven packages (thay placeholder bằng phiên bản khớp với Spark đang cài):

```text
SPARK_JARS_PACKAGES=org.apache.hadoop:hadoop-aws:<hadoop-version>,com.mysql:mysql-connector-j:<connector-version>
```

Không đoán phiên bản Hadoop; mismatch có thể khiến Spark không tải được `s3a://` hoặc gặp lỗi JVM khi khởi tạo.

## 7. Chạy ETL

Từ thư mục gốc repo, bắt đầu bằng dry-run để liệt kê object, khoảng dòng và số dòng dự kiến. Lệnh này gọi OCI nhưng không mở MySQL hoặc Spark:

```powershell
python warehouse/scripts/etl.py --dry-run
```

Nạp thử một object:

```powershell
python warehouse/scripts/etl.py --max-batches 1
```

Nạp mọi object chưa có trạng thái thành công:

```powershell
python warehouse/scripts/etl.py
```

Chủ động chạy lại cả object đã thành công:

```powershell
python warehouse/scripts/etl.py --reprocess
```

`--max-batches N` giới hạn số object được xử lý trong lần chạy. Mỗi lần chỉ chạy một ETL process để tránh hai tiến trình cùng cập nhật trạng thái/staging.

### Các bước xử lý một batch

1. Liệt kê `wildfire/`, đọc metadata của các object và xác minh source hash/khoảng dòng, tính liên tục của tập batch.
2. Đọc CSV bằng Spark qua `s3a://`; kiểm tra header bắt buộc và số dòng thực tế.
3. Chuẩn hóa thời gian, nhãn và kiểu số; sinh ID ổn định cho từng dòng; tách dòng hợp lệ và dòng reject.
4. Ghi dòng hợp lệ vào `stg_wildfire`, dòng bị loại vào `stg_wildfire_reject` qua JDBC.
5. Trong transaction MySQL, thay tập reject cũ của object, bổ sung ngày/vị trí còn thiếu, insert fact chưa tồn tại, cập nhật `etl_batch`, rồi dọn staging.
6. Nếu xử lý lỗi, batch được đánh dấu `FAILED` để có thể chạy lại. Reject từ lần thành công trước chỉ bị thay trong transaction thành công, vì vậy retry lỗi không làm mất chẩn đoán cũ.

Object đã `SUCCEEDED` được bỏ qua ở lần chạy mặc định. Fact có unique key trên `source_record_id`, do đó reprocess không nhân đôi fact. Object không có dòng hợp lệ vẫn có thể hoàn tất với `records_rejected > 0`; cần xem reject trước khi đánh giá chất lượng.

## 8. Đối soát sau khi nạp

Chạy trong MySQL:

```sql
SOURCE D:/duong-dan-repo/warehouse/sql/quality_checks.sql;
SOURCE D:/duong-dan-repo/warehouse/sql/queries.sql;
```

Các kiểm tra cần xem:

1. `etl_batch`: batch mong muốn đều `SUCCEEDED`; không còn batch `RUNNING` bị bỏ dở.
2. Đối chiếu từng batch: `records_read = records_accepted + records_rejected`.
3. Xem `etl_reject` theo `rejection_reason`; xác định reject có hợp lệ hay do sai hợp đồng dữ liệu.
4. So sánh tổng `facts_inserted` và số fact theo lineage; chạy lại batch không làm số fact tăng.
5. `mismatched_dates = 0`, không có fact thiếu dimension key, không có tọa độ sai phạm vi.
6. Xem các truy vấn phân tích mẫu: tổng quan sát/nhãn, tỷ lệ cháy theo năm, thời tiết theo nhãn và quan sát theo vị trí.
7. Đối chiếu số dòng/range đã nạp với crawler pointer `system-state/wildfire_pipeline/pointer.json` để biết dataset đã hoàn tất hay chỉ là một phần.

`quality_checks.sql` cũng thống kê dữ liệu thiếu và observation trùng vị trí/thời điểm. Observation trùng không mặc nhiên là lỗi vì grain fact là một dòng nguồn.

## 9. Trạng thái và giới hạn tại thời điểm cập nhật tài liệu

Phần code schema, migration, kiểm tra chất lượng và ETL đã có trong nhánh Data Warehouse. Kiểm tra cú pháp Python (`py_compile`) đã thành công và `git diff --check` không báo lỗi whitespace. ETL với OCI/MySQL thật **chưa được xác nhận chạy thành công**: máy làm việc chưa có `warehouse/.env`/credentials, thiếu `python-dotenv` trong môi trường Python, và chưa cấu hình Spark/MySQL/JARs. Vì vậy chưa có số liệu thực tế để báo số fact đã nạp hoặc tỷ lệ reject.

Các bước còn lại trước khi báo hoàn tất dữ liệu thật:

1. Cài dependencies, điền `.env` cục bộ và khởi tạo schema/migration.
2. Đối chiếu crawler pointer với object trong OCI, xác minh header và timezone.
3. Chạy `--dry-run`, sau đó nạp thử một batch và kiểm tra reject/fact.
4. Chạy tất cả batch, thực hiện các đối soát ở mục 8 và lưu số liệu cho báo cáo nhóm.
5. Sau khi xác nhận upstream và lịch sử nhánh, cập nhật/push nhánh warehouse để nhóm cùng review.

README mô tả phạm vi và trạng thái nhánh tại thời điểm cập nhật; số liệu tải/nạp cần được bổ sung sau khi ETL thật chạy xong.
