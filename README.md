# Dữ liệu cháy rừng Mỹ trên OCI Object Storage

Pipeline tải bộ dữ liệu [US Wildfire Dataset trên Kaggle](https://www.kaggle.com/datasets/firecastrl/us-wildfire-dataset) theo từng lô lên OCI Object Storage. Dữ liệu dùng để phân tích và dự báo cháy rừng.

| Bucket | Nội dung | Dùng để |
| --- | --- | --- |
| `raw-zone` | Các file CSV trong `wildfire/source_sha256=<mã nguồn>/rows_<bắt đầu>_<kết thúc>.csv` | Đọc dữ liệu để phân tích, huấn luyện mô hình |
| `system-state` | `wildfire_pipeline/pointer.json` | Pipeline theo dõi dòng đã tải; người phân tích không cần truy cập |

Mỗi lần chạy, `crawl-data/daily_pump.py` tải tối đa 1 triệu bản ghi CSV và cập nhật con trỏ sau khi tải thành công. Lịch GitHub Actions được cấu hình lúc **20:00 giờ Việt Nam**; lịch chỉ tự chạy khi workflow nằm trên nhánh mặc định và các GitHub Secrets/Variables đã được thiết lập. Xem [hướng dẫn cấu hình OCI và pipeline](crawl-data/OBJECT_STORAGE_SETUP.md).

## Cách 1: Xem hoặc tải CSV trong Oracle Cloud Console

1. Đăng nhập [OCI Console](https://cloud.oracle.com/), chọn vùng **Singapore (ap-singapore-1)**.
2. Vào **Storage → Buckets**, chọn compartment chứa bucket rồi mở **`raw-zone` → Objects**.
3. Mở `wildfire/` và thư mục `source_sha256=...`, sau đó chọn một file `rows_....csv`.
4. Chọn **Download** trong menu của file để mở CSV bằng Excel, pandas hoặc công cụ khác.

Bucket đang ở chế độ riêng tư. Người không có tài khoản OCI cần được chủ bucket cấp **link Pre-Authenticated Request (PAR) chỉ đọc** cho file hoặc tiền tố `wildfire/`. Ai có link đều có thể đọc dữ liệu trong phạm vi link cho đến khi hết hạn hoặc bị thu hồi; không đăng link lên Git. Link PAR tải CSV qua HTTPS, không phải cặp khóa S3 cho notebook. [Hướng dẫn tải object](https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/managingobjects_topic-To_download_an_object_from_a_bucket.htm) · [Hướng dẫn PAR](https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/usingpreauthenticatedrequests.htm)

## Cách 2: Xem CSV dạng bảng bằng Jupyter Notebook

Notebook [read_oci.ipynb](crawl-data/read_oci.ipynb) liệt kê các file CSV trong `raw-zone`, đọc thử tối đa 1.000 dòng của file được chọn và hiển thị bằng pandas. Người vận hành có thêm ô tùy chọn để xem con trỏ trong `system-state`.

Để truy cập bucket riêng tư bằng code, mỗi người cần **cặp OCI Customer Secret Key** của tài khoản được cấp quyền đọc và liệt kê object trong `raw-zone`. Không chia sẻ cặp khóa ghi dữ liệu đang dùng cho GitHub Actions. Tạo `crawl-data/.env`:

```powershell
Copy-Item crawl-data/.env.example crawl-data/.env
```

Điền `S3_ACCESS_KEY` và `S3_SECRET_KEY` vào `crawl-data/.env`. Endpoint và tên bucket đã có trong file mẫu. `.env` được Git bỏ qua. Sau đó cài thư viện và mở notebook:

```powershell
python -m pip install -r crawl-data/requirements-notebook.txt
python -m jupyter lab crawl-data
```

Trong Jupyter, mở `read_oci.ipynb` và chạy các ô từ trên xuống. Notebook chỉ đọc dữ liệu. Với nhiều triệu dòng, chỉ xem mẫu bằng pandas; để xử lý toàn bộ dữ liệu, dùng PySpark với đường dẫn `s3a://raw-zone/wildfire/...` và cùng endpoint/quyền đọc S3.

## Dành cho người vận hành pipeline

Script tải dữ liệu là [daily_pump.py](crawl-data/daily_pump.py); workflow là [.github/workflows/daily-wildfire-pump.yml](.github/workflows/daily-wildfire-pump.yml). `KAGGLE_API_TOKEN` chỉ cần cho pipeline tải từ Kaggle, không cần để đọc CSV đã có trên OCI. Giữ `system-state` và khóa ghi riêng cho pipeline. Hướng dẫn thiết lập GitHub Secrets/Variables nằm trong [OBJECT_STORAGE_SETUP.md](crawl-data/OBJECT_STORAGE_SETUP.md).
