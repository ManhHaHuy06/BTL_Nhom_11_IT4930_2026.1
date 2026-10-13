# Lưu dữ liệu Kaggle trên OCI Object Storage

OCI Object Storage là dịch vụ lưu file của Oracle. Script `daily_pump.py` dùng
giao diện S3 tương thích, nên không cần VM, Docker hay MinIO để chạy trên OCI.
GitHub Actions tải CSV từ Kaggle, ghi một lô tối đa 1 triệu bản ghi mỗi lần chạy,
và lưu con trỏ vào một bucket riêng.

## 1. Tạo hai bucket tại home region Singapore

Trong Oracle Cloud Console, chọn region **Singapore (ap-singapore-1)**, vào
**Storage > Buckets** và tạo hai bucket trong cùng compartment:

- `raw-zone`: file CSV dữ liệu, chọn **Standard** storage tier.
- `system-state`: file `wildfire_pipeline/pointer.json` để theo dõi dòng đã đọc.

Giữ cả hai bucket ở chế độ **Private**. Script có thể tự tạo bucket nếu tài khoản
đủ quyền, nhưng tạo sẵn trong Console giúp kiểm tra đúng region và storage tier.

## 2. Lấy S3 endpoint và Customer Secret Key

Trong trang chi tiết bucket, xem **Namespace** của Object Storage. Endpoint có dạng:

`https://<namespace>.compat.objectstorage.ap-singapore-1.oci.customer-oci.com`

Trong menu profile của Oracle Cloud, vào **User settings > Customer secret keys**
và tạo key mới. Lưu **Access Key** và **Secret Key** ở nơi an toàn. Secret Key
chỉ hiện một lần. Không đưa key vào repository hay ảnh chụp màn hình.

Tài khoản sử dụng key cần có quyền đọc/ghi object trong hai bucket. Nếu sử dụng
IAM user riêng cho pipeline, cấp quyền cần thiết cho user đó thay vì dùng tài
khoản quản trị.

## 3. Cài đặt GitHub Actions secrets

Trong repository, vào **Settings > Secrets and variables > Actions** và tạo:

| Secret | Giá trị |
| --- | --- |
| `KAGGLE_API_TOKEN` | Token Kaggle mới (nên đổi token đã từng chia sẻ) |
| `S3_ENDPOINT` | Endpoint ở bước 2 |
| `S3_ACCESS_KEY` | Access Key của Customer Secret Key |
| `S3_SECRET_KEY` | Secret Key của Customer Secret Key |

`S3_REGION` được đặt là `ap-singapore-1` trong workflow. Kaggle dataset mặc định
là `firecastrl/us-wildfire-dataset`; chỉ cần `KAGGLE_ENDPOINT` nếu muốn đổi dataset.

Để thử ở máy cá nhân, đặt các biến trên trong `.env` (file này đã được `.gitignore`).
Script vẫn chấp nhận `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`
trong `.env` cũ để thử MinIO local; `S3_*` được ưu tiên khi cùng tồn tại.

## 4. Chạy thử và bật lịch

Workflow hiện ở nhánh `feature/data-crawling`. Sau khi workflow được đưa vào
nhánh mặc định `main`, chạy thử qua **Actions > Daily US wildfire Kaggle to Object
Storage > Run workflow**. Kiểm tra file CSV trong `raw-zone` và pointer trong
`system-state`. Lịch `0 13 * * *` là khoảng **20:00 giờ Việt Nam**; GitHub có
thể bắt đầu trễ hơn giờ này.

Workflow theo lịch chỉ chạy khi file workflow nằm trên nhánh mặc định. Vì vậy,
cần merge nhánh tính năng vào `main` sau khi đã cấu hình và thử kết nối.

## 5. Chia sẻ dữ liệu cho nhóm

Trong OCI Console, tạo **Pre-Authenticated Request** với quyền đọc và thời hạn
phù hợp cho file CSV cần chia sẻ. Người có URL có thể tải file qua HTTPS mà
không cần tài khoản Oracle. Không tạo link đọc cho bucket `system-state`.

## Lưu ý về dữ liệu mới và hạn mức

Script hiện tại báo lỗi nếu CSV nguồn trên Kaggle thay đổi (kể cả chỉ thêm dòng)
để tránh bỏ sót hoặc trùng lặp. Nếu dataset tiếp tục được cập nhật sau khi nạp
hết 9,5 triệu bản ghi ban đầu, cần bổ sung xử lý phiên bản nguồn trước khi bật
lịch dài hạn.

OCI Always Free gồm 20 GB Object Storage tổng cộng. Trong giai đoạn Free Trial,
Oracle liệt kê 10 GB miễn phí cho Standard tier; cần theo dõi dung lượng khi
dữ liệu tăng. Nếu vượt 20 GB lúc Free Trial kết thúc mà không nâng cấp, Oracle
cảnh báo các object có thể bị xóa.

Tài liệu Oracle:

- https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/s3compatibleapi.htm
- https://docs.oracle.com/en-us/iaas/Content/Object/Tasks/usingpreauthenticatedrequests.htm
- https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm
