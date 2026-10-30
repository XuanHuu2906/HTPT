# Hướng dẫn bộ base

Repository hiện chỉ giữ Maven/modules, thư mục trống và tài liệu. Java, test, migration SQL, giao diện mẫu và cấu hình/script chạy demo đã được gỡ theo yêu cầu người dùng.

## Mở dự án

1. Chuẩn bị JDK 25 và Maven theo toolchain của nhóm.
2. Import `pom.xml` ở thư mục gốc; reactor gồm `shared`, `backend-node`, `desktop-client`, `desktop-monitor`, `integration-tests`.
3. Đọc [MASTER SPEC](specs/00-MASTER.md) trước khi thêm implementation.

## Kiểm tra cấu trúc Maven

```text
mvn -B -ntp validate
```

Lệnh chỉ kiểm tra Maven model. Khi thêm code/test, chạy build và test phù hợp; build trống không chứng minh AC01–AC10. Chưa có entrypoint để chạy `java -jar` hoặc `javafx:run`.

## Triển khai sau này

Môi trường DB, config và script khởi động cụm cần được tạo theo [SPEC-08](specs/08-DEPLOYMENT.md). Các lệnh demo cũ không còn áp dụng cho base hiện tại. Không tạo kết quả thành công giả để thay thế phần chưa triển khai.

Kết quả kiểm tra thực tế của lượt dọn base được ghi tại [BASE-CLEANUP-01](reviews/BASE-CLEANUP-01.md).
