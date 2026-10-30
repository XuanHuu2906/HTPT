# BASE-CLEANUP-01 — Đưa repository về base

Ngày: 29/10/2026. Căn cứ: người dùng chọn “Giữ Maven/modules và tài liệu; bỏ toàn bộ Java, test, SQL nghiệp vụ và script/config chạy demo”.

## Thay đổi

- Xóa 45 file Java, gồm 42 source chính và 3 test; xóa 45 class đã biên dịch còn trong target.
- Bỏ SQL nghiệp vụ, 6 config properties, Compose demo, FXML/CSS mẫu và bản copy trong target, script khởi động/khởi tạo DB và các script kiểm tra scaffold/review cũ. Tổng cộng 106 file được xóa.
- Giữ 5 Maven module, dependency nền, cấu trúc source/package bằng `.gitkeep`, tài liệu và evidence dạng văn bản.
- Bỏ plugin đóng gói/chạy trỏ tới NodeMain, BookingDesktopApp và ClusterMonitorApp trong 3 POM. Không thêm class/stub mới.
- Cập nhật README, FILE_MAP, RUNBOOK, manifest. Tài liệu bootstrap cũ được đánh dấu lịch sử; source được dẫn trong review trước đây đã bị xóa theo yêu cầu này.

## Đối chiếu SPEC

Không thay nội dung hoặc trạng thái APPROVED-v1 của 9 SPEC. FR/AC và kiến trúc mục tiêu giữ nguyên; base hiện tại chưa triển khai chức năng, không dùng để tuyên bố AC đạt. Đây là yêu cầu xóa implementation hiện có của người dùng, không phải thay wire contract bằng một implementation khác.

## Kiểm tra

- `rtk proxy mvn -version`: exit 1, không có Maven trên PATH; kiểm tra bằng Maven **BLOCKED**. Không cài thêm công cụ trong lượt dọn base.
- `rtk proxy mvn -B -ntp validate`: **BLOCKED**, Maven không được tìm thấy; [output thực tế](../../artifacts/base-cleanup/2026-10-29/maven-validate.txt).
- Kiểm tra tĩnh XML/POM, số module, không còn source/binary/demo config, không còn mainClass và hash SPEC: xem [validation.txt](../../artifacts/base-cleanup/2026-10-29/validation.txt).
- [Danh sách file đã xóa và SHA-256](../../artifacts/base-cleanup/2026-10-29/deleted-files.json). Danh sách chỉ lưu tên/hash, không giữ bản sao source bị xóa.
- Runtime/unit/integration/E2E: **NOT_RUN**, không còn implementation/test để chạy.

## Trạng thái sau dọn

Base dùng để bắt đầu triển khai từ SPEC. Chưa có ứng dụng có thể chạy, DB migration hoặc demo cụm. Các báo cáo trước lượt dọn mô tả trạng thái lịch sử, không phải file/chức năng hiện có.

Trong kiểm tra ban đầu, target xuất hiện lại và chỉ chứa bản copy `.gitkeep`; không có `.class` hoặc tài nguyên ứng dụng. Kiểm tra cuối xác minh không còn build output thực thi, cho phép công cụ IDE tạo lại thư mục/placeholder trống.
