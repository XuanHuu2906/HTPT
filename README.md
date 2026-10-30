# Railway Distributed System — Nhóm 6

**Trạng thái hiện tại: base Maven trống, chưa có mã nguồn ứng dụng hoặc test.**

Theo yêu cầu BASE-CLEANUP-01 ngày 29/10/2026, đã bỏ toàn bộ Java, class đã biên dịch, test, SQL nghiệp vụ, giao diện mẫu, script và cấu hình chạy demo. Giữ cấu trúc module, dependency nền và tài liệu để nhóm triển khai từ đầu.

## Cấu trúc base

| Module / thư mục | Mục đích khi triển khai |
|---|---|
| `shared/` | DTO, codec và giao thức dùng chung — TV3 |
| `backend-node/` | Backend dùng chung cho 5 instance — TV2–TV5 |
| `desktop-client/` | JavaFX Client — TV1 |
| `desktop-monitor/` | JavaFX Monitor — TV6 |
| `integration-tests/` | Kiểm thử tích hợp — cả nhóm |
| `database/`, `configs/`, `infra/`, `scripts/` | Thư mục giữ chỗ, chưa có cấu hình hoặc chương trình |
| `docs/specs/` | 9 SPEC ở trạng thái APPROVED-v1 |
| `artifacts/` | Hồ sơ rà soát và kiểm tra trước đây; không phải trạng thái runtime hiện tại |

Các thư mục source/package được giữ bằng `.gitkeep`, không có class mẫu, DTO, interface hoặc stub. Root POM giữ 5 child module; các module giữ dependency nền theo kiến trúc đã chọn. Chưa cấu hình main class, executable JAR hoặc JavaFX launcher.

## Bắt đầu làm việc

1. Đọc [AGENTS.md](AGENTS.md) và [MASTER SPEC](docs/specs/00-MASTER.md), sau đó các SPEC theo thứ tự quy định.
2. Mở root `pom.xml` dưới dạng Maven project. Toolchain cấu hình: JDK 25; dependencies JavaFX, Jackson, pgJDBC và JUnit được quản lý trong POM.
3. Triển khai module theo ownership và contract đã phê duyệt. Bộ base hiện không cung cấp chức năng đặt vé, TCP, Raft hay truy cập DB.

Để kiểm tra Maven model khi máy đã cài Maven/JDK:

```text
mvn -B -ntp validate
```

Build base không chứng minh tính năng hoặc AC đã đạt. Chưa có lệnh chạy ứng dụng, khởi động database hay demo cụm vì chưa có implementation.

## Tài liệu và kiểm tra

- [Bản đồ thư mục hiện tại](docs/FILE_MAP.md).
- [Hướng dẫn base](docs/RUNBOOK.md).
- [Báo cáo dọn base](docs/reviews/BASE-CLEANUP-01.md).

SPEC giữ nguyên kiến trúc Java/JavaFX Desktop, TCP Socket, PostgreSQL riêng từng Node và Raft 5 voter. Nội dung code/SQL minh họa trong tài liệu là đặc tả để tham khảo, không phải file implementation còn tồn tại. Các tài liệu bootstrap cũ được giữ và đánh dấu lịch sử; khi khác SPEC, dùng `docs/specs/` làm nguồn chuẩn.
# HTPT
