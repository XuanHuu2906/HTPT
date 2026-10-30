# Bản đồ bộ base

Trạng thái sau BASE-CLEANUP-01: giữ 5 module Maven và thư mục source rỗng; không có file Java hoặc test.

| Module / thư mục | Đường dẫn giữ chỗ |
|---|---|
| shared | `src/main/java/vn/nhom6/railway/protocol/dto/`, `transport/` |
| backend-node | `src/main/java/vn/nhom6/railway/`: booking, persistence, networking/fault, node, raft/core, raft/election, raft/replication, raft/recovery |
| desktop-client | `src/main/java/vn/nhom6/railway/client/`, `src/main/resources/` |
| desktop-monitor | `src/main/java/vn/nhom6/railway/monitor/` |
| integration-tests | `src/test/java/vn/nhom6/railway/tests/` |
| database, configs, infra, scripts | Thư mục rỗng cho triển khai sau |

Mỗi module giữ `pom.xml`; các thư mục trống được giữ bằng `.gitkeep`. Các package là vị trí dành cho triển khai sau; tên thư mục không chứng minh module đã có chức năng.

Danh sách file hiện hành, không gồm evidence và build output: [PROJECT_FILE_MANIFEST.txt](../PROJECT_FILE_MANIFEST.txt). Kiến trúc mục tiêu và ownership: [SPEC-02](specs/02-ARCHITECTURE.md), [AGENTS.md](../AGENTS.md).
