# D01–D10: trạng thái evidence theo nhiệm vụ

Base commit `adc4607b916cedcdbe654f6fdb6a5cdda229fa83`. Nguồn 60 nhiệm vụ: [task-source.json](task-source.json).

Phạm vi lượt này: TV2 Persistence + điểm nối TV3. NOT_REASSESSED nghĩa chưa nghiệm thu lại trong lượt này; không phủ định kết quả lịch sử. Không tự sửa trạng thái trong workbook nguồn.

| Task | Mục tiêu gốc | Trạng thái lượt này | Evidence |
|---|---|---|---|
| TV1-D01 | Chốt luồng bán vé trên giao diện | NOT_REASSESSED | [historical: TV1-D01-javafx-running.png](../D01/TV1-D01-javafx-running.png) |
| TV2-D01 | Chốt thực thể và quy tắc nghiệp vụ | PARTIAL | [review](../D01/TV2-D01-20261111-review.md) |
| TV3-D01 | Thu thập nhu cầu giao thức | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV4-D01 | Liệt kê bảo đảm an toàn Raft | NOT_REASSESSED | [historical: TV4-D01-maven-test.log](../D01/TV4-D01-maven-test.log), [historical: TV4-D01-maven-test.txt](../D01/TV4-D01-maven-test.txt) |
| TV5-D01 | Xác định vòng đời Node Raft | NOT_REASSESSED | [historical: TV5-D01-maven-test-jdk24-default.txt](../D01/TV5-D01-maven-test-jdk24-default.txt), [historical: TV5-D01-results.md](../D01/TV5-D01-results.md), [historical: TV5-D01-run-tests.ps1](../D01/TV5-D01-run-tests.ps1), [historical: TV5-D01-source-hashes.txt](../D01/TV5-D01-source-hashes.txt), [historical: TV5-D01-test-default-release.txt](../D01/TV5-D01-test-default-release.txt), [historical: TV5-D01-test-jdk24-override.txt](../D01/TV5-D01-test-jdk24-override.txt), [historical: TV5-D01-test-jdk25.txt](../D01/TV5-D01-test-jdk25.txt) |
| TV6-D01 | Tạo repo và quy tắc tích hợp | NOT_REASSESSED | [historical: TV6-D01-maven-validate.log](../D01/TV6-D01-maven-validate.log) |
| TV1-D02 | Khởi tạo JavaFX Desktop | NOT_REASSESSED | [historical: TV1-D02-main-navigation.png](../D02/TV1-D02-main-navigation.png), [historical: TV1-D02-seat-grid-empty.png](../D02/TV1-D02-seat-grid-empty.png), [historical: TV1-D02-seat-grid-selected.png](../D02/TV1-D02-seat-grid-selected.png) |
| TV2-D02 | Định nghĩa command và state machine | PASS_LOCAL | [review](../D02/TV2-D02-20261111-review.md) |
| TV3-D02 | Định nghĩa DTO dùng chung | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV4-D02 | Tạo mô hình state và log | NOT_REASSESSED | [historical: TV4-D02-maven-test.log](../D02/TV4-D02-maven-test.log) |
| TV5-D02 | Chốt quy tắc phiếu và election timeout | NOT_REASSESSED | [historical: TV5-D02-election-core-contract-test.log](../D02/TV5-D02-election-core-contract-test.log), [historical: TV5-D02-raft-regression.log](../D02/TV5-D02-raft-regression.log), [historical: TV5-D02-run-tests.ps1](../D02/TV5-D02-run-tests.ps1), [historical: TV5-D02-vote-rules.log](../D02/TV5-D02-vote-rules.log) |
| TV6-D02 | Khóa môi trường phát triển | NOT_REASSESSED | [historical: TV6-D02-config-check.log](../D02/TV6-D02-config-check.log), [historical: TV6-D02-verify-g1-build.log](../D02/TV6-D02-verify-g1-build.log) |
| TV1-D03 | Hoàn thành mock luồng đặt vé | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV2-D03 | Chốt API Booking–Raft–DB | PARTIAL | [review](../D03/TV2-D03-20261111-review.md) |
| TV3-D03 | Đóng băng Protocol v1 | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV4-D03 | Chốt Raft Core API với TV5/TV2 | NOT_REASSESSED | [historical: TV4-D03-maven-test.log](../D03/TV4-D03-maven-test.log) |
| TV5-D03 | Hợp nhất API Election với Raft Core | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV6-D03 | Nghiệm thu contract và build | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV1-D04 | Thiết kế CSS và các trạng thái | NOT_REASSESSED | [historical: TV1-D04-disconnected.png](../D04/TV1-D04-disconnected.png), [historical: TV1-D04-error.png](../D04/TV1-D04-error.png), [historical: TV1-D04-loading.png](../D04/TV1-D04-loading.png), [historical: TV1-D04-mock-change.png](../D04/TV1-D04-mock-change.png), [historical: TV1-D04-seat-4-states.png](../D04/TV1-D04-seat-4-states.png), [historical: TV1-D04-unknown.png](../D04/TV1-D04-unknown.png) |
| TV2-D04 | Tạo migration PostgreSQL | PASS_LOCAL | [review](../D04/TV2-D04-20261111-review.md) |
| TV3-D04 | Viết MessageCodec | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV4-D04 | Viết log in-memory trước | NOT_REASSESSED | [historical: TV4-D04-result.log](../D04/TV4-D04-result.log) |
| TV5-D04 | Viết election timer | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV6-D04 | Tạo 5 PostgreSQL container | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV1-D05 | Binding dữ liệu ghế | NOT_REASSESSED | [historical: TV1-D05-after-change.png](../D05/TV1-D05-after-change.png), [historical: TV1-D05-before-change.png](../D05/TV1-D05-before-change.png), [historical: TV1-D05-refresh-snapshot.png](../D05/TV1-D05-refresh-snapshot.png) |
| TV2-D05 | Seed 10 ghế và constraint | PASS_LOCAL | [review](../D05/TV2-D05-20261111-review.md) |
| TV3-D05 | Viết length-prefixed framing | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV4-D05 | Kiểm tra prevLog và conflict | NOT_REASSESSED | [historical: TV4-D05-result.log](../D05/TV4-D05-result.log) |
| TV5-D05 | Cài Candidate transition | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV6-D05 | Tạo Backend image + profile Node | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV1-D06 | Kiểm tra dữ liệu nhập | NOT_REASSESSED | [historical: TV1-D06-cancel-confirm.png](../D06/TV1-D06-cancel-confirm.png), [historical: TV1-D06-cancel-success.png](../D06/TV1-D06-cancel-success.png), [historical: TV1-D06-cancel-unknown.png](../D06/TV1-D06-cancel-unknown.png), [historical: TV1-D06-search-found.png](../D06/TV1-D06-search-found.png), [historical: TV1-D06-search-not-found.png](../D06/TV1-D06-search-not-found.png), [historical: TV1-D06-ticket-list.png](../D06/TV1-D06-ticket-list.png) |
| TV2-D06 | Viết kết nối JDBC | PASS_LOCAL | [review](../D06/TV2-D06-20261111-review.md) |
| TV3-D06 | Tạo TCP Server/Client | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV4-D06 | Nối RaftStorage thật | NOT_REASSESSED | [historical: TV4-D06-result.log](../D06/TV4-D06-result.log) |
| TV5-D06 | Xử lý điều kiện cấp phiếu | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV6-D06 | Healthcheck và chuẩn hóa logs | NOT_REASSESSED | [historical: TV6-D06-result.log](../D06/TV6-D06-result.log) |
| TV1-D07 | Tách tầng UI và Client Adapter | NOT_REASSESSED | [historical: TV1-D07-disconnected.png](../D07/TV1-D07-disconnected.png), [historical: TV1-D07-loading-2s.png](../D07/TV1-D07-loading-2s.png), [historical: TV1-D07-no-quorum.png](../D07/TV1-D07-no-quorum.png), [historical: TV1-D07-not-leader.png](../D07/TV1-D07-not-leader.png), [historical: TV1-D07-seat-already-booked.png](../D07/TV1-D07-seat-already-booked.png), [historical: TV1-D07-success.png](../D07/TV1-D07-success.png), [historical: TV1-D07-unknown-timeout.png](../D07/TV1-D07-unknown-timeout.png) |
| TV2-D07 | Viết transaction repository | PASS_LOCAL | [review](../D07/TV2-D07-20261111-review.md) |
| TV3-D07 | Tạo Message Dispatcher | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV4-D07 | Cài append với persistence contract | NOT_REASSESSED | [historical: TV4-D07-result.log](../D07/TV4-D07-result.log) |
| TV5-D07 | Xử lý split vote/timeouts | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV6-D07 | Kiểm thử DB một Node | NOT_REASSESSED | [historical: TV6-D07-result.log](../D07/TV6-D07-result.log) |
| TV1-D08 | Kiểm thử UI không khóa luồng | NOT_REASSESSED | [historical: TV1-D08-bug-list.md](../D08/TV1-D08-bug-list.md), [historical: TV1-D08-result.log](../D08/TV1-D08-result.log), [historical: TV1-D08-ui-demo-2.png](../D08/TV1-D08-ui-demo-2.png), [historical: TV1-D08-ui-demo.png](../D08/TV1-D08-ui-demo.png) |
| TV2-D08 | State Machine đặt vé trên một Node | PASS_LOCAL | [review](../D08/TV2-D08-20261111-review.md) |
| TV3-D08 | Tạo NodeMain v1 | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV4-D08 | Cài suffix truncate an toàn | NOT_REASSESSED | [historical: TV4-D08-result.log](../D08/TV4-D08-result.log) |
| TV5-D08 | Persist term/vote trước trả RPC | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV6-D08 | Tạo test runner và CI cơ bản | NOT_REASSESSED | [historical: TV6-D08-result.log](../D08/TV6-D08-result.log) |
| TV1-D09 | Nối TCP Client thật | NOT_REASSESSED | [historical: TV1-D09-result.log](../D09/TV1-D09-result.log) |
| TV2-D09 | Idempotency cục bộ | PASS_LOCAL | [review](../D09/TV2-D09-20261111-review.md) |
| TV3-D09 | Nối TCP với Booking 1 Node | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV4-D09 | Viết AppendEntries Follower | NOT_REASSESSED | [historical: TV4-D09-result.log](../D09/TV4-D09-result.log) |
| TV5-D09 | Kiểm thử election không cần TCP | NOT_REASSESSED | [historical: TV5-D09-result.log](../D09/TV5-D09-result.log) |
| TV6-D09 | Kiểm thử E2E một Node | NOT_REASSESSED | [historical: TV6-D09-result.log](../D09/TV6-D09-result.log) |
| TV1-D10 | Demo 1 Node end-to-end | NOT_REASSESSED | [historical: TV1-D10-result.log](../D10/TV1-D10-result.log), [historical: TV1-D10-ui.png](../D10/TV1-D10-ui.png) |
| TV2-D10 | Tích hợp 1 Node với TCP | BLOCKED_SYSTEM | [review](../D10/TV2-D10-20261111-review.md) |
| TV3-D10 | Khóa mốc Single Node | NOT_REASSESSED | [historical: TV3-D10-result.log](../D10/TV3-D10-result.log) |
| TV4-D10 | Viết unit tests AppendEntries | NOT_REASSESSED | [historical: TV4-D10-result.log](../D10/TV4-D10-result.log) |
| TV5-D10 | Nối Election vào NodeMain | NOT_REASSESSED | Chưa có file khớp tên; không suy diễn hoàn thành |
| TV6-D10 | Chốt MVP một Node | NOT_REASSESSED | [historical: TV6-D10-result.log](../D10/TV6-D10-result.log) |
