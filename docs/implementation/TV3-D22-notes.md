# TV3-D22: Reconnect Theo Peer và Leader (PeerConnectionManager) & Truy Vấn GET_REQUEST_RESULT

## 1. Thông tin Task
- **Mã task:** `tv3-d22`
- **Người thực hiện:** TV3 (Networking & Protocol)
- **Tài liệu tham chiếu:** `docs/specs/00-MASTER.md`, `01-REQUIREMENTS.md` (§AC04, §AC10), `02-ARCHITECTURE.md` (§5 Client routing, §7 Failure recovery), `03-CONTRACTS.md` (ERROR_RESPONSE, GET_REQUEST_RESULT, BOOK_TICKET), `04-RAFT.md`, `07-TESTING.md`
- **Ngày hoàn thành:** 22/11/2026

## 2. Mục tiêu kỹ thuật & Tiêu chí Nghiệm thu
1. **Làm Mới Peer Routing Sau Bầu Cử (Post-Election Peer Routing Refresh):**
   - Sau khi hoàn thành bầu cử (election notification hoặc `NOT_LEADER` redirect có `leaderHint`), client/peer router phải cập nhật ngay cached leader endpoint, đóng/làm mới kết nối cũ (stale connection) và chuyển hướng lưu lượng sang Leader mới.
   - Quản lý vòng đời kết nối qua `PeerConnectionManager` độc lập, hỗ trợ giao diện `LeaderRoutingProvider` độc lập không bị nghẽn phụ thuộc vào tiến độ của TV5 (dynamic Raft election).
2. **Bảo Tồn Bất Biến `requestId` (RequestId Invariance & State Invariant):**
   - Tuân thủ nghiêm ngặt **AGENTS.md §4 & §5**: `requestId` sinh ra từ client đại diện cho transaction nghiệp vụ duy nhất.
   - Dù gặp `NOT_LEADER`, timeout, rớt kết nối socket do Leader crash, `requestId` **tuyệt đối không được thay đổi**.
   - Mỗi lần retry hoặc query, chỉ được cấp `messageId` mới (và `correlationId` tương ứng) cho envelope vận chuyển.
3. **Truy Vấn `GET_REQUEST_RESULT` Thay Vì Tự `BOOK` Mới Khi Trạng Thái Chưa Rõ:**
   - Khi Leader cũ crash giữa chừng (mid-flight crash) sau khi nhận request, client gặp lỗi kết nối hoặc socket timeout (`UNKNOWN`).
   - Lúc này, bản tin có thể đã được nhân bản sang đa số (quorum) và được commit/apply trên state machine trước khi Leader sập.
   - Nếu client vội vàng gửi `BOOK_TICKET` mới:
     - Hoặc sẽ bị từ chối với `SEAT_ALREADY_BOOKED` (vì ghế đã được đặt thành công cho chính request trước đó).
     - Hoặc nếu đổi requestId mới sẽ làm sai lệch dữ liệu / bán trùng / mất vé của khách hàng.
   - Do đó, quy tắc phân tán bắt buộc: Client phải gửi `GET_REQUEST_RESULT(requestId)` sang Leader mới:
     - Nếu trả về `APPLIED_SUCCESS`: Nhận vé `CONFIRMED` ngay lập tức với đầy đủ `ticketId`, `seatId`, `tripId` từ replicated state machine.
     - Nếu trả về `APPLIED_REJECTED`: Ghi nhận `REJECTED` với đúng `reasonCode` gốc.
     - Nếu trả về `UNKNOWN` (chưa từng được ghi nhận trong log của Leader mới): Lúc này mới an toàn gửi lại `BOOK_TICKET` với **chính xác `requestId` ban đầu**.

## 3. Các thay đổi đã thực hiện

### 3.1. `backend-node` Module
- [`backend-node/src/main/java/vn/nhom6/railway/networking/PeerConnectionManager.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/PeerConnectionManager.java):
  - Khởi tạo bộ quản lý kết nối và định tuyến `PeerConnectionManager` hỗ trợ cả tầng Client lẫn Peer-to-Peer.
  - Cung cấp functional interface `LeaderRoutingProvider` cho phép cắm Leader discovery thực (TV5 Raft) hoặc giả lập (Test/Stubs).
  - Quản lý danh bạ endpoint các peer (`registerEndpoint`, `registerEndpoints`, `removeEndpoint`).
  - Xử lý sự kiện bầu cử `onElectionCompleted(String newLeaderId, int term)`: tự động dọn dẹp các socket cũ đến leader đã thoái vị, cập nhật leader cache và reset routing state.
  - Phương thức `sendToLeader(command)`:
    - Gửi request đến Leader hiện tại.
    - Tự động nhận diện phản hồi `ERROR_RESPONSE` có `errorCode == "NOT_LEADER"`.
    - Trích xuất `leaderHint`, cập nhật lại định tuyến và chuyển tiếp request trong suốt sang Leader mới mà không thay đổi `requestId`.
  - Phương thức `queryRequestResult(requestId, leaderId)`:
    - Đóng gói bản tin `GET_REQUEST_RESULT` đúng chuẩn SPEC-03 với `requestId`.
    - Phân tích kết quả từ Leader: `APPLIED_SUCCESS`, `APPLIED_REJECTED`, hoặc `UNKNOWN`.
  - Phương thức `executeBookingWithLeaderFailover(bookRequest)`:
    - Triển khai workflow chịu lỗi hoàn chỉnh:
    - Thử gửi `BOOK_TICKET` đến Leader hiện tại.
    - Nếu gặp sự cố mạng/Leader crash (SocketException, SocketTimeoutException):
      1. Kích hoạt chuyển đổi Leader (Failover / Refresh routing sang Leader kế tiếp theo topology hoặc routing provider).
      2. Ngay lập tức gửi `GET_REQUEST_RESULT(requestId)` sang Leader mới để phục hồi trạng thái.
      3. Nếu Leader mới xác nhận đã apply thành công (`APPLIED_SUCCESS`): trả về kết quả `CONFIRMED` ngay mà không cần gửi lại `BOOK_TICKET`.
      4. Nếu Leader mới chưa nhận được command (`UNKNOWN`): phát lại `BOOK_TICKET` với **giữ nguyên 100% `requestId` ban đầu**.
- [`backend-node/src/main/java/vn/nhom6/railway/networking/FakeBookingService.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/main/java/vn/nhom6/railway/networking/FakeBookingService.java):
  - Chuẩn hóa việc cấp phát `logIndex` đồng bộ với `stateMachine.lastApplied() + 1` để đảm bảo khi nhiều server/node cùng áp dụng vào chung một replicated state machine, thứ tự log index luôn đơn điệu tăng dần.
- [`backend-node/src/test/java/vn/nhom6/railway/networking/PeerConnectionManagerTest.java`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/backend-node/src/test/java/vn/nhom6/railway/networking/PeerConnectionManagerTest.java):
  - Xây dựng 6 ca kiểm thử chuyên sâu về kịch bản crash, failover và bảo toàn requestId:
    1. `testPostElectionRoutingRefreshClosesStaleAndSwitchesLeader`: Sau bầu cử, `onElectionCompleted` đóng kết nối cũ, trỏ đúng sang Leader mới.
    2. `testLeaderCrashMidFlightResolvedByGetRequestResultSuccess`: Leader cũ crash ngay sau khi ghi nhận booking; Client phát hiện crash, kết nối sang Leader mới, gọi `GET_REQUEST_RESULT` và nhận ngay `CONFIRMED` mà không cần BOOK lại.
    3. `testLeaderCrashBeforeCommitRetriesWithExactSameRequestId`: Leader cũ crash trước khi kịp commit; Client truy vấn Leader mới nhận `UNKNOWN`, sau đó phát lại `BOOK_TICKET` với chính xác `requestId` ban đầu và nhận `CONFIRMED`.
    4. `testFollowerNotLeaderRedirectRefreshesRoutingAndPreservesRequestId`: Gửi request nhầm vào Follower; nhận `NOT_LEADER` kèm `leaderHint`, client tự động chuyển hướng sang Leader và bảo toàn nguyên vẹn `requestId`.
    5. `testRepeatedLeaderCrashesFailoverSafety`: Thử thách nhiều Leader liên tiếp bị crash; Client lần lượt failover qua từng Node mà không gây rò rỉ kết nối hay sai lệch requestId.
    6. `testDirectQueryRequestResultOutcomes`: Kiểm tra trực tiếp các nhánh kết quả của `queryRequestResult` (`APPLIED_SUCCESS`, `APPLIED_REJECTED`, `UNKNOWN`).

## 4. Kết quả kiểm thử thực tế

- **Kiểm thử có chủ đích (`PeerConnectionManagerTest`):**
  ```bash
  mvn -B -ntp test -Dtest=PeerConnectionManagerTest
  ```
  - Kết quả: `Tests run: 6, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.703 s` (PASS 100%).
  - Bằng chứng: [`artifacts/evidence/D22/TV3-D22-targeted.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D22/TV3-D22-targeted.log).

- **Kiểm thử tổ hợp mạng & kết nối (`PeerConnectionManagerTest`, `TestNetworkFilterTest`, `TcpLoadTest`, `LeaderRedirectionTcpIntegrationTest`):**
  ```bash
  mvn -B -ntp test -Dtest=PeerConnectionManagerTest,TestNetworkFilterTest,TcpLoadTest,LeaderRedirectionTcpIntegrationTest
  ```
  - Kết quả: `Tests run: 27, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 8.700 s` (PASS 100%).

- **Kiểm thử toàn bộ Reactor 6 module:**
  ```bash
  mvn -B -ntp test | tee artifacts/evidence/D22/TV3-D22-result.log
  ```
  - Kết quả: **`BUILD SUCCESS`** trên toàn bộ 6/6 module:
    - `railway-distributed-system`: SUCCESS
    - `shared`: SUCCESS
    - `backend-node`: SUCCESS
    - `desktop-client`: SUCCESS
    - `desktop-monitor`: SUCCESS
    - `integration-tests`: SUCCESS
  - Bằng chứng: [`artifacts/evidence/D22/TV3-D22-result.log`](file:///home/chennan/Documents/code-project/do-an-phan-tan-java/artifacts/evidence/D22/TV3-D22-result.log).

## 5. Rủi ro & Bước tiếp theo
- **Rủi ro:** Khi tích hợp đầy đủ với TV5-D22 (bầu cử Raft động thực tế trên mạng LAN), `LeaderRoutingProvider` cần được gắn với `RaftNode.getLeaderId()` và lắng nghe `RoleChangeEvent` để kích hoạt `onElectionCompleted` tự động.
- **Bước tiếp theo (D23):** Phối hợp với TV4 và TV5 để tích hợp end-to-end Client Routing và Raft State Machine trên cụm 5 Node thực tế.
