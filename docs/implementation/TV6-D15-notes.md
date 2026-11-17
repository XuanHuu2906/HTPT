# TV6-D15 — Chuẩn bị và thử harness nghiệm thu, tách GATE

Owner: Trần Hoàng Đạt / TV6. Phạm vi chỉ gồm `ClusterSmokeTest.java`,
`artifacts/evidence/D15/TV6-D15-result.log` và tài liệu này.

## Baseline và nguồn chuẩn

- Baseline chạy thử: `f3c90f9fadb839da5d40b9b023c8995d9d0b0b9d`.
  Commit này chứa Monitor D13/D14 và collector D12; không tự coi là bản G4
  đã tích hợp các PR D15. Runtime/shared/docker không đổi so với `1f6668f`.
- Giữ nguyên POM, backend, DTO, Compose và script đã có. Dùng
  `docker/compose.election-test.yml` có sẵn để không chạm cụm dev.
- Đối chiếu SPEC-02 §3/7, SPEC-03 §2/3/5, SPEC-07 và SPEC-08 §5/7;
  phục vụ quan sát FR06/FR07/FR12. Không nhận PASS AC02–AC10 từ kiểm tra Leader.

## Kết quả thực chạy ngày 17/11/2026

| Phần kiểm tra | Kết quả |
|---|---|
| HARNESS mặc định | PASS: 5 test chạy, 0 lỗi; 2 test runtime skip theo cờ opt-in. Maven exit 0. |
| SMOKE-D14 + harness | PASS: 6 test chạy, 0 lỗi; 1 test G4-LEADER skip. Maven exit 0. |
| Leader thật 5 node baseline | PASS riêng: node-2, term 2, 3 mẫu ổn định; currentTerm compatibility. Không phải PASS G4. |
| Thu log / stop thật | PASS: 5 log backend không rỗng, thu trước stop; không còn container chạy trong project test. |
| G4-LEADER strict sau PR D15 | NOT_RUN; không bật cờ. |
| G4 chung | NOT_RUN; không gửi lệnh booking, không test commit/apply hoặc SQL nghiệp vụ. |

Project thực chạy: `railway-tv6-d15-262cdc44`. Giữ container/volume project này
ở trạng thái stopped sau test; cụm dev và các dịch vụ khác không bị stop.
Build image, command/exit, status JSON và 5 backend logs có trong file evidence
được liên kết cuối tài liệu. Dockerfile hiện có phát cảnh báo `DB_PASSWORD` qua
ENV; giữ nguyên output cảnh báo và không sửa Dockerfile ngoài phạm vi nhiệm vụ.

## Ba GATE độc lập

| GATE | Cách bật | Assertion / giới hạn |
|---|---|---|
| HARNESS | Mặc định | Kiểm tra thứ tự start → verifyLeader → collect → stop, cleanup khi lỗi, Leader động và ngăn legacy vượt GATE strict. Dùng fixture, không phải test cụm thật. |
| SMOKE-D14 | `-Dcluster.smoke=true` | Khởi động 5 backend + 5 PostgreSQL thật, chờ 3 mẫu liên tiếp có đủ 5 node khỏe, đúng 1 Leader động, cùng term; thu 5 log không rỗng; stop project test và kiểm tra không còn container chạy. |
| G4-LEADER | `-Dcluster.g4.leader=true` | Chỉ bật sau tích hợp PR D15/freeze candidate. Cùng kiểm tra runtime, nhưng yêu cầu field `term` và toàn bộ field set/peer keys/index/observedAt/leaderId theo SPEC-03. Không chấp nhận fallback `currentTerm`. Chỉ là thành phần Leader của G4. |

**G4 chung: NOT_RUN trong lượt TV6-D15 này.** Commit/apply, read barrier,
concurrent booking, idempotency và SQL assertions phải được cả nhóm thực hiện
trên candidate sau các PR D15. Leader xuất hiện không chứng minh quorum ghi,
commit hay dữ liệu nghiệp vụ đúng. Không dùng kết quả SMOKE-D14 làm PASS G4.

Baseline hiện trả `currentTerm`, thiếu `term/leaderId/lastLogIndex`, peer keys
chưa đúng ở mọi node. SMOKE-D14 đọc `currentTerm` và gắn nhãn compatibility rõ
trong evidence; không sửa hay định nghĩa lại contract. Test harness xác nhận
payload legacy không vượt được GATE strict.

## Lệnh chạy từ gốc repo

Cần JDK 25 cho Maven reactor hiện tại, Maven và Docker Desktop. Image backend
dùng JDK 21 theo Dockerfile đã có; không thay build config trong nhiệm vụ này.

Chỉ thử harness, không cần Docker:

```powershell
mvn -B -ntp -pl integration-tests -am '-Dtest=ClusterSmokeTest' '-Dsurefire.failIfNoSpecifiedTests=false' test
```

Chuẩn bị image từ đúng source baseline/candidate:

```powershell
docker build -t railway-backend-node:d13-election -f docker/Dockerfile.backend .
```

Chạy thử lifecycle và Leader thật trên baseline:

```powershell
mvn -B -ntp -pl integration-tests -am '-Dtest=ClusterSmokeTest' '-Dsurefire.failIfNoSpecifiedTests=false' '-Dcluster.smoke=true' test
```

Sau các PR D15 và freeze candidate, rebuild image từ candidate đó, rồi chạy
G4-LEADER riêng (không bật `cluster.smoke` trong cùng lệnh):

```powershell
mvn -B -ntp -pl integration-tests -am '-Dtest=ClusterSmokeTest' '-Dsurefire.failIfNoSpecifiedTests=false' '-Dcluster.g4.leader=true' test
```

`failIfNoSpecifiedTests=false` chỉ cho phép module shared/backend không có class
ClusterSmokeTest; các assertion thất bại trong integration-tests vẫn làm Maven
exit khác 0. Theo dõi số executed/skipped: mặc định 5 chạy, 2 skip; bật một
GATE runtime thì 6 chạy, 1 skip. Không suy diễn test skipped thành PASS.

## Lifecycle và bằng chứng

Mỗi lần chạy runtime tạo Compose project `railway-tv6-d15-<random>`, dùng cổng
client `19101..19105`, peer `19201..19205`, DB/storage riêng. Password test sinh
qua environment, không in resolved Compose config hay credentials.

Harness kiểm tra `config --quiet`, chạy `up --no-build --wait`, xác minh status
bằng `MessageEnvelope`/`FrameCodec`/`MessageCodec` shared và kiểm tra correlation,
sender, type, nodeId. Deadline chờ ổn định dùng `System.nanoTime()`; không hardcode
node-1 là Leader. Quan sát nối tiếp không phải snapshot nguyên tử toàn cụm.

Trong cả đường thành công và lỗi, harness cố thu log riêng từng node **trước**
`stop --timeout 5`. Không dùng `down -v`, không xóa volume, không điều khiển cụm
dev hay dịch vụ dự án khác. Project test sau chạy được giữ ở trạng thái stopped
để điều tra; Docker có thể còn container/anonymous volume của project đó.

Raw command/exit code, Docker logs, status samples và kết quả stop được ghi mặc
định trong `integration-tests/target/railway-tv6-d15-<random>.log` (generated,
ignored). Đổi bằng `-Dcluster.evidence=<absolute-log-path>`. File được append
để giữ các lần chạy, mỗi lần có marker BEGIN và project ID riêng.

Minh chứng bàn giao được gom vào đúng file được giao:
[TV6-D15-result.log](../../artifacts/evidence/D15/TV6-D15-result.log), gồm môi
trường, revision/hash, lệnh/exit và output thực tế. Đây là lần chạy bổ sung ngày
17/11/2026 (Asia/Saigon), không phải chứng minh đã chạy vào ngày lịch D15.
