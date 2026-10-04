# Bonus B2 — Thiết kế: pipeline RAG cho hồ sơ bệnh án PDF tiếng Việt

> Bản nháp brainstorm. Các quyết định dưới đây là phán đoán cá nhân; cần đọc lại và
> chỉnh theo bối cảnh thực tế trước khi coi là thiết kế cuối.

## 1. Bài toán và ràng buộc

Một chuỗi phòng khám tư (≈ 15 cơ sở) muốn một trợ lý cho bác sĩ: hỏi
*"bệnh nhân này đã từng dị ứng thuốc gì, lần khám gần nhất kê đơn gì?"* và nhận câu trả
lời có trích dẫn trang hồ sơ gốc. Dữ liệu nguồn:

- ~400.000 file PDF tích luỹ 8 năm: một phần là PDF sinh từ phần mềm HIS (có lớp text),
  phần lớn là **ảnh scan** giấy viết tay/đánh máy, nhiều bảng xét nghiệm, tiếng Việt có dấu.
- Mỗi ngày thêm ~1.500 file mới, và **hồ sơ cũ có thể bị sửa** (bổ sung kết quả, đính chính).
- Bệnh nhân có quyền yêu cầu xoá/hạn chế xử lý dữ liệu (Nghị định 13/2023/NĐ-CP).

Vì sao khó: OCR tiếng Việt trên scan kém chất lượng sai dấu (*"dị ứng"* → *"di ung"*),
bảng bị vỡ cột, và mỗi câu trả lời sai có hậu quả lâm sàng. Đây không phải bài toán
"nhét PDF vào vector DB".

## 2. Các quyết định chính

### Q1 — Batch hay streaming?
**Quyết định: micro-batch mỗi 15 phút, không streaming.**
Bác sĩ cần hồ sơ mới trong cùng buổi khám, không cần dưới một giây. Streaming (Kafka +
Flink) thêm hạ tầng mà đội 2 người không vận hành nổi, trong khi bước đắt nhất là OCR
(vài giây/trang) — độ trễ do OCR đã lớn hơn mọi lợi ích của streaming.
*Đánh đổi:* hồ sơ vừa scan phải chờ tối đa ~15 phút + thời gian OCR; chấp nhận được.

### Q2 — Bronze lưu gì? (Nguồn & hình dạng)
**Quyết định: Bronze = file PDF gốc bất biến, đặt tên theo `sha256(nội dung)`, kèm
manifest (cơ sở, bệnh nhân, thời điểm nhận).** Kết quả OCR là một tầng riêng (Silver),
có version theo `ocr_engine@version`.
*Đánh đổi X vs Y:* lưu cả output OCR vào Bronze thì tiết kiệm tính lại, nhưng khi đổi
engine OCR (chắc chắn sẽ xảy ra vì OCR tiếng Việt đang tiến bộ nhanh) ta không còn
"sự thật gốc" để chạy lại. Giống cache embedding trong lab: khoá = hash(input) +
model version, nên chạy lại chỉ OCR những trang chưa có cho engine mới.

### Q3 — Hợp đồng và chất lượng trước khi vào index
**Quyết định: chốt chất lượng theo trang, không theo file.** Mỗi trang có điểm
confidence OCR và tỉ lệ từ tiếng Việt hợp lệ (so với từ điển có dấu). Trang dưới ngưỡng
đi vào `quarantine_pages` và hàng đợi cho nhân viên nhập liệu xem lại; phần còn lại của
file vẫn vào index.
*Đánh đổi:* chốt theo file đơn giản hơn nhưng một trang mờ sẽ chặn cả hồ sơ 40 trang.
Cảnh báo khi tỉ lệ quarantine của một cơ sở vượt 2× trung bình 7 ngày — thường là máy
scan hỏng, không phải dữ liệu xấu ngẫu nhiên.

### Q4 — Vector RAG hay knowledge graph?
**Quyết định: vector RAG có lọc metadata cứng theo `patient_id`, cộng một bảng có cấu
trúc cho dị ứng/thuốc trích xuất bằng LLM.**
Câu hỏi của bác sĩ phần lớn là lookup trong phạm vi một bệnh nhân, không phải multi-hop
toàn cục. Lọc theo `patient_id` trước khi tìm kiếm vừa tăng độ chính xác, vừa là chốt
bảo mật (không bao giờ truy xuất chunk của người khác). Riêng dị ứng thuốc quá quan
trọng để phụ thuộc vào top-k retrieval, nên trích xuất thành bảng có schema
(`patient_id, substance, reaction, source_page`) — áp dụng đúng quy tắc "LLM là một
bước transform": cache theo hash, ép JSON, sai schema → quarantine.

### Q5 — Failure semantics: xoá và sửa hồ sơ
**Quyết định: mọi thay đổi đi qua khoá `document_id` + version tăng dần; xoá là
tombstone lan xuống mọi tầng (OCR, chunk, vector, bảng dị ứng).**
Giống CDC delete trong lab: nếu xoá cứng ở index mà Bronze/Silver chạy lại, hồ sơ sẽ
"hồi sinh". Tombstone giữ `document_id` + version của lần xoá, chặn mọi bản cũ hơn.
Với PDF gốc trong Bronze, dùng **crypto-shredding**: mỗi bệnh nhân một khoá mã hoá;
yêu cầu xoá = huỷ khoá, giữ được tính bất biến của Bronze mà vẫn tuân thủ quyền xoá.

### Q6 — Chi phí
80% chi phí dự kiến là OCR + LLM trích xuất trong đợt backfill 400k file. Cắt bằng:
(1) bỏ qua OCR với PDF đã có lớp text; (2) cache theo hash trang — scan trùng (rất
phổ biến: cùng một phiếu xét nghiệm được scan lại) chỉ tính một lần; (3) ước lượng chi
phí trước mỗi backfill (số trang × giá), như bước `estimate_tokens` của lab.

## 3. Phương án bị loại

**Loại: đưa toàn bộ PDF vào một dịch vụ "chat with your documents" có sẵn.**
Nhanh nhất để demo, nhưng: (a) dữ liệu y tế rời khỏi hạ tầng của phòng khám, vướng quy
định về dữ liệu nhạy cảm; (b) không kiểm soát được việc xoá theo yêu cầu bệnh nhân —
không có tombstone, không chứng minh được dữ liệu đã bị gỡ khỏi index; (c) không có
chốt chất lượng OCR theo trang, nên lỗi dấu tiếng Việt lọt thẳng vào câu trả lời.

**Cũng cân nhắc và loại: knowledge graph toàn bộ hồ sơ.** Chi phí trích xuất thực thể
và quan hệ cho 400k file rất lớn, trong khi câu hỏi thực tế hiếm khi cần multi-hop qua
nhiều bệnh nhân.

## 4. Sơ đồ kiến trúc

```
 HIS export ─┐                                  ┌─▶ quarantine_pages ─▶ hàng đợi nhập liệu
 Máy scan ───┼─▶ BRONZE ──────▶ SILVER ─────────┤
 Upload tay ─┘   PDF gốc         OCR theo trang  └─▶ pages_ok
                 sha256 tên       key = hash(trang)        │
                 mã hoá theo      + ocr_engine@ver         ├─▶ GOLD chunks + embedding
                 bệnh nhân        tombstone theo           │     (lọc patient_id) ──▶ RAG
                 (crypto-shred)   document_id+version      │
                                                           └─▶ GOLD allergies/meds
                                                                 (LLM, JSON, cache,
                                                                  quarantine) ──▶ cảnh báo
 Điều phối: micro-batch 15'; backfill = cùng code path theo ngày; checksum mỗi lần chạy lại
```

## 5. Câu hỏi còn mở

- Đo chất lượng OCR thế nào khi không có ground truth? Ý tưởng: gán nhãn tay 500 trang
  ngẫu nhiên phân tầng theo cơ sở/loại giấy, đo CER (character error rate) có dấu.
- Ngưỡng confidence cho quarantine chọn bao nhiêu để hàng đợi nhập liệu không quá tải?
  Cần đo trên dữ liệu thật, giống cách lab đo P99 lateness thay vì đoán.
