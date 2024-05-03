
# Bài tập: Phân tích dữ liệu bán hàng

## Mô tả bài toán:
Bạn đang là một nhà phân tích dữ liệu cho một cửa hàng bán lẻ. Bạn được cung cấp một tập dữ liệu về các giao dịch bán hàng từ cửa hàng này. Nhiệm vụ của bạn là thực hiện các phân tích cơ bản để hiểu về hoạt động kinh doanh của cửa hàng.

Dữ liệu:
Tập dữ liệu chứa thông tin về các giao dịch bán hàng, bao gồm các trường sau:


- `transaction_id`: ID của giao dịch.
- `customer_id`: ID của khách hàng.
- `product_id`: ID của sản phẩm.
- `quantity`: Số lượng sản phẩm được mua trong mỗi giao dịch.
- `unit_price`: Giá của mỗi sản phẩm.
- `timestamp`: Thời gian giao dịch.

### Câu hỏi cần trả lời:
- Tổng doanh số bán hàng trong mỗi ngày/tháng/năm?
- Số lượng giao dịch được thực hiện trong mỗi ngày/tháng/năm?
- Top 10 sản phẩm bán chạy nhất?
- Top 10 khách hàng có chi tiêu cao nhất?
- Sản phẩm nào có doanh số cao nhất trong mỗi tháng?
### Yêu cầu:
- Tạo bảng trong Hive để lưu trữ dữ liệu từ tập dữ liệu được cung cấp.
- Thực hiện các truy vấn Hive để trả lời các câu hỏi được đề xuất.
