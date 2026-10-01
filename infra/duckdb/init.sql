-- ==============================================================================
-- Khởi tạo kiến trúc Data Warehouse 3 Layer (Medallion Architecture) trong DuckDB
-- ==============================================================================

-- 1. Layer Landing (hoặc Raw):
-- Nơi chứa dữ liệu thô được Ingestion service nạp từ HDFS / Kafka / Parquet.
-- Giữ nguyên cấu trúc gốc, lưu vết metadata (ingestion_time, source_file, v.v.)
CREATE SCHEMA IF NOT EXISTS landing;

-- 2. Layer Staging:
-- Nơi làm sạch dữ liệu, chuẩn hóa tên cột, ép kiểu (cast types), dedup và xử lý null.
-- Làm nền tảng cho việc biến đổi dữ liệu (transformation) bằng dbt.
CREATE SCHEMA IF NOT EXISTS staging;

-- 3. Layer Marts:
-- Nơi chứa các bảng Fact, Dimension và Data Marts / KPI tổng hợp.
-- Phục vụ trực tiếp cho BI, Dashboard và các câu truy vấn phân tích.
CREATE SCHEMA IF NOT EXISTS marts;

-- Kiểm tra danh sách schemas đã khởi tạo:
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name IN ('landing', 'staging', 'marts');
