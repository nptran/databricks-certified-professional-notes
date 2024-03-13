# Databricks notebook source
# MAGIC %md
# MAGIC ### Tài liệu này nói về Delta Lake Trasaction Log

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Trước hết, hãy xem qua các file transaction log hiện có trong bảng bronze.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Thử đọc một file transaction log JSON xem sao.
# MAGIC
# MAGIC Trong cột add, ta thấy danh sách các file được thêm vào ở commit này. Mỗi file này đi kèm thông tin về thống kê được lưu trong trường `stats`. Bao gồm thống kê về tổng số lượng records - `numRecords`, `minValues`, `maxValues` và `nullCount` trong __32 cột đầu tiên__ của bảng `bronze`.
# MAGIC
# MAGIC Thống kê của Delta Lake thực hiện trên từng data file, thông tin vị trí data file thể hiện qua trường `path`.
# MAGIC
# MAGIC Query Optimizer sử dụng các thống kê này để tạo kết quả cho các truy vấn filter, nó có thể nhanh chóng bỏ qua việc quét các tệp không thoả mãn với điều kiện lọc.
# MAGIC Ví dụ, nếu filter thực hiện trên một offset có giá trị lớn hơn giá trị maximum offset, thì Delta Lake sẽ ngay lập tức bỏ qua file đó.

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log/00000000000000000001.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC Ví dụ về việc Delta Lake tận dụng statistics để tối ưu truy vấn là khi đếm tổng số bản ghi có trong bảng. Dù bảng có rất lớn, nhưng truy vấn này vẫn có thể trả về kết quả nhanh chóng thông qua giá trị đã được thống kê `numRecords` trong statistics.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------

# MAGIC %md
# MAGIC > Tại thời điểm Demo này được tạo, Databricks sẽ tạo 1 checkpoint file sau mỗi 10 JSON commit. Tuy nhiên, hiện tại, Databricks tạo checkpoint này sau mỗi 100 commit. Do đó, kết quả bên dưới sẽ không hiển thị checkpoint file.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Checkpoint file không được Databricks tạo, do đó, câu lệnh bên dưới sẽ bị lỗi.

# COMMAND ----------

display(spark.read.parquet("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log/00000000000000000010.checkpoint.parquet"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update in the latest version
# MAGIC Mặc định, mỗi __100__ commit, Databricks sẽ tạo một __checkpoint file__ (__10__ ở các version trước đó). Trong demo này, ta sẽ sửa số commit này về 10 bằng cách cập nhật thuộc tính `delta.checkpointInterval` của bảng.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze SET TBLPROPERTIES("delta.checkpointInterval" = "10")

# COMMAND ----------

# MAGIC %md
# MAGIC Kiểm tra xem property `checkpointInterval` đã được cập nhật hay chưa:

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Vậy là từ giờ, sau mỗi 10 commit, Databricks sẽ tạo một checkpoint file bên cạnh một file JSON log trên bảng `bronze`.