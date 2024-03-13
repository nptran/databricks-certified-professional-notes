# Databricks notebook source
# MAGIC %md
# MAGIC ### Tài liệu này mô tả việc thực thi partition trong delta table

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Việc partition sẽ thực hiện trên bảng `bronze`. Đây là một managed table được lưu trữ trong đường dẫn Hive mặc định: `dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Liệt kê các file và thư mục trong đường dẫn của bảng này, ta thấy các đường dẫn partition trên cột `topic`. Bản chất chúng cũng là các đường dẫn DBFS thông thường, do đó, ta có thể __áp dụng kiểm soát truy cập cho từng partition__ này nếu cần.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/topic=customers")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Quan sát vào một đường dẫn partition cụ thế, nó đơn giản là chứa các file dữ liệu dạng parquet như thông thường.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/topic=customers/year_month=2021-12/")
display(files)

# COMMAND ----------

