# Databricks notebook source
# MAGIC %run ../../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Đầu tiên, để tránh có quá nhiều các file quá nhỏ, bật __Auto Optimize__ (gồm __*Optimize Write*__ và __*Auto Compact*__) cho tất cả các bảng trong pipeline.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC Sử dụng widget để chỉ định số file sẽ mượn. Widget value này cũng có thể được truyền dưới dạng __Parameter__ từ cấu hình của __Workflow task__ tới notebook này.

# COMMAND ----------

dbutils.widgets.text("number_of_files", "1")
num_files = int(dbutils.widgets.get("number_of_files"))

# COMMAND ----------

bookstore.load_new_data(num_files)