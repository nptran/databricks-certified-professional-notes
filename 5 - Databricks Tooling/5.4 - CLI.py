# Databricks notebook source
# MAGIC %md
# MAGIC Chạy lệnh dưới để kiểm tra đường dẫn đã tồn tại chưa. Nếu chưa, hãy tạo một cái bằng CLI với câu lệnh sau:
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC databricks fs mkdir dbfs:/mnt/uploads
# MAGIC ```

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/uploads'

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

db_password = dbutils.secrets.get("bookstore-dev", "db_password")

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks __redacts các secret value__ để tránh việc bạn vô tình in ra chúng.

# COMMAND ----------

print(db_password)

# COMMAND ----------

