# Databricks notebook source
# MAGIC %md
# MAGIC #### Các Dynamic View cho phép áp dụng tính năng ACL cho dữ liệu tại cấp độ Column hoặc Row. Nhờ đó, có thể thực hiện phân quyền dữ liệu theo user.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Cùng xem lại các column có trong bảng `customers_silver`.

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE TABLE customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Một vài cột chứa thông tin PII về email, name và address. Chúng ta sẽ tiến hành redact các cột này trong một view sau. Với hàm `is_member()`, chỉ các member trong group `admins_demo` mới có thể xem được dữ liệu này.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW customers_vw AS
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN email
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS email,
# MAGIC     gender,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN first_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS first_name,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN last_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS last_name,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admins_demo') THEN street
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS street,
# MAGIC     city,
# MAGIC     country,
# MAGIC     row_time
# MAGIC   FROM customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Kiểm tra kết quả trong view. Hiện tại, user không nằm trong admins_demo group nên sẽ không thấy được dữ liệu.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_vw

# COMMAND ----------

# MAGIC %md
# MAGIC Tạo một User group `admins_demo` trong Databricks và thêm user hiện tại vào.
# MAGIC Sau khi thêm user hiện tại vào `admins_demo` group, ta có thể thấy được dữ liệu hiển thị trong view.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_vw

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tiếp tục áp dụng bảo mật trên các Row.
# MAGIC
# MAGIC
# MAGIC Tạo một view khác trên view `customer_vw`.
# MAGIC
# MAGIC Khi này, chỉ các user trong __admins_demo__ group mới có thể thấy toàn bộ dữ liệu, nếu không, sẽ chỉ hiển thị các bản ghi từ `France` và thời gian từ `2022-01-01`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW customers_fr_vw AS
# MAGIC SELECT * FROM customers_vw
# MAGIC WHERE 
# MAGIC   CASE 
# MAGIC     WHEN is_member('admins_demo') THEN TRUE
# MAGIC     ELSE country = "France" AND row_time > "2022-01-01"
# MAGIC   END

# COMMAND ----------

# MAGIC %md
# MAGIC Xoá user khỏi __admins_demo__ group, rồi thực hiện truy vấn vào View. Ta sẽ thấy các cột đã REDACTED, và `country` chỉ là `France` với `row_time` sau ngày 2022-01-01.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_fr_vw

# COMMAND ----------

