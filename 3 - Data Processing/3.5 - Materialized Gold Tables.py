# Databricks notebook source
# MAGIC %md
# MAGIC > ### Tài liệu này mô tả cách để tạo các __Stored View__ và __Materialized View__ trong Databricks.
# MAGIC
# MAGIC Trong Demo này tạo ra hai đối tượng trong Gold layer là `countries_stats_vw` và `authors_stats` từ các bảng nguồn trong silver layer.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/gold.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC #### View
# MAGIC View `countries_stats_vw` thể hiện số lượng đơn hàng và sách được bán ra trên mỗi quốc gia theo ngày. Hàm `COUNT()` để đếm số lượng order, và `SUM()` để đếm số lượng book đã được bán, nhóm lại bảng `GROUP BY` trên mỗi `country` và `ordered Date` (hàm `date_trunc()` với tham số "DD" sẽ trích xuất giá trị `ngày` từ timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VIEW IF NOT EXISTS countries_stats_vw AS (
# MAGIC   SELECT country, date_trunc("DD", order_timestamp) order_date, count(order_id) orders_count, sum(quantity) books_count
# MAGIC   FROM customers_orders
# MAGIC   GROUP BY country, date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Kiểm tra dữ liệu trong view. Kết quả truy vấn sẽ được Databricks cache, dù truy vấn phức tạp đến đâu, thì những lần thực thi sau sẽ luôn nhanh hơn lần thực thi đầu tiên (trong cùng một Cluster session).
# MAGIC > Mỗi lần chạy truy vấn từ View, Delta sẽ lại thực thi truy vấn trên 1 lần. Do đó, với các các truy vấn phức tạp thì sẽ rất tốn kém.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries_stats_vw
# MAGIC WHERE country = "France"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Materialized View - Gold Table
# MAGIC Trong các CSDL truyền thống, việc kiểm soát chi phí có thể đạt được bằng cách sử dụng materialied view. Trong Databricks, khái niệm MatView này tương đương với một gold table. __Gold table giúp cắt giảm chi phí và độ trễ__ cho các _ad-hoc query_ phức tạp.
# MAGIC
# MAGIC Bên dưới là truy vấn để ghi dữ liệu vào gold table `authors_stats`.
# MAGIC
# MAGIC Bảng này thống kê số lượng sách bán ra trên mỗi tác giả. Hàm `agg()` tổng hợp dữ liệu bằng cách đếm lượng đơn hàng và trung bình số lượng sách trên mỗi tác giả trong mỗi khoảng thời gian order_timestamp là 5 phút - `groupBy()`.

# COMMAND ----------

from pyspark.sql import functions as F

query = (spark.readStream
                 .table("books_sales")
                 # Xử lý các bản ghi tới trễ khi stream bằng một watermark
                 .withWatermark("order_timestamp", "10 minutes")
                 .groupBy(
                     F.window("order_timestamp", "5 minutes").alias("time"),
                     "author")
                 # tổng hợp dữ liệu bằng cách đếm lượng đơn hàng và trung bình số lượng
                 .agg(
                     F.count("order_id").alias("orders_count"),
                     F.avg("quantity").alias ("avg_quantity"))
              .writeStream
                 .option("checkpointLocation", f"dbfs:/mnt/demo_pro/checkpoints/authors_stats")
                 .trigger(availableNow=True)
                 .table("authors_stats")
            )

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM authors_stats

# COMMAND ----------

