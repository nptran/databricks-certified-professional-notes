# Databricks notebook source
# MAGIC %md
# MAGIC >### Tài liệu này mô tả cách xử lý Delete Request tăng dần và truyền các Delete này tới Lakehouse
# MAGIC
# MAGIC _Delete Request A.K.A Request to be forgotton_
# MAGIC
# MAGIC Delete Request là các yêu cầu về xoá dữ liệu người dùng (__PII__)
# MAGIC
# MAGIC Trong demo này, có 2 bảng chứa PII data là __customers__ và __customer_orders__.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/deletes.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Đoạn streaming query dưới đây sẽ ghi dữ liệu vào một bảng tên __delete_requests__, bảng này dùng để theo dõi các delete request tới từ bronze table dưới dạng CDC feed (có kiểu `row_status` là `delete` nằm trong *customers* topic).
# MAGIC
# MAGIC Trong khi có thể thực hiện delete và insert/update CDC feed cùng lúc, các tuỳ chỉnh đi kèm các forgotten request có thể yêu cầu một luồng xử lý riêng biệt.

# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

(spark.readStream
        .table("bronze")
        .filter("topic = 'customers'")
        .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
        .select("v.*", F.col('v.row_time').alias("request_timestamp")) # the time at which the delete was request
        .filter("row_status = 'delete'")
        .select("customer_id", "request_timestamp",
                F.date_add("request_timestamp", 30).alias("deadline"), # Add 30 days to ensure compliance
                F.lit("requested").alias("status")) # add new column `status` with literal value "requested" as current status of the request process
    .writeStream
        .outputMode("append")
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/delete_requests")
        .trigger(availableNow=True)
        .table("delete_requests") # Tạo một bảng tên delete_requests để theo dõi các delete request
)

# COMMAND ----------

# MAGIC %md
# MAGIC Giờ hãy kiểm tra xem trong bảng `delete_requests` có gì.
# MAGIC
# MAGIC Ta thấy các bản ghi chứa thông tin về các yêu cầu delete với status `requested` đã được thêm vào.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC Giờ bắt đầu thực hiện việc xoá ở __customer_silver__ dựa trên thông tin `customer_id` trong bảng `delete_requests`

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM customers_silver
# MAGIC WHERE customer_id IN (SELECT customer_id FROM delete_requests WHERE status = 'requested')

# COMMAND ----------

# MAGIC %md
# MAGIC > *Trong các tài liệu trước, chúng ta đã bật CDF cho bảng customers. Do đó, ta sẽ tận dụng CDF như một incremental records của các data change để truyền các delete xuống các bảng hạ nguồn.*
# MAGIC
# MAGIC Truy vấn sau cấu hình một incremental read cho tất cả các sự kiện change được commit của bảng `customers_silver`.
# MAGIC
# MAGIC

# COMMAND ----------

deleteDF = (spark.readStream
                 .format("delta")
                 .option("readChangeFeed", "true")
                 .option("startingVersion", 2)
                 .table("customers_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC Tiếp tục tạo một hàm có chức năng xử lý các sự kiện delete bằng __foreachBatch__. Hàm này sẽ xử lý việc truyền hành động delete (hay nói cách khác là xoá các dữ liệu customer còn lại ở các bảng hạ nguồn), sau đó cập nhật trạng thái request trong bảng `delete_requests`.

# COMMAND ----------

def process_deletes(microBatchDF, batchId):
    
    (microBatchDF
        .filter("_change_type = 'delete'")
        .createOrReplaceTempView("deletes"))

    # Commit delete changes to `customer_orders`, which also contains deleted customer data
    microBatchDF._jdf.sparkSession().sql("""
        DELETE FROM customers_orders
        WHERE customer_id IN (SELECT customer_id FROM deletes)
    """)
    
    # After that, update the status of delete requests for deleted customer data.
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO delete_requests r
        USING deletes d
        ON d.customer_id = r.customer_id
        WHEN MATCHED
          THEN UPDATE SET status = "deleted"
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC Cuối cùng, chạy một trigger availableNow batch để truyền hành động delete tới __customers_orders__ sử dụng hàm ở trên.

# COMMAND ----------

(deleteDF.writeStream
         .foreachBatch(process_deletes)
         .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/deletes")
         .trigger(availableNow=True)
         .start())

# COMMAND ----------

# MAGIC %md
# MAGIC Kiểm tra trạng thái trong bảng delete_requests để thấy các deleted request.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC Kiểm tra điều gì đã xảy ra trong bảng customers_orders. Ta thấy một bản ghi với operation `DELETE` đã xuất hiện.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY customers_orders

# COMMAND ----------

# MAGIC %md
# MAGIC Nhưng liệu việc delete này đã thực sự được commit? Câu trả lời là __Chưa__, hoặc __Không hẳn__.
# MAGIC Các dữ liệu bị xoá vẫn còn tồn tại trong các phiên bản cũ hơn của bảng, điều này để phục vụ tính năng Time Travel và CDF của Delta Lake. Nói cách khác, __việc xoá dữ liệu trong bảng không xoá các data files của bảng, thay vào đó, nó tạo các data file mới mà không có các bản ghi bị xoá rồi tham chiếu bảng tới data file mới này__.

# COMMAND ----------

# MAGIC %md
# MAGIC Xác nhận lại bằng cách truy xuất vào version ngay trước khi delete xảy ra. Ta có thể thấy 192 bản ghi đã bị xoá.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_orders@v4
# MAGIC EXCEPT
# MAGIC SELECT * FROM customers_orders

# COMMAND ----------

# MAGIC %md
# MAGIC Mặc dù, việc DELETE đã được commit vào bảng `customers_orders`, nhưng dữ liệu vẫn còn lại trong CDF feed.

# COMMAND ----------

df = (spark.read
           .option("readChangeFeed", "true")
           .option("startingVersion", 2)
           .table("customers_silver")
           .filter("_change_type = 'delete'"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Do vậy, nếu muốn xoá hoàn toàn dữ liệu - fully commit, chúng ta cần thực hiện lệnh `VACUUM` để xoá cả các data files.