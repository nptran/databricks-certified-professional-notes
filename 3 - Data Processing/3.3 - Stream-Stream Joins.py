# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/customers_orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC > ## Notebook này mô tả cách sử dụng CDF để truyền các thay đổi xuống các bảng hạ nguồn.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Trước tiên, viết một hàm `batch_upsert()` dùng để cập nhật dữ liệu về `rank` cho cho các thay đổi trong bảng của chúng ta.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    # Tạo một partition window với key là order_id và customer_id, cột tham chiếu sắp xếp thời gian là cột _commit_timestamp.
    window = Window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())
    
    (microBatchDF.filter(F.col("_change_type").isin(["insert", "update_postimage"])) # Lọc lấy các kiểu change tạo ra dữ liệu mới (insert/update)
                 .withColumn("rank", F.rank().over(window)) # tạo thêm 1 cột rank bằng window chỉ định 
                 .filter("rank = 1")  # lọc lấy các thay đổi mới nhất
                 .drop("rank", "_change_type", "_commit_version") # dọn bỏ các cột rank sau khi lọc xong
                 .withColumnRenamed("_commit_timestamp", "processed_timestamp") # đổi tên cột
                 .createOrReplaceTempView("ranked_updates"))  # tạo một temp view `ranked_updates`
    
    # Truy vấn MERGE dữ liệu vào bảng target customer_orders sử dụng tempview vừa tạo
    query = """
        MERGE INTO customers_orders c
        USING ranked_updates r
        ON c.order_id=r.order_id AND c.customer_id=r.customer_id
            WHEN MATCHED AND c.processed_timestamp < r.processed_timestamp
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC Tạo bảng target customer_orders.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_orders
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP, processed_timestamp TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC Start defining streaming query:
# MAGIC Ở đây, chúng ta thực hiện join giữa 2 streaming table là __order_silver__ và __customer_silver__. Khi thực hiện join giữa 2 stream-stream với nhau, Spark sẽ __buffer các input trong quá khứ dưới dạng *streaming state*__ ở cả 2 stream, do đó, nó có thể match mọi input trong tương lai với input trong quá khứ và tạo ra các kết quả join tương ứng.

# COMMAND ----------

def porcess_customers_orders():
    # Đọc từ bảng orders dưới dạng streaming source.
    orders_df = spark.readStream.table("orders_silver")
    
    # Đọc từ bảng customers_silver dưới dạng streaming source
    cdf_customers_df = (spark.readStream
                             .option("readChangeData", True)
                             .option("startingVersion", 2)
                             .table("customers_silver")
                       )
    
    query = (
        # Inner Join giữa 2 streaming df order và customers
        orders_df
                .join(cdf_customers_df, ["customer_id"], "inner")
                .writeStream # streaming query to write to the target table
                    .foreachBatch(batch_upsert) # sử dụng foreachBatch xử lý CDC Feed với hàm batch_upsert vừa viết
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_orders")
                    .trigger(availableNow=True) # trigger batch run
                    .start()
            )
    
    query.awaitTermination()
    
porcess_customers_orders()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_orders

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_orders_silver()
bookstore.porcess_customers_silver()

porcess_customers_orders()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM customers_orders

# COMMAND ----------

