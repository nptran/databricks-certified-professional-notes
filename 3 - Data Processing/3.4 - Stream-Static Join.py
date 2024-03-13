# Databricks notebook source
# MAGIC %md
# MAGIC ### Tài liệu này mô tả cách thực hiện Join giữa một Stream với một Static dataset

# COMMAND ----------

# MAGIC %md
# MAGIC Demo này sẽ tạo ra một silver table là books_sales từ hai bảng orders (streaming table) và current_books (static table).
# MAGIC
# MAGIC Có thể quan sát sơ đồ bên dưới, current_books được update bằng một batch overwrite logic nên nó phá vỡ qui tắc ever-appending source của Structured streaming.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/books_sales.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC Bên dưới là hàm định nghĩa truy vấn streaming sẽ được sử dụng trong Demo này. Chạy để tạo và khởi chạy truy vấn.

# COMMAND ----------

from pyspark.sql import functions as F

def process_books_sales():
    # Đọc bảng order_silver dưới dạng một streaming source bằng Spark API `spark.readStream`
    orders_df = (spark.readStream.table("orders_silver")
                        .withColumn("book", F.explode("books"))
                )

    # Đọc bảng static current_books bằng Spark API `spark.read` 
    books_df = spark.read.table("current_books")

    query = (
        # Thực hiện join hai df với nhau
        orders_df
                  .join(books_df, orders_df.book.book_id == books_df.book_id, "inner")
                  # Chạy một trigger now batch để xử lý dữ liệu và ghi dữ liệu
                  .writeStream
                     .outputMode("append") # ghi bằng append mode
                     .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_sales")
                     .trigger(availableNow=True)
                     .table("books_sales")
    )

    query.awaitTermination()
    
process_books_sales()

# COMMAND ----------

# MAGIC %md
# MAGIC Kiểm tra xem truy vấn trên đã load được dữ liệu vào bảng `books_sales` hay chưa:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_sales

# COMMAND ----------

# MAGIC %md
# MAGIC Số lượng bản ghi được nạp vào từ truy vấn join hiện tại là `5089` bản ghi.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM books_sales

# COMMAND ----------

# MAGIC %md
# MAGIC Thử thực hiện tải thêm dữ liệu vào cho bảng nguồn `current_books`. Đây là bảng Static, do đó, việc thêm dữ liệu vào bảng này sẽ __không trigger quá trình join__.

# COMMAND ----------

bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_books_silver()
bookstore.process_current_books()

process_books_sales()

# COMMAND ----------

# MAGIC %md
# MAGIC Vì quá trình join không được kích hoạt nên số lượng bản ghi vẫn sẽ là `5089`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM books_sales

# COMMAND ----------

# MAGIC %md
# MAGIC Tiến hành thêm bản ghi vào bảng stream orders. Việc này  __sẽ kích hoạt quá trình join__ và làm thay đổi số lượng dữ liệu trong bảng đích `books_sales`.

# COMMAND ----------

bookstore.porcess_orders_silver()

process_books_sales()

# COMMAND ----------

# MAGIC %md
# MAGIC Số lượng bản ghi trong bảng đích đã tăng từ `5089` lên `5582` do có dữ liệu được thêm vào trong bảng streaming là `orders`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM books_sales

# COMMAND ----------

