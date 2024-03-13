# Databricks notebook source
# MAGIC %md
# MAGIC Như đã biết ta có thể import một Notebook vào trong một Notebook khác để sử dụng

# COMMAND ----------

# MAGIC %run ./helpers/cube_notebook

# COMMAND ----------

c1 = Cube(3)
c1.get_volume()

# COMMAND ----------

# MAGIC %md
# MAGIC Tuy nhiên, vài tính năng của Python sẽ không hoạt động với một file Notebook.
# MAGIC
# MAGIC Đoạn mã import Python sau sẽ không hoạt động với Notebook.

# COMMAND ----------

from helpers.cube_notebook import Cube

# COMMAND ----------

# MAGIC %md
# MAGIC Thay vì nằm trong một Notebook, mã cần nằm trong một Python file (`*.py` file).
# MAGIC
# MAGIC > __Lưu ý__: Nếu view các tệp notebook ở ngoài Databricks, bản chất chúng vẫn là các file python `*.py` thông thường. Tuy nhiên, các file này sẽ có thêm một comment đặc biệt ở đầu để đánh dấu giúp Databricks nhận diện đó là một notebook.
# MAGIC >
# MAGIC >`# Databricks notebook source`
# MAGIC >
# MAGIC > __*Trong phạm vi Demo này, sẽ hiểu các Python file là một file `*.py` và không có comment đó ở đầu.*__

# COMMAND ----------

from helpers.cube import Cube_PY

# COMMAND ----------

c2 = Cube_PY(3)
c2.get_volume()

# COMMAND ----------

# MAGIC %md
# MAGIC Vì file `cube.py` nằm trong một sub-directory trong cùng một directory với Notebook này, nên việc import trên dễ dàng hoạt động bình thường.
# MAGIC
# MAGIC Dùng câu lệnh shell `pwd` (Print working directory) để kiểm tra.

# COMMAND ----------

# MAGIC %sh pwd

# COMMAND ----------

# MAGIC %sh ls ./helpers

# COMMAND ----------

# MAGIC %md
# MAGIC Để import các Python file từ bên ngoài current working directory, trong Python, cần thực hiện thêm một số thủ thuật.
# MAGIC
# MAGIC Câu lệnh `sys.path` sau liệt kê tất cả các đường dẫn mà Python hiện dùng để tìm kiếm modules.

# COMMAND ----------

import sys

for path in sys.path:
    print(path)

# COMMAND ----------

# MAGIC %md
# MAGIC Để import các file từ bên ngoài working directory hiện tại. Ta cần nối đường dẫn của chúng vào danh sách này.

# COMMAND ----------

import os
sys.path.append(os.path.abspath('../modules'))

# COMMAND ----------

# MAGIC %md
# MAGIC Kết quả sau khi nối, ta thấy thư mục modules đã có trong danh sách.

# COMMAND ----------

for path in sys.path:
    print(path)

# COMMAND ----------

# MAGIC %md
# MAGIC Giờ, có thể thực hiện import file từ đường dẫn `modules` như bình thường.

# COMMAND ----------

from shapes.cube import Cube as CubeShape

# COMMAND ----------

c3 = CubeShape(3)
c3.get_volume()

# COMMAND ----------

# MAGIC %md
# MAGIC `%pip` magic để chạy các câu lệnh `pip`. 
# MAGIC
# MAGIC Câu lệnh sau sẽ cài đặt một python wheel vào Notebook session hiện tại.

# COMMAND ----------

# MAGIC %pip install ../wheels/shapes-1.0.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC Sau khi cài đặt, Python interpreter cần được restart. Việc này sẽ xoá sạch các `variable` hiện có trong Notebook. Do đó, các câu lệnh `pip` nên được đặt ở đầu Notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC Sau khi install, ta có thể import các class từ wheel này.

# COMMAND ----------

from shapes_wheel.cube import Cube as Cube_WHL

# COMMAND ----------

c4 = Cube_WHL(3)
c4.get_volume()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tại sao không dùng shell magic để chạy câu lệnh pip?

# COMMAND ----------

# MAGIC %md
# MAGIC Như đã biết, Databricks cluster có thể bao gồm nhiều các worker node khác nhau. 
# MAGIC
# MAGIC Bản chất, các shell magic chỉ thực thi shell command trên local node, trong khi PIP magic thực thi trên tất cả các node trong cluster hiện tại.

# COMMAND ----------

#%sh pip install ../wheels/shapes-1.0.0-py3-none-any.whl