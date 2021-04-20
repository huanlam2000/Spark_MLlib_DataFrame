# Spark DataFrame

## What is it?

DataFrame là một tập hợp dữ liệu phân tán được tổ chức theo các cột đã được đặt tên. Khái niệm này tương đương với một bản trong cơ sở dữ liệu quan hệ hay DataFrame trong R/Python, nhưng nó có khả năng tối ưu hóa phong phú hơn. DataFrame có thể được xây dựng từ một mảng lớn các tài nguyên như các file dữ liệu đã được cấu trúc, bảng, hoặc cơ sở dữ liệu bên ngoài hoặc một RDDs đã có sẵn.

Có một điểm khác biệt giữa PySpark DataFrame và Pandas DataFrame đó là PySpark DataFrame được lưu trữ phân tán trên cluster( có nghĩa là DataFrame được lưu trong nhiều máy khác nhau trong cluster) và bất cứ phép tính/ hoạt động nào trong PySpark thì đều chạy song song trên tất cả các máy chủ trong khi Pandas DataFrame lưu và sử dụng/ tính toán trên 1 máy chủ đơn lẻ.


## Is PySpark faster than Pandas?

Bởi vì thực thi song song trên tất cả các `cores` trên nhiều máy chủ, Pyspark thực thi các `operation` nhanh hơn Pandas. Nói cách khác, Pandas thực thi các `operation` trên  `single node` trong khi PySpark thực thi trên nhiều máy chủ (`multiple machines`).


## DataFrame Creation
Cách đơn giản nhất để tạo DataFrame là từ một Python list chứa dữ liệu. DataFrame cũng có thể được tạo từ một RDD bằng cách đọc file từ nhiều nguồn.

- Sử dụng hàm `createDataFrame()`: bằng cách dùng hàm `createDataFrame()` của SparkSession ta có thể tạo một DataFrame, dưới đây là ví dụ:

      df = spark.createDataFrame(data = data, schema = columns)

- Sử dụng hàm  `toDF()`: bằng cách sử dụng hàm `toDF()` ta có thể biến đổi một RDD thành DataFrame, bên dưới là một ví dụ:
    
      df = rdd.toDF()

Vì DataFrame có định dạng cấu trúc chứa tên và cột, chúng ta có thể lấy `schema` của DataFrame bằng cách sử dụng `df.printSchema()`

Output sẽ có format như thế này:


   ![Schema](https://user-images.githubusercontent.com/57447237/115386538-b928ea00-a203-11eb-862d-cf8d3d2b3ce6.png)

- Sử dụng hàm  `read`:
  - `spark.read.csv`: tạo DataFrame từ file `CSV`.

        df = spark.read.csv(filepath)
    
  - `spark.read.text`: tạo DataFrame từ file `text`.

        df = spark.read.text(filepath)
    
  - `spark.read.json`: tạo DataFrame từ file `JSON`( Javascript Objec Notation).

        df = spark.read.json(filepath)
    
  - `spark.read.parquet`: tạo DataFrame từ `parquet file`.

        df = spark.read.parquet(filepath)
    
Hàm `df.show()` cho ta thấy 20 dòng đầu tiên từ DataFrame.

![show](https://user-images.githubusercontent.com/57447237/115387519-06f22200-a205-11eb-95e7-f1ab51472a0a.jpg)

## Rename Columns on DataFrame

Chúng ta thường xuyên cần phải đổi tên một hoặc nhiều (hoặc tất cả) cột trên PySpark DataFrame, có rất nhiều cách.

Vì DataFrame là một tập hợp không thay đổi, ta không thể đổi tên hay cập nhật một cột. Thay vào đó, khi sử dụng `withColumnRenamed()`, nó sẽ tạo ra một DataFrame mới với những cột đã được cập nhật lại tên.
### Rename a column

    df.withColumnRenamed(existingName, newName)
   
### Rename multiple columns
- first way:


       df.withColumnRenamed(existingName1, newName1) \
          .withColumnRenamed.(existingName2, newName2)
       
- second way:

      listNameColumns = [oldName1 : newName1, oldName2 : newName2, oldName3 : newName3]
      
      
      for old, new in listNameColumns:
        df = df.withColumnRenamed(old, new)
        
        
### Using withColumn to rename nested columns

    from pyspark.sql.functions import *
    df2 = df.withColumn("newCol1", col(parentColName.childrenColName1)) \
            .withColumn("newCol2", col(parentColName.childrenColName2)) \
            .withColumn("newCol3", col(parentColName.childrenColName3)) \
            .drop(parentColName)
            
#### Another usage of withColumn function
#### Change DataType: 

        df.withColumn(newCol, col(existingCol).cast(datatype))

#### Update value of an existing column:

        df.withColumn(existingCol, col(existingCol)*100)
            
#### Create a column from an existing:

       df.withColumn(newCol, col(existingCol) * -1) 
       
## Filter function
Hàm `filter()` của PySpark được dùng để lọc các dòng dữ liệu từ RDD hoặc DataFrame dựa trên điều kiện cho trước hoặc câu truy vấn SQL,
hoặc cũng có thể dùng `where()` thay cho `filter()` nếu thuần thục SQL, cả hai hàm đó hoạt động như nhau.

Cú pháp: `filter(condition)`

Giả sử ta có một DataFrame như hình bên dưới:

![DataFrame](https://user-images.githubusercontent.com/57447237/115392633-fa70c800-a20a-11eb-9939-9d6ddde4c3b0.png)

Và ta chỉ muốn tìm những dòng dữ liệu có State = OH, ta sẽ làm như sau:

      df.filter(df.state == "OH").show()
      
Kết quả: 

![OH](https://user-images.githubusercontent.com/57447237/115393085-808d0e80-a20b-11eb-93ce-7962e6303255.png)

### Filter với  `multiple conditions`:

      df.filter( (df.state == "OH") & (df.gender == "M") ) \
         .show()
         
Kết quả: 

![multiple](https://user-images.githubusercontent.com/57447237/115393452-e9748680-a20b-11eb-9a15-e2d6a5ff9f17.png)

### Filter dựa trên `List Values`:

      li = ["OH", "CA", "DE"]
      df.filter(df.state.isin(li)).show()
      
Kết quả: 

![ListBase](https://user-images.githubusercontent.com/57447237/115393721-2e98b880-a20c-11eb-8440-92016d79ce82.png)

### Filter dựa trên `Starts With`, `Ends With`, `Contains`:

Chúng ta cũng có thể filter DataFrame bằng cách dùng `startswith()`, `endswith()`, và `contains()` những phương thức này thuộc lớp Column.

      df.filter(df.state.startswith("N")).show()
      
Kết quả:

![startswith](https://user-images.githubusercontent.com/57447237/115394220-c39bb180-a20c-11eb-98a8-328102624281.png)

      
      df.filter(df.state.endswith("H")).show()
      
      df.filter(df.state.contains("H")).show()


### Filter `like` and `rlike`

Nếu bạn có kiến thức về SQL, bạn sẽ quen thuộc với `like` và `rlike`(regex like), Pyspark cũng cung cấp những phương thức tương tự trong lớp Column để lọc các giá trị tương tự bằng cách sử dụng các kí hiệu đại diện.

#### Tạo DataFrame đơn giản:

      data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
                (4,"Rames Rose"),(5,"Rames rose")]
      df2 = spark.createDataFrame(data = data2, schema = ["id","name"])
      
#### Lọc dữ liệu bằng `like`:

      df2.filter(df2.name.like("%rose%")).show()
      
Kết quả như sau:
    
![like](https://user-images.githubusercontent.com/57447237/115395481-20e43280-a20e-11eb-8d73-fd3dd0c7a2d0.png)

#### Lọc dữ liệu bằng `rlike`:
  
    df2.filter(df2.name.rlike("(?i)^*rose$")).show()

![rlike](https://user-images.githubusercontent.com/57447237/115395738-6acd1880-a20e-11eb-80a5-8d2bd0aafc55.png)

### Filter trên một Array column
Khi ta muốn lọc các dòng từ DataFrame dựa trên giá trị xuất biểu diễn trong một mảng tập hợp các cột.
Ví dụ này sẽ sử dụng `array_contains()` từ `Pyspark SQL functions`, nó kiểm tra nếu một giá trị nằm trong một mảng, nếu có nó sẽ trả về true ngược lại trả về false.

      from pyspark.sql.functions import array_contains
      df.filter(array_contains(df.languages,"Java")) \
         .show(truncate=False) 
 
![contains](https://user-images.githubusercontent.com/57447237/115396749-884eb200-a20f-11eb-98c6-80abc517508d.png)


### Filtering on Nested Struct columns:

    df.filter(df.name.lastname == "Williams") \
      .show(truncate=False) 
      
![NestedCol](https://user-images.githubusercontent.com/57447237/115396955-be8c3180-a20f-11eb-8b5d-bdbf35af45a8.png)

## orderBy() and sort()

Giả sử ta có DataFrame như bên dưới:

![Screenshot_1](https://user-images.githubusercontent.com/57447237/115397560-702b6280-a210-11eb-9cbf-f3b2b0ae05a2.png)

### Sorting using sort() function

PySpark DataFrame cung cấp hàm `sort()` để sắp xếp trên một hoặc nhiều cột.

Mặc định, thứ tự sắp xếp là tăng dần.

      df.orderBy("department", "state").show()
      df.sort(col("department"), col("state")).show()

![Screenshot_1](https://user-images.githubusercontent.com/57447237/115397804-b1237700-a210-11eb-9ad3-45af08508756.png)

Cả hai ví dụ trên đều cho ra một kết quả như hình, ví dụ đầu tiên nhận đầu vào là tên các cột dưới dạng String
, ví dụ thứ hai nhận đầu vào là tên cột dưới dạng kiểu Column.

### Sorting using orderby() function

PySpark DataFrame cũng cung cấp hàm `orderBy()` để sắp xếp trên một hoặc nhiều cột.

Mặc định, thứ tự sắp xếp là tăng dần.

### Sor by Ascending (ASC)

     df.sort(df.department.asc(),df.state.asc()).show(truncate=False)
     df.sort(col("department").asc(),col("state").asc()).show(truncate=False)
     df.orderBy(col("department").asc(),col("state").asc()).show(truncate=False)

Cả 3 ví dụ trên đều trả về cùng một kết quả bên dưới:

![Screenshot_1](https://user-images.githubusercontent.com/57447237/115399382-54c15700-a212-11eb-922e-7fae248e44e1.png)

### Sort by Decending (DESC)

    df.sort(df.department.asc(),df.state.desc()).show(truncate=False)
    df.sort(col("department").asc(),col("state").desc()).show(truncate=False)
    df.orderBy(col("department").asc(),col("state").desc()).show(truncate=False)

![Screenshot_4](https://user-images.githubusercontent.com/57447237/115399499-7a4e6080-a212-11eb-92fb-7a251a244b99.png)

### Sort using Raw SQL

    df.createOrReplaceTempView("EMP")
    spark.sql("select employee_name,department,state,salary,age,bonus from EMP ORDER BY department asc").show(truncate=False)

Ví dụ này trả về kết quả tương tự hình bên trên.


## groupBy()

    df.groupBy("department").sum("salary").show(truncate=False)
    
![Screenshot_5](https://user-images.githubusercontent.com/57447237/115399943-e9c45000-a212-11eb-8617-e8f5d09f313e.png)

Một vài hàm aggregate:
1. `count()` - trả về số lượng dòng trong mỗi nhóm.
2. `mean()` - trả về mean của giá trị cho mỗi nhóm.
3. `max()` - trả về giá trị lớn nhất của mỗi nhóm.
4. `min()` - trả về giá trị nhỏ nhất của mỗi nhóm.
5. `sum()` - trả về tổng giá trị của mỗi nhóm.
6. `avg()` - trả về giá trị trung bình của mỗi nhóm.
7. `agg()` - sử dụng hàm agg() ta có thể tính toán nhiều hơn một hàm aggregate cùng lúc.
8. `pivot()`
