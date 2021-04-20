# Spark MLlib

## What is it?

MLlib là một thư viện machinelearning của Spark. Mục đích của nó là tạo nên những chương trình machine learning thực tế và có thể mở rộng dễ dàng.
- ML Algorithms: những thuật toán học máy thông dụng như classification, regession, clustering và collaborative filtering.
- Featurization: các phương pháp trích xuất thuộc tính - đặc trưng, transformation, giảm chiều dữ liệu, và chọn lọc dữ liệu.
- Pipelines: công cụ cho xây dựng, đánh giá, và điều chỉnh ML Pipelines
- Persistence: lưu trữ và tải các thuật toán, các mô hình và các Pipelines.

## ML Pipelines
Ở phần này chúng ta sẽ tiếp cận khái niệm ML Pipelines. ML Pipelines cung cấp một tập không đổi các API bậc cao được xây dựng trên DataFrame, điều này giúp các người sử dụng tạo và điều chỉnh/chỉnh sửa machine learning pipelines thực tế.

## Các khái niệm chính trong Pipelines.

- DataFrame: ML API này sử dụng DataFrame từ Spark SQL như một ML dataset cái mà có thể lưu giữ một lượng đa dạng các kiểu dữ liệu. Dataframe có thể có nhiều cột lưu trữ cả chữ, vector thuộc tính, các nhãn đúng và các dự đoán.
- Transformer: Một transormer là một thuật toán biến đổi một DataFrame thành một DataFrame khác. Một mô hình ML là một transformer biến đổi DataFrame với các thuộc tính thành một DataFrame với các dự đoán.
- Estimator: Một Estimator là một thuật toán có thể `fit` trên DataFrame và tạo ra một transformer. Một thuật toán học là một Estimator, cái có thể `trains` trên DataFrame và tạo ra một model.
- Pipeline: Một Pipeline kết nối nhiều Transformer và Estimators lại với nhau để xác định một luồng làm việc của ML.
- Parameter: Tất cả Transformers và Estimators chia sẻ một API để chỉ định các tham số.

### DataFrame
Machine learning có thể áp dụng vào rất nhiều loại dữ liệu như vectors, text, images và dữ liệu có cấu trúc. API này thông qua DataFrame từ Spark SQL để hỗ trợ đa dạng kiểu dữ liệu.

Một DataFrame có thể được tạo một cách rõ ràng hoặc không rõ ràng từ một RDD thông thường.

Phẩn code Demo sẽ nằm ở phần dưới của bài viết này.


### Pipeline components

#### Transformers

Một Transformer là một sự trườu tượng bao gồm các feature transformer và learned models. Nói một cách kỹ thuật thì một Transformer hiện thực một phương thức `transform()` biến đổi một DataFrame thành một cái khác, thông thường là bằng cách thêm một hoặc nhiều cột. Ví dụ như:
- Một feature transformer nhận vào một DataFrame, đọc một cột, map nó sang một cột mới và đầu ra của nó là một DataFrame mới với một cột đã được map thêm vào sau đó.
- Một mô hình học máy nhận vào một DataFrame, đọc vào một chứa các feature vector, dự đoán nhãn cho mỗi feature vector và đầu ra là một DataFrame mới với các nhãn được dự đoán đã được thêm vào dưới dạng 1 cột.

#### Estimators
Một Estimator rút trích khái niệm của một thuật toán học máy hay bất kì thuật toán nào `fits` or `trains` trên dữ liệu. Nói một cách kĩ thuật thì một Estimator hiện thực lại phương thức `fit()`, phương thức này nhận một DataFrame và tạo ra một Model là một Transformer. Ví dụ, một thuật toán học máy như Logistic Regression là một Estimator và gọi hàm `fit()` để `trains` một LogisticRegressionModel là một Model và do đó nên nó là một Transformer.

#### Properties of pipeline coomponents

`Transformer.transform()` và `Estimator.fit()` đều không được công nhận, trong tương lai, các thuật toán stateful có lẽ được hỗ trợ thông qua khái niệm khác.

Mỗi đối tượng của một Transformer hoặc Estimator có một ID không trùng lặp, ID này hữu dụng trong việc chỉ định rõ các parameter( đề cập sau).

#### Pipeline

Trong học máy, nó rất thông dụng để chạy một chuỗi các thuật toán để xử lí và học từ data. Một quá trình xử lí tài liệu chữ thường sẽ chứa những bước sau:
- `Split` text trong mỗi tài liệu thành các từ.
- Biến đổi mỗi từ của tài liệu thành một vector số.
- Học một mô hình dự đoán sử dụng feature vectors và labels.

#### Cách nó hoạt động ra sao?
![image](https://user-images.githubusercontent.com/57447237/115342399-68e66380-a1d4-11eb-8f66-5bb6dda016cc.png)
![image](https://user-images.githubusercontent.com/57447237/115342455-81567e00-a1d4-11eb-9b5e-0ed637b7697c.png)


#### Parameters
MLlib Estimators và Transformers sử dụng một API đồng nhất để chỉ định tham số.

Một *Param* là một parameter đã được đặt tên với dữ liệu độc lập. Một ParamMap là một tập hợp các cặp (parameter, value)

Có 2 cách để truyền các parameter vào một thuật toán:
1. Cài đặt các parameter cho một đối tượng. nếu `lr` là một đối tượng của LogisticRegression, nó có thể gọi hàm `lr.setMaxIter(10)` để làm cho hàm `lr.fit()` lặp 10 lần. API này giống với API sử dụng trong `spark.mllib` package
2. Truyền một ParamMap vào `fit()` hoặc `transform()`. Bất kì parameters trong ParamMap sẽ chạy đè lên parameters trước đó được chỉ định thông qua phương thức setter.

#### ML persistence: Saving and Loading Pipelines
ML persistence hoạt động trên Scala, Java và Python. Tuy nhiên, R hiện đang sử dụng một định dạng đã sửa đổi, vì vậy các mô hình được lưu trong R chỉ có thể được tải trở lại trong R; điều này sẽ được khắc phục trong tương lai và được theo dõi trong SPARK-15572.

## Code Example
[Example: Estimator, Transformer and Param](https://github.com/huanlam2000/Spark_MLlib_DataFrame/blob/main/Estimator_Transformer_Param.ipynb)

[Example: Pipeline](https://github.com/huanlam2000/Spark_MLlib_DataFrame/blob/main/Pipeline.ipynb)



# Reference
[Spark MLlib](http://spark.apache.org/docs/latest/ml-pipeline.html#main-concepts-in-pipelines)
