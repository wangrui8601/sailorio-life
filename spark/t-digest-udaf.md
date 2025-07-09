在 PySpark 中使用 Scala 编写的 UDAF 函数需要将 Scala 代码编译为 JAR 文件并通过 PySpark 加载。以下是详细步骤：

---

### **步骤1：编写并打包 Scala UDAF**
#### 1.1 确保 Scala 代码正确实现 UDAF
```scala
// TDigestUDAF.scala
package com.example

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import com.tdunning.math.stats.TDigest

class TDigestUDAF extends UserDefinedAggregateFunction {
  // 定义输入数据类型（Double类型）
  def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

  // 定义缓冲区类型（BinaryType存储序列化的TDigest）
  def bufferSchema: StructType = StructType(StructField("buffer", BinaryType) :: Nil)

  // 定义输出类型（Double类型，例如中位数）
  def dataType: DataType = DoubleType

  // 是否幂等
  def deterministic: Boolean = true

  // 初始化缓冲区
  def initialize(buffer: Row): Unit = {
    val digest = TDigest.createAvlTreeDigest(100.0)
    buffer.update(0, digest.toBytes)
  }

  // 更新缓冲区（合并数据）
  def update(buffer: Row, input: Row): Unit = {
    val digest = TDigest.fromBytes(buffer.getAs[Array[Byte]](0))
    digest.add(input.getDouble(0))
    buffer.update(0, digest.toBytes)
  }

  // 合并多个缓冲区
  def merge(buffer1: Row, buffer2: Row): Unit = {
    val digest1 = TDigest.fromBytes(buffer1.getAs[Array[Byte]](0))
    val digest2 = TDigest.fromBytes(buffer2.getAs[Array[Byte]](0))
    digest1.add(digest2)
    buffer1.update(0, digest1.toBytes)
  }

  // 计算最终结果（例如中位数）
  def evaluate(buffer: Row): Double = {
    val digest = TDigest.fromBytes(buffer.getAs[Array[Byte]](0))
    digest.quantile(0.5)
  }
}
```

#### 1.2 添加 `build.sbt` 配置
```scala
// build.sbt
name := "spark-udaf"
version := "1.0"
scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1" % Provided,
  "org.apache.spark" %% "spark-sql" % "3.4.1" % Provided,
  "com.tdunning" % "t-digest" % "3.2"
)

// 打包包含依赖的 JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
```

#### 1.3 生成 Fat JAR
```bash
sbt assembly
# 生成的 JAR 路径：target/scala-2.13/spark-udaf-assembly-1.0.jar
```

---

### **步骤2：在 PySpark 中加载 JAR**
#### 2.1 启动 PySpark 时指定 JAR
```bash
pyspark --jars /path/to/spark-udaf-assembly-1.0.jar
```

#### 2.2 或在代码中动态加载
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Scala UDAF Demo") \
    .config("spark.jars", "/path/to/spark-udaf-assembly-1.0.jar") \
    .getOrCreate()
```

---

### **步骤3：注册并使用 Scala UDAF**
#### 3.1 注册 UDAF
```python
# 注册 Scala UDAF
spark.udf.registerJavaFunction(
    "tdigest_udaf",  # 在 SQL 中使用的函数名
    "com.example.TDigestUDAF"  # Scala 类的全限定名
)
```

#### 3.2 使用 UDAF 计算分位数
```python
# 示例数据
data = [(1, 10.0), (1, 20.0), (2, 30.0), (2, 40.0)]
df = spark.createDataFrame(data, ["key", "value"])

# 使用 SQL 调用 UDAF
df.createOrReplaceTempView("data")
result = spark.sql("""
    SELECT key, tdigest_udaf(value) AS median 
    FROM data 
    GROUP BY key
""")
result.show()

# 输出结果
# +---+------+
# |key|median|
# +---+------+
# |  1|  15.0|
# |  2|  35.0|
# +---+------+
```

---

### **步骤4：验证与调试**
#### 4.1 检查 JAR 是否加载成功
```python
print(spark.sparkContext._jsc.sc().listJars())
# 应包含 spark-udaf-assembly-1.0.jar
```

#### 4.2 处理常见错误
- **ClassNotFoundException**：检查 JAR 路径和类名拼写。
- **NoSuchMethodError**：确保 Scala 和 Spark 版本兼容。
- **序列化错误**：确认所有依赖已打包到 JAR 中。

---

### **完整代码示例**
```python
from pyspark.sql import SparkSession

# 初始化 Spark 并加载 JAR
spark = SparkSession.builder \
    .appName("Scala UDAF Demo") \
    .config("spark.jars", "/path/to/spark-udaf-assembly-1.0.jar") \
    .getOrCreate()

# 注册 UDAF
spark.udf.registerJavaFunction("tdigest_udaf", "com.example.TDigestUDAF")

# 创建测试数据
data = [(1, 10.0), (1, 20.0), (2, 30.0), (2, 40.0)]
df = spark.createDataFrame(data, ["key", "value"])

# 使用 UDAF 计算中位数
df.createOrReplaceTempView("data")
result = spark.sql("SELECT key, tdigest_udaf(value) AS median FROM data GROUP BY key")
result.show()

# 输出结果
# +---+------+
# |key|median|
# +---+------+
# |  1|  15.0|
# |  2|  35.0|
# +---+------+
```

---

### **关键注意事项**
1. **依赖管理**：确保 JAR 包含所有依赖（如 `t-digest`），避免运行时找不到类。
2. **版本兼容性**：Scala、Spark 和 Python 的版本需一致（如 Scala 2.13 对应 Spark 3.x）。
3. **性能优化**：跨语言调用会有性能损耗，建议在高频场景下优先使用 PySpark 原生 UDF。
