在 PySpark 中使用 Scala 编写的 `Aggregator` 需要将其转换为 **UserDefinedAggregateFunction (UDAF)** 并通过 JAR 加载。以下是详细步骤：

---

### 步骤 1：编写 Scala Aggregator (适配为 UDAF)
假设你有一个 Scala `Aggregator` 如下（以计算分位数为例）：

```scala
// TDigestAggregator.scala
package com.example

import org.apache.spark.sql.expressions.{Aggregator, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, Row}
import com.tdunning.math.stats.TDigest

// 定义 Aggregator
class TDigestAggregator(compression: Double = 100.0) 
  extends Aggregator[Double, Array[Byte], Double] {

  override def zero: Array[Byte] = TDigest.createAvlTreeDigest(compression).toBytes()

  override def reduce(buffer: Array[Byte], input: Double): Array[Byte] = {
    val digest = TDigest.fromBytes(buffer)
    digest.add(input)
    digest.toBytes()
  }

  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = {
    val digest1 = TDigest.fromBytes(b1)
    val digest2 = TDigest.fromBytes(b2)
    digest1.add(digest2)
    digest1.toBytes()
  }

  override def finish(reduction: Array[Byte]): Double = {
    TDigest.fromBytes(reduction).quantile(0.5)
  }

  override def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

// 将 Aggregator 包装为 UDAF
object TDigestUDAF extends UserDefinedAggregateFunction {
  private val aggregator = new TDigestAggregator()

  override def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil
  override def bufferSchema: StructType = StructType(StructField("buffer", BinaryType) :: Nil
  override def dataType: DataType = DoubleType
  override def deterministic: Boolean = true

  override def initialize(buffer: Row): Unit = {
    buffer.update(0, aggregator.zero)
  }

  override def update(buffer: Row, input: Row): Unit = {
    val newBuffer = aggregator.reduce(buffer.getAs[Array[Byte]](0), input.getAs[Double](0))
    buffer.update(0, newBuffer)
  }

  override def merge(buffer1: Row, buffer2: Row): Unit = {
    val merged = aggregator.merge(buffer1.getAs[Array[Byte]](0), buffer2.getAs[Array[Byte]](0))
    buffer1.update(0, merged)
  }

  override def evaluate(buffer: Row): Double = {
    aggregator.finish(buffer.getAs[Array[Byte]](0))
  }
}
```

---

### 步骤 2：打包 Scala 代码为 JAR
在 `build.sbt` 中添加依赖和打包配置：
```scala
// build.sbt
name := "spark-aggregator"
version := "1.0"
scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.1" % Provided,
  "com.tdunning" % "t-digest" % "3.2"
)

// 打包包含依赖的 Fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
```

生成 JAR：
```bash
sbt assembly
# 输出路径：target/scala-2.13/spark-aggregator-assembly-1.0.jar
```

---

### 步骤 3：在 PySpark 中加载 JAR 并注册 UDAF
```python
from pyspark.sql import SparkSession

# 初始化 Spark 并加载 JAR
spark = SparkSession.builder \
    .appName("Scala Aggregator Demo") \
    .config("spark.jars", "/path/to/spark-aggregator-assembly-1.0.jar") \
    .getOrCreate()

# 注册 Scala UDAF
spark.udf.registerJavaFunction(
    "tdigest_median",  # 在 SQL 中使用的函数名
    "com.example.TDigestUDAF"  # Scala 类的全限定名
)

# 测试数据
data = [(1, 10.0), (1, 20.0), (2, 30.0), (2, 40.0)]
df = spark.createDataFrame(data, ["key", "value"])
df.createOrReplaceTempView("data")

# 调用 UDAF
result = spark.sql("""
    SELECT key, tdigest_median(value) AS median 
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

### 关键注意事项

1. **类型匹配**  
   - `Aggregator` 的输入类型 (`Double`) 和输出类型 (`Double`) 必须与 PySpark DataFrame 的列类型一致。
   - 如果使用复杂类型（如自定义对象），需要确保 Scala 和 PySpark 的序列化兼容。

2. **性能优化**  
   - 跨语言调用（JVM ↔ Python）会有性能损耗，建议在高吞吐场景下优先使用原生 PySpark UDF。
   - 使用 `BinaryType` 传输中间结果可以减少序列化开销。

3. **依赖管理**  
   - JAR 必须包含所有运行时依赖（如 `t-digest`），否则会抛出 `ClassNotFoundException`。

4. **版本兼容性**  
   - Scala 版本需与 Spark 的 Scala 版本一致（例如 Spark 3.4.1 使用 Scala 2.13.10）。
   - JDK 版本需兼容（推荐 JDK 8/11）。

---

### 常见错误及解决

| 错误信息                        | 原因                   | 解决方案                          |
|---------------------------------|------------------------|-----------------------------------|
| `ClassNotFoundException`        | JAR 未正确加载或依赖缺失 | 检查 JAR 路径和 `build.sbt` 依赖  |
| `NoSuchMethodError`             | 版本不兼容             | 统一 Scala、Spark 和依赖版本      |
| `Type mismatch`                 | 输入/输出类型不匹配    | 检查 `inputSchema` 和 `dataType`  |
| `Serialization error`           | 中间类型序列化失败     | 使用 `BinaryType` 简化数据传输    |

---

通过以上步骤，你可以在 PySpark 中无缝使用 Scala 编写的 `Aggregator`，实现复杂聚合逻辑的高效复用。
