在Spark中使用t-digest实现分位数计算并输出中间结果的步骤如下：

### 步骤1：添加依赖
在项目的构建文件（如build.sbt）中添加t-digest依赖：
```scala
libraryDependencies += "com.tdunning" % "t-digest" % "3.2"
```

### 步骤2：定义聚合函数和UDF
```scala
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, functions}
import com.tdunning.math.stats.TDigest

// 将原始数据聚合为t-digest的二进制结构
class TDigestBuildAggregator(compression: Double = 100.0) extends Aggregator[Double, Array[Byte], Array[Byte]] {

  override def zero: Array[Byte] = serialize(TDigest.createAvlTreeDigest(compression))

  override def reduce(buffer: Array[Byte], data: Double): Array[Byte] = {
    val digest = deserialize(buffer)
    digest.add(data)
    serialize(digest)
  }

  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = {
    val digest1 = deserialize(b1)
    val digest2 = deserialize(b2)
    digest1.add(digest2)
    serialize(digest1)
  }

  override def finish(reduction: Array[Byte]): Array[Byte] = reduction

  override def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY
  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY

  private def serialize(digest: TDigest): Array[Byte] = digest.toBytes()
  private def deserialize(bytes: Array[Byte]): TDigest = TDigest.fromBytes(bytes)
}

// 注册聚合函数
val tdigestBuildAgg = new TDigestBuildAggregator(100.0)
spark.udf.register("tdigest_build", functions.udaf(tdigestBuildAgg))

// 定义合并多个 t-digest 的聚合函数（输入类型为 Array[Byte]）
class TDigestMergeAggregator(compression: Double = 100.0) 
  extends Aggregator[Array[Byte], Array[Byte], Array[Byte]] {

  override def zero: Array[Byte] = serialize(TDigest.createAvlTreeDigest(compression))

  override def reduce(buffer: Array[Byte], inputDigestBytes: Array[Byte]): Array[Byte] = {
    val bufferDigest = deserialize(buffer)
    val inputDigest = deserialize(inputDigestBytes)
    bufferDigest.add(inputDigest) // 合并输入的 t-digest 到 buffer
    serialize(bufferDigest)
  }

  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] = {
    val digest1 = deserialize(b1)
    val digest2 = deserialize(b2)
    digest1.add(digest2)
    serialize(digest1)
  }

  override def finish(reduction: Array[Byte]): Array[Byte] = reduction

  override def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY
  override def outputEncoder: Encoder[Array[Byte]] = Encoders.BINARY

  private def serialize(digest: TDigest): Array[Byte] = digest.toBytes()
  private def deserialize(bytes: Array[Byte]): TDigest = TDigest.fromBytes(bytes)
}

// 注册合并用的 UDAF
val tdigestMergeAgg = new TDigestMergeAggregator(100.0)
spark.udf.register("tdigest_merge", functions.udaf(tdigestMergeAgg))

// 定义计算分位数的UDF
val getQuantileUDF = (bytes: Array[Byte], q: Double) => {
  val digest = TDigest.fromBytes(bytes)
  digest.quantile(q)
}
spark.udf.register("get_quantile", getQuantileUDF)
```

### 步骤3：生成中间结果
```scala
// 处理原始数据，生成每个分组的t-digest中间结果
val intermediateDF = spark.sql("""
  SELECT key, tdigest_build(value) AS tdigest 
  FROM your_table 
  GROUP BY key
""")
intermediateDF.write.saveAsTable("intermediate_tdigests")
```

### 步骤4：合并中间结果并计算分位数
若中间结果需要进一步合并（如跨多个批次或分区）：
```scala
// 合并多个 t-digest 中间结果（例如跨分区或批次）
val mergedDF = spark.sql("""
  SELECT key, tdigest_merge(tdigest) AS merged_tdigest 
  FROM intermediate_tdigests 
  GROUP BY key
""")

// 计算最终分位数
val resultDF = mergedDF.selectExpr(
  "key", 
  "get_quantile(merged_tdigest, 0.5) AS median",
  "get_quantile(merged_tdigest, 0.9) AS p90"
)
resultDF.show()
```

---

### **关键区别说明**

| 函数名         | 输入类型       | 用途                                |
|----------------|----------------|-----------------------------------|
| `tdigest_build` | `Double`       | 从原始数据构建 t-digest（逐点添加）   |
| `tdigest_merge` | `Array[Byte]`  | 合并多个已存在的 t-digest 结构       |

- **为什么需要两个 UDAF？**  
  - `tdigest_build` 用于首次聚合原始数据（输入为数值），而 `tdigest_merge` 用于二次聚合已生成的 t-digest 中间结果（输入为二进制数据）。两者操作对象不同，需分开实现。

---

### **完整工作流程**

1. **首次聚合（原始数据 → t-digest）**  
   ```sql
   SELECT key, tdigest_build(value) AS tdigest 
   FROM raw_data 
   GROUP BY key
   ```

2. **合并中间结果（t-digest → 合并后的 t-digest）**  
   ```sql
   SELECT key, tdigest_merge(tdigest) AS merged_tdigest 
   FROM intermediate_tdigests 
   GROUP BY key
   ```

3. **计算最终分位数**  
   ```sql
   SELECT key, get_quantile(merged_tdigest, 0.5) AS median 
   FROM merged_tdigests
   ```

---

### **验证步骤**
确保合并后的分位数计算与直接处理全量数据一致：
```scala
// 全量数据直接计算中位数
val groundTruth = spark.sql("SELECT key, percentile_approx(value, 0.5) FROM raw_data GROUP BY key")

// 对比 t-digest 结果与全量计算结果
resultDF.join(groundTruth, "key").show()
```