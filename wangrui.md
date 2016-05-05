 课程安排
第一阶段：Java基础课程

该部分内容是针对非计算机专业并且java 零基础的同学设置的。目的是让 这些同学迈入到计算机编程行业，为后面学习大数据课程做准备。
Java基础 	

    多线程
    高并发
    网络编程
    Jvm 优化
    基本语法
    脚本编写

第二阶段：linux 和 shell
linux 和 shell 	

    linux 安装
    linux 常用命令
    shell 编程

第三阶段：数据库相关
数据库相关 	

    mysql 安装与维护
    SQL 语句编写

第四阶段：hadoop2课程

搭建伪分布实验环境： 本节是最基本的课程，属于入门级别，主要讲述在linux单机上面安装hadoop的伪分布模式，在linux集群上面安装hadoop集群。对于不熟悉linux的同学，课程中会简单的讲解常用的linux命令。这两种是必须要掌握的。通过现在的教学发现，很多同学并不能正确的配置环境。
搭建伪分布实验环境 	

    Hadoop概念、版本、历史
    Hadoop和核心组成介绍及hdfs、mapreduce体系结构
    Hadoop的集群结构
    Hadoop伪分布的详细安装步骤
    如何通过命令行和浏览器观察hadoop

介绍HDFS体系结构及shell、java操作方式： 本节是对hadoop核心之一——hdfs的讲解。hdfs是所有hadoop操作的基础，属于基本的内容。对本节内容的理解直接影响以后所有课程的学习。在本节学习中，我们会讲述hdfs的体系结构，以及使用shell、java不同方式对hdfs的操作。在工作中，这两种方式都非常常用。学会了本节内容，就可以自己开发网盘应用了。在本节学习中，我们不仅对理论和操作进行讲解，也会讲解hdfs的源代码，方便部分学员以后对hadoop源码进行修改。最后，还要讲解hadoop的RPC机制，这是hadoop运行的基础，通过该节学习，我们就可以明白hadoop是怎么明白的了，不必糊涂了，本节内容特别重要。
介绍HDFS体系结构及
shell、java操作方式 	

    Hdfs体系结构详述
    NameNode、DataNode、SecondaryNameNode体系结构
    如果保证namenode的高可靠
    Datanode中block的划分原理和存储方式
    如何修改namenode、datanode数据存储位置
    如何使用命令行操纵hdfs
    如何使用java操作hdfs
    介绍rpc机制
    通过查看源码，知晓hadoop是建构在rpc之上的
    通过查看hdfs源码，知晓客户端是如何与Namenode通过rpc通信的

介绍MapReduce体系结构及各种算法(1)： 本节开始对hadoop核心之一——mapreduce的讲解。mapreduce是hadoop的核心，是以后各种框架运行的基础，这是必须掌握的。在本次讲解中，掌握mapreduce执行的详细过程，以单词计数为例，讲解mapreduce的详细执行过程。还讲解hadoop的序列化机制和数据类型，并使用自定义类型实现电信日志信息的统计。
介绍MapReduce体
系结构及各种算法(1) 	

    Mapreduce原理
    Mapreduce执行的八大步骤
    详细讲述如何使用mapreduce实现单词计数功能
    详细讲述如何覆盖Mapper功能、如何覆盖Reducer功能。在各种hadoop认证中，这是考察重点
    详细讲述hadoop的自定义类型Writable接口
    通过电信上网日志实例讲述如何自定义hadoop类型
    实例讲述hadoop1的各种输入来源处理器，包括数据库输入、xml文件、多文件输入等，并且讲解如何自定
    义输入来源处理器
    实例讲述hadoop1的各种输出来源，包括数据库输出、文件输出等，并且讲解如何自定义输出来源处理器，
    实现自定义输出文件名称
    通过源码讲述hadoop是如何读取hdfs文件，并且转化为键值对，供map方法调用的

介绍MapReduce体系结构及各种算法(2)： 本节继续讲解mapreduce，会把旧api的用法、计数器、combiner、partitioner、排序算法、分组算法等全部讲解完毕。通过这两次课程学习，学员可以把整个mapreduce的执行细节搞清楚，把各个可扩展点都搞明白。本节内容在目前市面可见的图书、视频中还没有发现如此全面的哪。
介绍MapReduce体
系结构及各种算法(2) 	

    讲解新旧api的区别，如何使用旧api完成操作
    介绍如何打包成jar，在命令行运行hadoop程序
    介绍hadoop的内置计数器，以及自定义计数器
    介绍合并(combiner)概念、为什么使用、如何使用、使用时有什么限制条件
    介绍了hadoop内置的分区(partitioner)概念、为什么使用、如何使用
    介绍了hadoop内置的排序算法，以及如何自定义排序规则
    介绍了hadoop内置的分组算法，以及如何自定义分组规则
    介绍了mapreduce的常见应用场景，以及如何实现mapreduce算法讲解
    如何优化mapreduce算法，实现更高的运行效率

第五阶段：zookeeper课程

本节内容与hadoop关系不大，只是在hbase集群安装时才用到。但是，zookeeper在分布式项目中应用较多。
zookeeper 	

    Zookeeper是什么
    搭建zookeeper集群环境
    如何使用命令行操作zookeeper
    如何使用java操作zookeeper

第六阶段：HBase课程

hbase是个好东西，在以后工作中会经常遇到，特别是电信、银行、保险等行业。本节讲解hbase的伪分布和集群的安装，讲解基本理论和各种操作。我们通过对hbase原理的讲解，让大家明白为什么hbase会这么适合大数据的实时查询。最后讲解hbase如何设计表结构，这是hbase优化的重点。
HBase 	

    hbase的概述
    hbase的数据模型
    hbase的表设计
    hbase的伪分布式和集群安装
    hbase的shell操作
    hbase的JavaAPI操作
    hbase的数据迁移
    hbase的数据备份及恢复
    Hbase结合Hive使用
    hbase的集群管理
    hbase的性能调优

第七阶段：CM+CDH集群管理课程

由cloudera公司开发的集群web管理工具cloudera manager(简称CM)和CDH目前在企业中使用的比重很大，掌握CM+CDH集群管理和使用，不仅简化了集群安装、配置、调优等工作，而且对任务监控、集群预警、快速定位问题都有很大的帮助。
CM+CDH集群管理 	

    CM + CDH集群的安装
    基于CM主机及各种服务组件的管理
    CDH集群的配置和参数调优
    CDH集群HA配置及集群升级
    CM的监控管理
    集群管理的注意事项

第八阶段：Hive课程

在《hadoop1零基础拿高薪》课程中我们涉及了Hive框架内容，不过内容偏少，作为入门讲解可以，但是在工作中还会遇到很多课程中没有的。本课程的目的就是把Hive框架的边边角角都涉猎到，重点讲解Hive的数据库管理、数据表管理、表连接、查询优化、如何设计Hive表结构。这都是工作中最急需的内容，是工作中的重点。
Hive的概述、安装
与基本操作 	

    大家习惯把Hive称为hadoop领域的数据仓库。Hive使用起来非常像MySQL，但是比使用MySQL更有意思。
    我们在这里要讲述Hive的体系结构、如何安装Hive。还会讲述Hive的基本操作，目的是为了下面的继续学习。
    (理论所占比重★★★ 实战所占比重★★)

Hive支持的数据类型 	

    Hive的支持的数据类型逐渐增多。其中复合数据类型，可以把关系数据库中的一对多关系挪到Hive的一张表中，
    这是一个很神奇的事情，颠覆了我们之前的数据库设计范式。我们会讲解如何使用这种数据类型，如何把关系数
    据库的表迁移到Hive表。
    (理论所占比重★★ 实战所占比重★★★)

Hive数据的管理 	

    我们总拿Hive与MySQL做类比。其中，Hive对数据的操作方法是与MySQL最大的不同。我们会学习如何导入数
    据、导出数据，会学习如何分区导入、如何增量导入，会学习导入过程中如何优化操作等内容。这部分内容是工
    作中使用频率最高的内容之一。
    (理论所占比重★ 实战所占比重★★★★)

Hive的查询 	

    这部分内容讲解Hive查询语句的基本结构，重点讲解表连接。其中，有一些我们原来不知道的语法如left semi-
    join、sort by、cluster by等。这部分也在工作中用的是最多的内容之一。
    (理论所占比重★★ 实战所占比重★★★)

Hive的函数 	

    Hive是对查询语法的扩充，Hive运行我们非常方便的使用java来编写函数，特别方便。我们除了简单介绍常见的
    单行函数、聚合函数、表函数之外，还会介绍如何自定义函数。这样，我们就可以扩充原有函数库，实现自己的
    业务逻辑。这是体系我们能力的好地方！
    (理论所占比重★★★ 实战所占比重★★)

Hive的文件格式 	

    Hive的存储除了普通文件格式，也包括序列化文件格式和列式存储格式。讲解分别如何使用他们，已经何种场景
    下使用他们。最后讲解如何自定义数据存储格式。
    (理论所占比重★★★ 实战所占比重★★)

Hive的性能调优 	

    终于来到性能调优部分。我们会讲解本地模式、严格模式、并行执行、join优化等内容。通过实验对比发现优化
    手段的价值所在。这是整个课程的精华，也是我们以后工作能力的最重要的体现。
    (理论所占比重★ 实战所占比重★★★★)

项目实战 	

    我们会通过一个电信项目来把前面的内容综合运用起来。这是一个来自于真实工作环境的项目，学习如何使用各
    个知识点满足项目要求。并有真实数据提供给大家，供大家课下自己练习。
    (理论所占比重★ 实战所占比重★★★★)

杂记 	

    包括一些琐碎知识点，比如视图、索引、与HBase整合等。这些不好归入前面的某个章节，单独列出。并且根据
    学员就业面试情况，也不会不断增补内容。
    (理论所占比重★★★ 实战所占比重★★)

第九阶段：Sqoop课程

sqoop适用于在关系数据库与hdfs之间进行双向数据转换的，在企业中，非常常用。
Sqoop 	

    Sqoop是什么
    实战：讲解Sqoop如何把mysql中的数据导入到hdfs中
    实战：讲解Sqoop如何把hdfs中的数据导出到mysql中
    Sqoop如何做成job，方便以后快速执行

第十阶段：Flume课程

Flume是cloudera公布的分布式日志收集系统，是用来把各个的服务器中数据收集，统一提交到hdfs或者其他目的地，是hadoop存储数据的来源，企业中非常流行。
Flume 	

    Flume是什么
    详细Flume的体系结构
    讲述如何书写flume的agent配置信息
    实战：flume如何动态监控文件夹中文件变化
    实战：flume如何把数据导入到hdfs中
    实战：讲解如何通过flume动态监控日志文件变化，然后导入到hdfs中

第十一阶段：Kafka课程

Kafka是消息系统，类似于ActiveMQ、RabbitMQ,但是效率更高。
Kafka 	

    kafka是什么
    kafka体系结构
    kafka的安装
    kafka的存储策略
    kafka的发布与订阅
    使用Zookeeper协调管理
    实战：Kafka和Storm的综合应用

第十二阶段：Storm课程

Storm是专门用于解决实时计算的，与hadoop框架搭配使用。本课程讲解Storm的基础结构、理论体系，如何部署Storm集群，如何进行本地开发和分布式开发。通过本课程，大家可以进入到Storm殿堂，看各种Storm技术文章不再难，进行Storm开发也不再畏惧。
Storm 	

    Storm是什么，包括基本概念和应用领域
    Storm的体系结构、工作原理
    Storm的单机环境配置、集群环境配置
    Storm核心组件，包括Spout、Bolt、Stream Groupings等等
    Storm如何实现消息处理的安全性，保证消息处理无遗漏
    Storm的批处理事务处理
    实战：使用Storm完成单词计数等操作
    实战：计算网站的pv、uv等操作

第十三阶段：Redis课程

Redis是一款高性能的基于内存的键值数据库，在互联网公司中应用很广泛。
Redis 	

    redis特点、与其他数据库的比较
    如何安装redis
    如何使用命令行客户端
    redis的字符串类型
    redis的散列类型
    redis的列表类型
    redis的集合类型
    如何使用java访问redis
    redis的事务(transaction)
    redis的管道(pipeline)
    redis持久化(AOF+RDB)
    redis优化
    redis的主从复制
    redis的sentinel高可用
    redis3.x集群安装配置

第十四阶段：Scala课程

Scala是学习spark的必备基础语言，必须要掌握的。
Scala 	

    scala解释器、变量、常用数据类型等
    scala的条件表达式、输入输出、循环等控制结构
    scala的函数、默认参数、变长参数等
    scala的数组、变长数组、多维数组等
    scala的映射、元祖等操作
    scala的类，包括bean属性、辅助构造器、主构造器等
    scala的对象、单例对象、伴生对象、扩展类、apply方法等
    scala的包、引入、继承等概念
    scala的特质
    scala的操作符
    scala的高阶函数（这是重点，spark的原代码几乎全是高阶函数）
    scala的集合

第十五阶段：Spark课程

Spark是一款高性能的分布式计算框架，传言比MapReduce计算快100倍，本课程为你揭秘。
Spark 	

    Spark入门
    Spark与Hadoop的比较
    Spark环境搭建
    实战：使用Spark完成单词计数
    Spark缓存策略
    Spark的transformation和action
    Spark的容错机制
    Spark的核心组件
    Spark的各种RDD
    Spark的流计算

第十六阶段：Impala课程

Impala是Cloudera公司参照 Google Dreme系统进行设计并主导开发的新型查询系统，它提供复杂SQL语义，能查询存储在Hadoop的HDFS和HBase中的PB级大数据。
Impala 	

    Impala及其架构介绍
    Impala使用方法
    Impala配置及其调优
    Impala项目应用
    Impala和spark SQL对比

第十七阶段：Elastic Search课程
Elastic Search 	

    elasticsearch简介
    elasticsearch和solr的对比
    elasticsearch安装部署
    elasticsearch service wrapper启动插件
    使用curl操作elasticsearch索引库
    elasticsearch DSL查询
    elasticsearch批量查询meet
    elasticsearch批量操作bulk
    elasticsearch插件介绍
    elasticsearch配置文件详解
    java操作elasticsearch
    elasticsearch的分页查询
    elasticsearch中文分词工具的集成
    elasticsearch优化
    elasticsearch集群部署

第十八阶段：python课程

当前， 在大数据领域， Java成为了当仁不让的必修语言。原因就是大数据平台必备的Hadoop分布式管理平台需要使用Java。

Python是一个非常流行的编程语言， 无论在网络程序员中（比如Google的相当多的产品就是用Python编写的，Python也是豆瓣的主要开发语言），还是在科学计算领域， Python都有很广泛的应用。

大数据的各个框架几乎都有Python对应的接口，方便大家的使用。
python 	

    Python简介
    Python基本语法
    Python常用的数据结构
    Numpy类库的使用

第十九阶段：机器学习算法

了解机器学习是什么，掌握一些常见算法的技术原理和思想。
机器学习算法 	

    聚类-k-means算法
    协同过滤
    分类算法-kn
    贝叶斯算法

第二十阶段：Mahout课程

Mahout是数据挖掘和机器学习领域的利器，本课程是带领大家进入到这个领域中。课程内容包括Mahout体系结构介绍、Mahout如何在推荐、分类、聚类领域中使用。
Mahout 	

    Mahout是什么，有哪些应用场景
    Mahout机器学习环境的搭建和部署
    Mahout中支持哪些高大上的算法
    使用Mahout完成推荐引擎
    实战：实现基于web访问的推荐程序
    什么是聚类
    基于Mahout的常见聚类算法，如k-means算法
    实战：实现新闻内容的聚类

第二十一阶段：Docker课程

Docker 是一个开源的应用容器引擎，让开发者可以打包他们的应用以及依赖包到一个可移植的容器中，然后发布到任何流行的 Linux 机器上，也可以实现虚拟化。容器是完全使用沙箱机制，相互之间不会有任何接口（类似 iPhone 的 app）。几乎没有性能开销,可以很容易地在机器和数据中心中运行。最重要的是,他们不依赖于任何语言、框架包括系统。
Docker 	

    Docker简介
    Docker和普通虚拟机的区别
    Docker的三大组件
    Docker安装部署
    Docker的操作命令
    使用dockerfile构建镜像
    Docker高级特性
    Docker实战-构建web项目

第二十二阶段：实战项目

实战项目一：互联网爬虫

实战项目二：互联网数据的接入和清洗

实战项目三：互联网数据的实时计算

实战项目四：互联网数据的全文检索

实战项目五：互联网安全数据的统计与分析

实战项目六：互联网个性化推荐系统(含mahout)
