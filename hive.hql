2018-08-21

这部分关于 hive sql 的基本学习内容
hive 相关的报错 https://blog.csdn.net/jinjiating/article/details/18009503

建表
CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name 
  [(col_name data_type [COMMENT col_comment], ...)] 
  [COMMENT table_comment] 
  [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)] 
  [CLUSTERED BY (col_name, col_name, ...) 
  [SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS] 
  [ROW FORMAT row_format] 
  [STORED AS file_format] 
  [LOCATION hdfs_path]

hive不支持用insert语句一条一条的进行插入操作，也不支持update操作。
数据是以load的方式加载到建立好的表中。数据一旦导入就不可以修改。


向数据表内加载文件
•LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]
•Load 操作只是单纯的复制/移动操作，将数据文件移动到 Hive 表对应的位置。
•filepath
•相对路径，例如：project/data1
•绝对路径，例如： /user/hive/project/data1
•包含模式的完整 URI，例如：hdfs://namenode:9000/user/hive/project/data1
例如：
hive> LOAD DATA LOCAL INPATH './examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;



查询sql：
SELECT [ALL | DISTINCT] select_expr, select_expr, ...

FROM table_reference

[WHERE where_condition]

[GROUP BY col_list [HAVING condition]]

[ CLUSTER BY col_list

| [DISTRIBUTE BY col_list] [SORT BY| ORDER BY col_list]

]

[LIMIT number]

默认为ALL

例如：
SELECT COUNT(*)
FROM log.pay_rc_warden_event_basic
WHERE dt = "20180715" AND method = "sendevent" AND paytype > 31 AND result = 1


上述内容的总结和示例参见：
Hadoop Hive sql语法详解
https://blog.csdn.net/hguisu/article/details/7256833




窗口函数提取相关数值的优化： 不了解的字段可以查询 sql相关文档
-- 统计用户近七天内的每天提单数累计
SELECT user_id,
       ord_time,
       SUM(ord_num) over(partition by user_id ORDER BY ord_time) as ord_num_7
  FROM aggr_user_submitted_dd
 where dt>${begin_time_daykey}
   and dt<date_add(${begin_time_daykey},7 day)
   and ord_time BETWEEN ${begin_time_daykey} and date_add(${begin_time_daykey},7 day)


Hive分析窗口函数
1. CUME_DIST 小于等于当前值的行数/分组内总行数
比如，统计小于等于当前薪水的人数，所占总人数的比例
CUME_DIST() OVER(ORDER BY sal) AS rn1,
CUME_DIST() OVER(PARTITION BY dept ORDER BY sal) AS rn2 

 CUME_DIST() OVER(partition by a.dt,b.city_id order by sum(coalesce(c.original_price,0))) as rn


2. PERCENT_RANK 分组内当前行的RANK值-1/分组内总行数-1, 暂时不了解应用场景


*** 在Hive 中，尽量使用group by代替distinct， 先尽量过滤没用的数据
比如使用 Multi-group by 优化：

FROM logs WHERE dt>'20171101'
insert overwrite table test1 select log.id
 group by log.id 
insert overwrite table test2 select log.name
 group by log.name


Hive Sql中的where支持和聚合函数一起使用
如：
WHERE dt='20171101'
 GROUP BY dt,
          city_id,
          wm_poi_id
HAVING SUM(original_price)>10000



•ORDER BY 全局排序，只有一个Reduce任务
•SORT BY 只在本机做排序



窗口内分片函数
需求：业务需要计算2017年11月1号每个城市订单量排名前百分之20的商家的订单总额。
实现：使用nitle

SELECT SUM(f.ori_amt)
  FROM (
        SELECT a.wm_poi_id,
               a.ori_amt,
               NTILE(5) over(partition by city_id order by ord_num desc) rn
          FROM (
                SELECT wm_poi_id,
                       city_id,
                       SUM(original_price) ori_amt,
                       COUNT(id) ord_num
                  FROM mart_waimai.fact_ord_arranged
                 WHERE dt='20171101'
                 GROUP BY wm_poi_id,
                          city_id
               ) a
       ) f
 WHERE rn=1


Hive常见的保存搜索结果方法，详见：
https://blog.csdn.net/zhuce1986/article/details/39586189



Hive sql排名函数
1. row_number的用途的非常广泛，排序最好用他，一般可以用来实现web程序的分页，他会为查询出来的每一行记录生成一个序号，依次排序且不会重复，
注意使用row_number函数时必须要用over子句选择对某一列进行排序才能生成序号。row_number用法实例:
select ROW_NUMBER() OVER(order by [SubTime] desc) as row_num,* from [Order]


2. rank函数用于返回结果集的分区内每行的排名， 行的排名是相关行之前的排名数加一
select RANK() OVER(order by [UserId]) as rank,* from [Order] 


3. dense_rank函数的功能与rank函数类似，dense_rank函数在生成序号时是连续的，
而rank函数生成的序号有可能不连续。dense_rank函数出现相同排名时，将不跳过相同排名号，
rank值紧接上一次的rank值
select DENSE_RANK() OVER(order by [UserId]) as den_rank,* from [Order]


4. ntile函数可以对序号进行分组处理，将有序分区中的行分发到指定数目的组中。 
各个组有编号，编号从一开始。 对于每一个行，ntile 将返回此行所属的组的编号
select NTILE(4) OVER(order by [SubTime] desc) as ntile,* from [Order]

上述内容详细参见：
https://www.cnblogs.com/52XF/p/4209211.html


over 用法：开窗函数
over (order by salary): 按照salary排序进行累计，order by是默认的开窗函数
over (partition by deptno)： 按照部门分区

窗口范围：
over (order by salary range between 5 preceding and 5 following): 窗口范围为当前行数据幅度减5加5后的范围内。
over (order by salary rows between 5 preceding and 5 following ): 窗口范围为房前行前后各移动5行

内置参数的使用 (此处使用到Hive Sql中)
SUM(ord_num) over(partition by user_id ORDER BY ord_time) -- 默认为从起点到当前行
SUM(ord_num) over(partition by user_id ORDER BY ord_time ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) -- 从当前行前推3行到当前行
SUM(ord_num) over(partition by user_id ORDER BY ord_time ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) --  从当前行前推3行到当前行后推1行
SUM(ord_num) over(partition by user_id ORDER BY ord_time ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING/FOLLOWING)  -- 从当前行到始点/终点行


 count(1) over (partition by geohash) as all_poi_num


hive 计算分位数
percentile(col, p)、percentile_approx(col, p)，p∈(0,1) 
其中percentile要求输入的字段必须是int类型的，而percentile_approx则是数值类似型的都可以 
其实percentile_approx还有一个参数B：percentile_approx(col, p，B)，参数B控制内存消耗的近似精度，
B越大，结果的准确度越高。默认为10,000。当col字段中的distinct值的个数小于B时，结果为准确的百分位数。 


格式化时间戳
from_unixtime(unix_timestamp, format)

如需要统计商家是否为首次签约
select if (from_unixtime(bj_cmp_first_time, 'yyyyMMdd')=dt, poi_id, null) as fst_agree_poi_cnt

cast (expression as date_type)
as 后面为要处理的数据要转换的数据类型，目标系统所提供的数据类型包括： bigint, sql_variant, 不能使用用户定义的数据类型
（bigint： SQL Server在整数值超过int数据类型支持的范围时，将使用bigint数据类型）

count(*) 和 count(1) 以及 count([列])
前两者都是：评估count() 中的表达式是否为null,如果未非null则计数，前者会进行全表扫描
count(a) a != null 且不是表的列名的时候，count(a)为该表的行数

Hive get_json_object 
在SQL中存放的json串，对某个元素判断来查询结果
get_json_object(string json_string, string path)

SQL test 定义
id              int                    自增id
content         string                 内容

其中的conten是个json串，内容如下
{
  "status": {
     "person": {
        "name": false
    }
  }
}

查询结果
select get_json_object(content,'$.status') from test limit 1;
OK
{"person":{"name":false}}
Time taken: 0.066 seconds, Fetched: 1 row(s)
select get_json_object(content,'$.status.person') from test limit 1;
OK
{"name":false}
Time taken: 0.081 seconds, Fetched: 1 row(s)
select get_json_object(content,'$.status.person.name') from test limit 1;
OK
false
Time taken: 0.077 seconds, Fetched: 1 row(s)


如果要判断name的值，这个值不是bool，而是string，则需要加上‘’

select id,content from test where get_json_object(content,'$.status.person.name')='false' limit 2;
OK
7	{.status":{"person":{"name":false}}}
31	{.status":{"person":{"name":false}}}
Time taken: 0.085 seco	nds, Fetched: 2 row(s)

(get_json_object(get_json_object(entry_detail, '$.attribute'), '$.ad') is null or 
get_json_object(get_json_object(entry_detail, '$.attribute'), '$.ad')= '') -- 排除铂金CPM



coalesce 函数 coalesce(expression, expression[,...])
功能： 返回列表中的第一个非空表达式


选取某一段时间内的数据   ?  能否可行  （商家渗透率wiki) , 或者下面的作用是什么
select concat($$begindatekey, '~', $$enddatekey) as dt_time

	上述时间内的通常用法为 where dt between $$begindatekey and $$enddatekey




date_sub(date, interval expr type): 从日期减去指定的时间间隔

关于hive 中其它相关的日期函数参见  https://blog.csdn.net/qq_33481114/article/details/78845224





hive：
1. hive不允许直接访问非group by字段，这些字段可以利用hive的collect_set函数收集，返回一个数组；

group by 比order by先执行，order by不会对group by 内部进行排序，如果group by后只有一条记录，那么order by 将无效。
要查出group by中最大的或最小的某一字段使用 max或min函数。

hive中 order by, sort by, distribute by, cluster by的区别见：
https://blog.csdn.net/lzm1340458776/article/details/43306115
Order By即常规的SQL order by，保证结果的全序。

1. Sort By与Order By的区别是，在有多个reducer时，Sort By只保证每个reducer的输出是有序的，不保证整体的输出有序。

2. Distribute By和Cluster By通常和Map/Reduce/Transform脚本一起用。

3. Distribute By类似指定MapReduce的partition keys, 但是不保证同个partition里的key有序。

4. Distribute By＋Sort By起的作用类似shuffle。注意这里的Sort By就不是使reducer的输出有序，而是输入有序

5. 当Distribute By和Sort By的key相同时，可以使用Cluster By，效果是一样的。Cluster By id只是Distribute By id Sort By id的简写

/****************************************************************************************/
/****************************************************************************************/
hive与sql的区别

1. hive不支持等值连接
eg.  sql中对两表内联
select * from dual a, dual b where a.key = b.key

到hive中为：
select * from dual a join dual b on a.key = b.key 


2. 分号字符
分号为SQL语句结束标记，但是在hive中需要修改转义
select concat(key, concat(';', key)) from dual
需要改为
select concat(key, concat('\073', key)) from dual 


3. is [not] null
SQL中null代表空值
hive中string类型的字段若是空（empty）字符串，长度为0，则对其进行is null 判断结果会为false

where dt between $$begindatekey and $$enddatekey
                and (entry_event_id='b_IWSaX' or entry_event_id='b_MdbaO')      																											'' ' '
                and (json_extract_scalar(json_extract_scalar(entry_detail, '$.attribute'),'$.ad') is null or json_extract_scalar(json_extract_scalar(entry_detail, '$.attribute'),'$.ad')= '')     -- 等于 ''的目的是什么？
                and poi_id=json_extract_scalar(json_extract_scalar(entry_detail, '$.attribute'),'$.poi_id') 												-- in database blank space is not null
          group by dt,  																																	-- in database blank string is null
                   poi_id    																																-- 但是在此部分中属于 hive : ''表示的是字段不为null且为空字符串， 此时用a is null 无法查询这种情况，必须通过 a='' 或者 length(a) = 0查询


4. hive不支持将数据插入现有的表或者分区中，仅支持覆盖重写整个表   ************
hive不支持 insert into , update , delete


/****************************************************************************************/
									mapreduce
/****************************************************************************************/
参加运行机制示例可参照 
mapreduce示例.jpg

运行机制，时间上可以分为
插入分片（input split), map阶段，combiner阶段，shuffle阶段和reduce阶段

输入分片（input split）存储的并非数据本身，而是一个分片长度和一个记录数据的位置的数组

Combiner是一个本地化的reduce操作，它是map运算的后续操作，主要是在map计算出中间文件前做一个简单的合并重复key值的操作

在一个SQL中没有执行对表的列字段进行任何处理并且在WHERE 条件中没有进行除分区以外的数据过滤时，也没有进行任务聚合操作时，Hive执行不启用Map/Reduce。
1）SQL中没有聚合类操作，如Group By/Distinct/Sum 等；2）没有排序类操作如Order BY；3）没有ReduceJoin操作。  只运行Map操作不进行Reduce操作


MapJoin系统对大表进行分块分配给多个Map task进行处理，小表一次性加一个载到每个Map task任务的内存一个Hash集合中。
然后在Map中通过JoinKey对大表的每行数据进行处理，并取出大表的JoinKey从Hash表中取对应数据。然后将结果输出,通过Shuffle过程后Reduce取到自己的进程中。

ReduceJoin是大表和小表进行分别Map task处理然后通过JoinKey聚合到Reduce中去，然后在Reduce中进行遍历处理两个表数据的关联关系。


Hive Sql 优化：

1. 数据倾斜
数据倾斜参数设置：
set hive.optimize.skewjoin = true; -- 对于存在全局处理函数SQL不适用
set hive.map.aggr=true;  -- 对于求平均值、方差等不适用
set hive.groupby.skewindata=true; -- 对于存在全局处理函数SQL不适用

2. 小文件的输入输出（要避免小文件在数据仓库中的数量，其对集群的性能，表的使用影响很大）
2.1 小文件合并输入
-- 每个Map最大输入大小，决定合并后的文件数
set mapred.max.split.size=256000000;
-- 一个节点上split的至少的大小 ，决定了多个data node上的文件是否需要合并
set mapred.min.split.size.per.node=100000000;
-- 一个交换机下split的至少的大小，决定了多个交换机上的文件是否需要合并
set mapred.min.split.size.per.rack=100000000;
-- 执行Map前进行小文件合并
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat; 

2.2 小文件的输出
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256*1000*1000
set hive.merge.smallfiles.avgsize=16000000

3. SQL复杂逻辑不相关的串行job较多
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;

4. reduce端运行慢
通过map端数据合并和中间结果压缩节约数据shuffle时间提前启动redeuce及减少部分计算压力。
（下列三选一）
set hive.map.aggr=true；						--		map数据合并
set hive.exec.compress.intermediate=true;	--		map输出中间结果压缩
set mapred.reduce.parallel.copies=20;		--		增加reduce从map拷备线程数

5. map端运行慢
如果想增加map个数，则设置mapred.map.tasks 为一个较大的值；如果想减小map个数，则设置mapred.min.split.size 为一个较大的值；
set mapred.map.taskes=300
set mapred.min.split.size=500000000

hive的参数运行机制可以参照 Hive执行机制图.jpg

hive支持嵌入mapreduce程序，来处理复杂的逻辑. 如 
FROM (
MAP doctext USING 'python wc_mapper.py' AS (word, cnt)   -- doctext： 输入
FROM docs
CLUSTER BY word
) a
REDUCE word, cnt USING 'python wc_reduce.py'；			-- word，cnt： map程序的输出


hive支持转换后的数据直接写入不同的表，还能写入分区，hdfs和本地目录

FROM t1
 
INSERT OVERWRITE TABLE t2
SELECT t3.c2, count(1)
FROM t3
WHERE t3.c1 <= 20
GROUP BY t3.c2
 
INSERT OVERWRITE DIRECTORY '/output_dir'
SELECT t3.c2, avg(t3.c1)
FROM t3
WHERE t3.c1 > 20 AND t3.c1 <= 30
GROUP BY t3.c2
 
INSERT OVERWRITE LOCAL DIRECTORY '/home/dir'
SELECT t3.c2, sum(t3.c1)
FROM t3
WHERE t3.c1 > 30
GROUP BY t3.c2