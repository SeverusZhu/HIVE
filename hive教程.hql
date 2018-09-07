hive教程

hive中的表分为内部表和外部表

内部表（MANAGED_TABLE),创建时默认为内部表，
DROP时候会删除HDFS上的数据，适用于hive中间表、结果表、一般不需要从外部（如本地文件，HDFS上）load数据的情况

外部表(EXTERNAL_TABLE)
DROP时候不会删除HDFS上的数据，适用于源表，需要定期将表中的数据映射到表中。



database 和 table 的创建参见下述教程
http://lxw1234.com/archives/2015/06/265.htm

hive中的视图：
· 只有逻辑视图，没有物化视图
· 视图只能查询，不能Load/Insert/Update/Delete数据；
· 视图在创建时候，只是保存了一份元数据，当查询视图的时候，才开始执行视图对应的那些子查询；

分区相关的介绍参见：
 https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-AddPartitions


hive的动态分区，需要设置相关的参数，详见
http://lxw1234.com/archives/2015/06/286.htm


向hive表中加载数据
http://lxw1234.com/archives/2015/06/290.htm
https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-Loadingfilesintotables


hive命令行
http://lxw1234.com/archives/2015/06/292.htm


hive查询中的 order by 和 sort by
ORDER BY  用于全局排序，就是对指定的所有排序键进行全局排序，使用ORDER BY的查询语句，最后会用一个Reduce Task来完成全局排序。
SORT BY   用于分区内排序，即每个Reduce任务内排序。


distribute by ：按照指定的字段或表达式对数据进行划分，输出到对应的 Reduce 或者文件中。
cluster by ：除了兼具 distribute by 的功能，还兼具sort by的排序功能。


对同一张表的union all 要比多重insert快的多，
原因是hive本身对这种union all做过优化，即只扫描一次源表；

而多重insert也只扫描一次，但应为要insert到多个分区，所以做了很多其他的事情，导致消耗的时间非常长；





NTILE(n)，用于将分组数据按照顺序切分成n片，返回当前切片值
NTILE 不支持 ROWS BETWEEN，比如 NTILE(2) OVER(PARTITION BY cookieid ORDER BY createtime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
如果切片不均匀，默认增加第一个切片的分布


ROW_NUMBER() –从1开始，按照顺序，生成分组内记录的序列
–比如，按照pv降序排列，生成分组内每天的pv名次
ROW_NUMBER() 的应用场景非常多，再比如，获取分组内排序第一的记录;获取一个session中的第一条refer等。


—RANK() 生成数据项在分组中的排名，排名相等会在名次中留下空位
—DENSE_RANK() 生成数据项在分组中的排名，排名相等会在名次中不会留下空位


–CUME_DIST 小于等于当前值的行数/分组内总行数
–比如，统计小于等于当前薪水的人数，所占总人数的比例

CUME_DIST() OVER(ORDER BY sal) AS rn1,
CUME_DIST() OVER(PARTITION BY dept ORDER BY sal) AS rn2 


LAG(col,n,DEFAULT) 用于统计窗口内往上第n行值
第一个参数为列名，第二个参数为往上第n行（可选，默认为1），第三个参数为默认值（当往上第n行为NULL时候，取默认值，如不指定，则为NULL）

LAG(createtime,1,'1970-01-01 00:00:00') OVER(PARTITION BY cookieid ORDER BY createtime) AS last_1_time,
LAG(createtime,2) OVER(PARTITION BY cookieid ORDER BY createtime) AS last_2_time 


与LAG相反
LEAD(col,n,DEFAULT) 用于统计窗口内往下第n行值
第一个参数为列名，第二个参数为往下第n行（可选，默认为1），第三个参数为默认值（当往下第n行为NULL时候，取默认值，如不指定，则为NULL）



FIRST_VALUE
取分组内排序后，截止到当前行，第一个值


LAST_VALUE
取分组内排序后，截止到当前行，最后一个值



GROUPING SETS
在一个GROUP BY查询中，根据不同的维度组合进行聚合，等价于将不同维度的GROUP BY结果集进行UNION ALL

SELECT 
month,
day,
COUNT(DISTINCT cookieid) AS uv,
GROUPING__ID 
FROM lxw1234 
GROUP BY month,day 
GROUPING SETS (month,day) 
ORDER BY GROUPING__ID;



CUBE
根据GROUP BY的维度的所有组合进行聚合。
SELECT 
month,
day,
COUNT(DISTINCT cookieid) AS uv,
GROUPING__ID 
FROM lxw1234 
GROUP BY month,day 
WITH CUBE 
ORDER BY GROUPING__ID;

hive中只要涉及到两个表的关联，需要先观察数据，看是否存在多对多的关联。


hive并行执行job 
http://superlxw1234.iteye.com/blog/1703713


Hive元数据表结构
http://lxw1234.com/archives/2015/07/378.htm

hive表和分区的统计信息
http://lxw1234.com/archives/2015/07/413.htm

