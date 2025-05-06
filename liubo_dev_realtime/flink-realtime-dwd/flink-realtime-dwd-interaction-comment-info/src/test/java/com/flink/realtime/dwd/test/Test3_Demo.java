package com.flink.realtime.dwd.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @version 1.0
 * @Package com.flink.realtime.dwd.test.Test3_Demo
 * @Author liu.bo
 * @Date 2025/5/3 16:31
 * @description: 通过当前demo类模拟评论事实表实现过程
 */
public class Test3_Demo {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关的设置（略）
        //TODO 3.从kafka的first主题中读取员工数据并创建动态表
        tableEnv.executeSql("CREATE TABLE emp (\n" +
                "  empno string,\n" +
                "  ename string,\n" +
                "  deptno string," +
                "  proc_time as PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'first',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        tableEnv.executeSql("select * from emp").print();
        //TODO 4.从Hbase表中读取部门数据并创建动态表
        tableEnv.executeSql("CREATE TABLE dept (\n" +
                " deptno string,\n" +
                " info ROW<dname string>,\n" +
                " PRIMARY KEY (deptno) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 't_dept',\n" +
                " 'zookeeper.quorum' = 'cdh01:2181'\n" +
                ")");
//        tableEnv.executeSql("select * from dept").print();
        //TODO 5.关联员工和部门
        Table joinedTable = tableEnv.sqlQuery("SELECT e.empno,e.ename,d.deptno,d.dname\n" +
                "FROM emp AS e\n" +
                "  JOIN dept FOR SYSTEM_TIME AS OF e.proc_time AS d\n" +
                "    ON e.deptno = d.deptno");
        joinedTable.execute().print();
        //TODO 6.将关联的结果写道kafka主题
        //6.1 创建动态表和要写入的主题进行映射
        //6.2 写入
    }
}
