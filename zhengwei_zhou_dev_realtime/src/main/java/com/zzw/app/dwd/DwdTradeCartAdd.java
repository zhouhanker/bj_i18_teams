package com.zzw.app.dwd;

import com.zzw.constant.Constant;
import com.zzw.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.lzy.app.bwd.DwdTradeCartAdd
 * @Author zhengwei_zhou
 * @Date 2025/4/11 20:49
 * @description: DwdTradeCartAdd
 */

public class DwdTradeCartAdd {

    public static void main(String[] args) throws Exception {
        // 获取Flink的流式执行环境，这是Flink流处理程序的基础运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置该流处理程序的并行度为4，即同时运行4个并行任务来处理数据
        env.setParallelism(4);

        // 创建一个StreamTableEnvironment，用于在流处理环境中进行表操作和SQL查询
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 启用检查点机制，每隔5000毫秒（5秒）进行一次检查点操作
        // 使用EXACTLY_ONCE模式，确保在故障恢复时数据的一致性，即每条数据仅被处理一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 使用SQL语句在tableEnv中创建一个名为topic_db的表
        // 表结构包含：
        //   - after：是一个MAP类型，键和值都是字符串，可能用于存储一些详细信息
        //   - source：也是一个MAP类型，键和值都是字符串，可能存储数据来源相关信息
        //   - op：字符串类型，可能表示操作类型（如增删改查等）
        //   - ts_ms：长整型，可能表示时间戳（毫秒）
        // 同时通过SQLUtil工具类获取Kafka的DDL（数据定义语言）配置，
        // 配置从Constant.TOPIC_DB主题读取数据，并将处理后的数据写入Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO主题
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 以下代码被注释，若取消注释，会执行查询并打印topic_db表中的所有数据，用于调试
//        tableEnv.executeSql("select * from topic_db").print();

        // 使用SQL语句在tableEnv中创建一个名为base_dic的表
        // 表结构包含：
        //   - dic_code：字符串类型，可能是字典代码
        //   - info：是一个ROW类型，包含一个名为dic_name的字符串字段，可能用于存储字典名称
        //   - 声明dic_code为主键，但不强制约束
        // 通过SQLUtil工具类获取HBase的DDL配置，从HBase的"dim_base_dic"表读取数据
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );
        // 以下代码被注释，若取消注释，会执行查询并打印base_dic表中的所有数据，用于调试
//        tableEnv.executeSql("select * from base_dic").print();

        // 从topic_db表中查询符合条件的数据，并进行数据转换和处理
        // 具体操作包括：
        //   - 选取after字段中的'id'、'user_id'、'sku_id'，分别重命名为id、user_id、sku_id
        //   - 根据op字段的值进行条件判断：
        //     如果op等于'r'，则选取after字段中的'sku_num'作为sku_num的值；
        //     否则（这里的计算逻辑可能有误，正常情况下CAST((CAST(after['sku_num'] AS INT) - CAST(`after`['sku_num'] AS INT)) AS STRING)结果应为0），将计算结果转换为字符串作为sku_num的值
        //   - 选取ts_ms字段
        // 筛选条件为：source['table']等于'cart_info'，并且（op等于'r' 或者 （op等于'r' 且 after['sku_num']不为空且 CAST(after['sku_num'] AS INT) > CAST(after['sku_num'] AS INT)））
        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "   `after`['id'] id,\n" +
                "   `after`['user_id'] user_id,\n" +
                "   `after`['sku_id'] sku_id,\n" +
                "   if(op = 'r',`after`['sku_num'], CAST((CAST(after['sku_num'] AS INT) - CAST(`after`['sku_num'] AS INT)) AS STRING)) sku_num,\n" +
                "   ts_ms \n" +
                "   from topic_db \n" +
                "   where source['table'] = 'cart_info' \n" +
                "   and ( op = 'r' or \n" +
                "   ( op='r' and after['sku_num'] is not null and (CAST(after['sku_num'] AS INT) > CAST(after['sku_num'] AS INT))))"
        );
        // 以下代码被注释，若取消注释，会执行查询并打印cartInfo表中的数据，用于调试
//        cartInfo.execute().print();

        // 使用SQL语句在tableEnv中创建一个与Constant.TOPIC_DWD_TRADE_CART_ADD主题对应的表
        // 表结构包含：
        //   - id：字符串类型
        //   - user_id：字符串类型
        //   - sku_id：字符串类型
        //   - sku_num：字符串类型
        //   - ts_ms：长整型
        //   - 声明id为主键，但不强制约束
        // 通过SQLUtil工具类获取Kafka的Upsert DDL配置，用于将数据写入对应的Kafka主题
        tableEnv.executeSql(" create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts_ms bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));

        // 将前面查询得到的cartInfo表中的数据插入到与Constant.TOPIC_DWD_TRADE_CART_ADD主题对应的表中
        // 从而将数据写入对应的Kafka主题
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);

        // 执行整个Flink作业，作业名称为"dwd_kafka"
        // 启动流处理程序，开始处理数据并按照上述配置进行数据的读取、转换和写入操作
        env.execute("dwd_kafka");
    }


//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(4);
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//
//        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
//                "  after MAP<string, string>, \n" +
//                "  source MAP<string, string>, \n" +
//                "  `op` string, \n" +
//                "  ts_ms bigint " +
//                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
////        tableEnv.executeSql("select * from topic_db").print();
//
//
//        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
//                " dic_code string,\n" +
//                " info ROW<dic_name string>,\n" +
//                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
//                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
//        );
////        tableEnv.executeSql("select * from base_dic").print();
//
//        Table cartInfo = tableEnv.sqlQuery("select \n" +
//                "   `after`['id'] id,\n" +
//                "   `after`['user_id'] user_id,\n" +
//                "   `after`['sku_id'] sku_id,\n" +
//                "   if(op = 'r',`after`['sku_num'], CAST((CAST(after['sku_num'] AS INT) - CAST(`after`['sku_num'] AS INT)) AS STRING)) sku_num,\n" +
//                "   ts_ms \n" +
//                "   from topic_db \n" +
//                "   where source['table'] = 'cart_info' \n" +
//                "   and ( op = 'r' or \n" +
//                "   ( op='r' and after['sku_num'] is not null and (CAST(after['sku_num'] AS INT) > CAST(after['sku_num'] AS INT))))"
//        );
////        cartInfo.execute().print();
//
//        tableEnv.executeSql(" create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
//                "    id string,\n" +
//                "    user_id string,\n" +
//                "    sku_id string,\n" +
//                "    sku_num string,\n" +
//                "    ts_ms bigint,\n" +
//                "    PRIMARY KEY (id) NOT ENFORCED\n" +
//                " )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));
//
//        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
//
//        env.execute("dwd_kafka");
//    }
}
