package com.lyx.stream.realtime.v2.app.dwd;

import com.lyx.stream.realtime.v2.app.constant.Constant;
import com.lyx.stream.realtime.v2.app.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.lyx.stream.realtime.v2.app.bwd.DwdTradeCartAdd
 * @Author yuxin_li
 * @Date 2025/4/11 20:49
 * @description: DwdTradeCartAdd
 * 将处理后的数据插入到目标 Kafka 表中，并执行整个 Flink 任务
 */

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        // 创建流执行环境
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为4，即使用4个并行任务处理数据
                env.setParallelism(4);
        // 创建流表环境
                StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 启用检查点机制，检查点间隔为5000毫秒，采用EXACTLY_ONCE语义保证数据处理的准确性
                env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 1. 创建Kafka源表
        // 创建一个名为topic_db的表，表结构包含after（键值对映射）、source（键值对映射）、op（操作类型）和ts_ms（时间戳）字段
        // SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO)用于生成Kafka相关的DDL配置
                tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                        "  after MAP<string, string>, \n" +
                        "  source MAP<string, string>, \n" +
                        "  `op` string, \n" +
                        "  ts_ms bigint " +
                        ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));
        // 可以用于测试，打印topic_db表中的所有数据
        // tableEnv.executeSql("select * from topic_db").print();


        // 2. 创建HBase维度表
        // 创建一个名为base_dic的表，表结构包含dic_code（字典代码）和info（包含dic_name字段的行）字段
        // SQLUtil.getHBaseDDL("dim_base_dic")用于生成HBase相关的DDL配置
                tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                        " dic_code string,\n" +
                        " info ROW<dic_name string>,\n" +
                        " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                        ") " + SQLUtil.getHBaseDDL("dim_base_dic")
                );
        // 可以用于测试，打印base_dic表中的所有数据
        // tableEnv.executeSql("select * from base_dic").print();

        // 3. 从Kafka表中查询数据并处理
        // 从topic_db表中查询数据，根据条件筛选出source['table'] = 'cart_info'且满足一定操作条件的数据
        // 提取相关字段，并对sku_num字段进行条件处理
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
        // 可以用于测试，打印cartInfo查询结果
        // cartInfo.execute().print();

        // 4. 创建Kafka目标表
        // 创建一个名为Constant.TOPIC_DWD_TRADE_CART_ADD的表，表结构包含id、user_id、sku_id、sku_num和ts_ms字段，并指定id为主键（不强制约束）
        // SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD)用于生成Kafka相关的DDL配置
                tableEnv.executeSql(" create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                        "    id string,\n" +
                        "    user_id string,\n" +
                        "    sku_id string,\n" +
                        "    sku_num string,\n" +
                        "    ts_ms bigint,\n" +
                        "    PRIMARY KEY (id) NOT ENFORCED\n" +
                        " )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));

        // 5. 将处理后的数据插入到Kafka目标表中
                cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);


                env.execute("dwd_kafka");
    }
}
