package com.zpy.stream.realtime.v1.app.dwd;

import com.zpy.stream.realtime.v1.constant.Constant;
import com.zpy.stream.realtime.v1.utils.SQLUtil;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.zpy.stream.realtime.v1.app.bwd.DwdTradeCartAdd
 * @Author pengyu_zhu
 * @Date 2025/4/11 20:49
 * @description: 该类用于处理实时交易数据，将购物车添加信息写入目标Kafka主题
 * 1）读取业务数据
 * 2）过滤出加购行为
 * 3）将加购表数据写到kafka
 */

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        // 创建Flink流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置任务并行度为4
        env.setParallelism(4);

        // 创建Flink表执行环境，用于执行SQL查询
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 启用检查点机制，每5000毫秒进行一次检查点，确保精确一次处理语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 创建topic_db表，从Kafka主题读取数据
        // 表包含after（变更后数据映射）、source（数据源信息映射）、op（操作类型）和ts_ms（时间戳）字段
        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "  after MAP<string, string>, \n" +
                "  source MAP<string, string>, \n" +
                "  `op` string, \n" +
                "  ts_ms bigint " +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DB, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        // 创建base_dic表，从HBase读取数据
        // 表包含dic_code（字典代码）和info（包含dic_name的行类型）字段，dic_code设为主键
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name string>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") " + SQLUtil.getHBaseDDL("dim_base_dic")
        );

        // 从topic_db表筛选购物车信息，处理sku_num字段
        // 操作类型为'r'时取after中的sku_num，其他情况可能需计算差值（原代码逻辑有误）
        // 选取id、user_id、sku_id、sku_num和ts_ms字段存入cartInfo表
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

        // 创建TOPIC_DWD_TRADE_CART_ADD表，用于写入Kafka主题
        // 表包含id、user_id、sku_id、sku_num、ts_ms字段，id设为主键
        tableEnv.executeSql(" create table "+Constant.TOPIC_DWD_TRADE_CART_ADD+"(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts_ms bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));

        // 将cartInfo表数据插入到TOPIC_DWD_TRADE_CART_ADD对应的Kafka主题
        cartInfo.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);

        // 执行Flink作业，作业名为dwd_kafka
        env.execute("dwd_kafka");
    }
}