package com.hwq.dws.ikfenci.app;


import com.hwq.dws.ikfenci.function.KeywordUDTF;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author hu.wen.qi
 * @date 2024/5/4
 * 搜索关键词聚合统计
 * 需要启动的进程
 *      zk、kafka、flume、doris、DwdBaseLog、DwsTrafficSourceKeywordPageViewWindow
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(60000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
// 精确一次语义
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 检查点超时时间（2分钟）
        checkpointConfig.setCheckpointTimeout(120000);
// 最小间隔：500毫秒（防止检查点过于频繁）
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
// 最大并发检查点数
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        //TODO 注册自定义函数到表执行环境中
         tEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 从页面日志事实表中读取数据 创建动态表  并指定Watermark的生成策略以及提取事件时间字段
        tEnv.executeSql(" CREATE TABLE page_log (\n" +
                "              `common` Map<String,String>,\n" +
                "              `displays` Map<String,String>,\n" +
                "              `page` Map<String,String>,\n" +
                "              ts BIGINT,\n" +
                "              time_ltz AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "              WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND\n" +
                "            ) WITH (\n" +
                "              'connector' = 'kafka',\n" +
                "              'topic' = 'page_log',\n" +
                "              'properties.bootstrap.servers' = 'cdh03:9092',\n" +
                "              'properties.group.id' = 'testGroup',\n" +
                "              'scan.startup.mode' = 'earliest-offset',\n" +
                "              'format' = 'json'\n" +
                "            )");


       // tEnv.executeSql("select * from page_log").print();

        //TODO 过滤出搜索行为
        Table searchTable = tEnv.sqlQuery("select \n" +
                "   page['item']  fullword,\n" +
                "   time_ltz\n" +
                "from page_log\n" +
                "where page['last_page_id'] = 'search' and page['item_type'] ='keyword' and page['item'] is not null");
        //searchTable.execute().print();

        tEnv.createTemporaryView("search_table",searchTable);
        //TODO 调用自定义函数完成分词   并和原表的其它字段进行join
        Table splitTable = tEnv.sqlQuery("SELECT keyword,time_ltz FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        tEnv.createTemporaryView("split_table",splitTable);
        //tEnv.executeSql("select * from split_table").print();
        //TODO 分组、开窗、聚合
        Table resTable = tEnv.sqlQuery("SELECT \n" +
                "     date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "     date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "     date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "     keyword,\n" +
                "     count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(time_ltz), INTERVAL '10' second))\n" +
                "  GROUP BY window_start, window_end,keyword");
        resTable.execute().print();
        //TODO 将聚合的结果写到Doris中



//
//        tEnv.executeSql("create table dws_trafficVcChAr_isNew_pageViewWindow(" +
//                " stt string, " + // 2023-07-11 14:14:14
//                " edt string, " +
//                " cur_date string, " +
//                " keyword string, " +
//                " keyword_count bigint " +
//                ")with(" +
//                " 'connector' = 'doris'," +
//                " 'fenodes' = '" + "cdh03:8030" + "'," +
//                " 'table.identifier' = 'dws_to_doris.dws_trafficVcChAr_isNew_pageViewWindow'," +
//                " 'username' = 'root'," +
//                " 'password' = 'mysql', " +
//                " 'sink.properties.format' = 'json', " +
//                " 'sink.buffer-count' = '4', " +
//                " 'sink.buffer-size' = '4096'," +
//                " 'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
//                " 'sink.properties.read_json_by_line' = 'true' " +
//                ")");
//        resTable.executeInsert("dws_trafficVcChAr_isNew_pageViewWindow");






//        env.execute();
    }
}