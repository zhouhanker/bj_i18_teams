package com.zsf.retail_v1.realtime.dws;


import com.zsf.retail_v1.realtime.constant.Constant;
import com.zsf.retail_v1.realtime.util.SQLUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * &#064;Package  com.zsf.retail.v1.realtime.dws.DwsTrafficSourceKeywordPageViewWindow
 * &#064;Author  zhao.shuai.fei  &#064;Date  2025/4/11 20:49
 * &#064;description:  搜索关键词聚合统计
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    @SneakyThrows
    public static void main(String[] args) {
        //流处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);
        // flink sql初始化
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


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
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 从页面日志事实表中读取数据 创建动态表  并指定Watermark的生成策略以及提取事件时间字段
        tableEnv.executeSql("create table page_log(\n" +
                "     common map<string,string>,\n" +
                "     page map<string,string>,\n" +
                "     ts bigint,\n" +
                "     et as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "     WATERMARK FOR et AS et - INTERVAL '3' SECOND\n" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));
        //tableEnv.executeSql("select * from page_log").print();

        //TODO 过滤出搜索行为
        Table searchTable = tableEnv.sqlQuery("select \n" +
                "   page['item']  fullword,\n" +
                "   et\n" +
                "from page_log\n" +
                "where page['last_page_id'] = 'search' and page['item_type'] ='keyword' and page['item'] is not null");
        //searchTable.execute().print();
        tableEnv.createTemporaryView("search_table",searchTable);
        //TODO 调用自定义函数完成分词   并和原表的其它字段进行join
        Table splitTable = tableEnv.sqlQuery("SELECT keyword,et FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        tableEnv.createTemporaryView("split_table",splitTable);
//        tableEnv.executeSql("select * from split_table").print();

        //TODO 分组、开窗、聚合 无数据删除任意：window_start，window_end 隐式分组替换显式分组
        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "     date_format(window_start, 'yyyy-MM-dd HH:mm:ss') as stt,\n" +
                "     date_format(window_start + INTERVAL '10' SECOND, 'yyyy-MM-dd HH:mm:ss')  as edt,\n" +
                "     date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "     keyword,\n" +
                "     count(*) as keyword_count\n" +
                "  FROM TABLE(\n" +
                "     TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '1' second))\n" +
                "  GROUP BY window_start,keyword");
//        resTable.execute().print();


        //TODO 将聚合的结果写到Doris中
        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                " stt string, " + // 2023-07-11 14:14:14
                " edt string, " +
                " cur_date string, " +
                " keyword string, " +
                " keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + "cdh03:8030" + "'," +
                " 'table.identifier' = '" + "sx_001" + ".dws_traffic_source_keyword_page_view_window'," +
                " 'username' = 'root'," +
                " 'password' = 'root', " +
                " 'sink.properties.format' = 'json', " +
                " 'sink.buffer-count' = '4', " +
                " 'sink.buffer-size' = '4096'," +
                " 'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                " 'sink.properties.read_json_by_line' = 'true' " +
                ")");
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");


//        env.execute("dws01");
    }
}
