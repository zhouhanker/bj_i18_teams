package com.gjn.dws;

import com.gjn.constant.Constant;
import lombok.SneakyThrows;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.gjn.dws.DwsTrafficSourceKeywordPageViewWindow
 * @Author jingnan.guo
 * @Date 2025/4/14 11:43
 * @description: 1
 */
public class DwsTrafficSourceKeywordPageViewWindow {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        设置了检查点的超时时间为 60000 毫秒（即 60 秒）。如果在 60 秒内检查点操作没有完成，就会被视为失败。
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        当作业被取消时，检查点数据不会被删除，而是会保留下来，这样在后续需要时可以利用这些检查点数据进行恢复操作。
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        两次检查点操作之间的最小间隔时间为 2000 毫秒（即 2 秒）。这是为了避免在短时间内频繁进行检查点操作，从而影响作业的正常处理性能。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        表示在 30 天内允许的最大失败次数为 3 次。
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.days(30), org.apache.flink.api.common.time.Time.seconds(3)));
//        状态后端用于管理 Flink 作业的状态数据，HashMapStateBackend 会将状态数据存储在 TaskManager 的内存中，适用于小规
//
//
//
//        模的状态管理。

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 从页面日志事实表中读取数据 创建动态表  并指定Watermark的生成策略以及提取事件时间字段
        tenv.executeSql("create table page_log(\n" +
                "     common map<string,string>,\n" +
                "     page map<string,string>,\n" +
                "     ts bigint,\n" +
                "     et as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "     WATERMARK FOR et AS et\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_traffic_page',\n" +
                "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//        tenv.executeSql("select * from page_log").print();

        //TODO 过滤出搜索行为
        Table searchTable = tenv.sqlQuery("select \n" +
                "   page['item']  fullword,\n" +
                "   et\n" +
                "from page_log\n" +
                "where page['last_page_id'] = 'search' and page['item_type'] ='keyword' and page['item'] is not null");
//        searchTable.execute().print();
        tenv.createTemporaryView("search_table",searchTable);
        //TODO 调用自定义函数完成分词   并和原表的其它字段进行join
        Table splitTable = tenv.sqlQuery("SELECT keyword,et FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        tenv.createTemporaryView("split_table",splitTable);
//        tenv.executeSql("select * from split_table").print();
        //TODO 分组、开窗、聚合
        Table resTable = tenv.sqlQuery("SELECT \n" +
                "     date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "     date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "     date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "     keyword,\n" +
                "     count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second))\n" +
                "  GROUP BY window_start, window_end,keyword");
        resTable.execute().print();
        //TODO 将聚合的结果写到Doris中
        tenv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 2023-07-11 14:14:14
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                "  'username' = 'root'," +
                "  'password' = 'root', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");

    }
}
