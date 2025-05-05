package dws;


import constant.Constant;
import dws.function.KeywordUDTF;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cm.dws.DwsTrafficSourceKeywordPageViewWindow
 * @Author chen.ming
 * @Date 2025/4/14 13:37
 * @description: 12.1流量域搜索关键词粒度页面浏览各窗口汇总表
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    @SneakyThrows
    public static void main(String[] args) {
        //TODO 注册自定义函数到表执行环境中
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

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


        tenv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 从页面日志事实表中读取数据 创建动态表并指定watermark的生成策略以及提取事件时间字段
        tenv.executeSql("create table page_log(" +
                " page map<string, string>, " +
                " ts bigint, " +
                " et as to_timestamp_ltz(ts, 3), " +
                " watermark for et as et - interval '5' second " +
                ")WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dwd_xinyi_jiao_page',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        Table table = tenv.sqlQuery("select * from page_log");
//        tenv.toChangelogStream(table).print();
        //TODO 过滤出搜索行为
        Table searchTable = tenv.sqlQuery("select \n" +
                "   page['item']  fullword,\n" +
                "   et\n" +
                "from page_log\n" +
                "where page['last_page_id'] = 'search' and page['item_type'] ='keyword' and page['item'] is not null");
        //searchTable.execute().print();
        tenv.createTemporaryView("search_table",searchTable);
//            tenv.toChangelogStream(searchTable).print();
        //TODO 调用自定义函数完成分词并和原表的其它字段进行join
        Table splitTable = tenv.sqlQuery("SELECT keyword,et FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        tenv.createTemporaryView("split_table",splitTable);
//        tenv.toChangelogStream(splitTable).print();
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
//        tenv.toChangelogStream(resTable).print();
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
                "  'username' = 'admin'," +
                "  'password' = 'zh1028,./', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");



        env.execute("DwsTrafficSourceKeywordPageViewWindow");
    }
}
