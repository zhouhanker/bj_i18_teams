package com.sdy.dws;


import com.sdy.bean.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
/**
 * @Package com.sdy.retail.v1.realtime.dws.dwsPage
 * @Author danyu-shi
 * @Date 2025/4/15 19:04
 * @description:
 */
public class dwsPage {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));

        DataStreamSource<String> kafkasource = KafkaUtil.getKafkaSource(env, "stream_dwdpage_danyushi", "dws_Page");
//        kafkasource.print();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporarySystemFunction("ik_analyze", KwSplit.class);

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "    `common` MAP<STRING, STRING>,\n" +
                "    `page` MAP<STRING, STRING>,\n" +
                "    `ts` bigint,\n" +
                " et as to_timestamp_ltz(ts, 3), " +
                " watermark for et as et - interval '5' second " +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'stream_dwdpage_danyushi',\n" +
                "    'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                "    'properties.group.id' = 'testGroup',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json'\n" +
                ")");

        Table Pagetable = tableEnv.sqlQuery("select * from topic_db");
//        Pagetable.execute().print();

// 2. 读取搜索关键词
        Table kwTable = tableEnv.sqlQuery("select " +
                "page['item'] kw, " +
                "et " +
                "from topic_db " +
                "where ( page['last_page_id'] ='search' " +
                "        or page['last_page_id'] ='home' " +
                "       )" +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");
        tableEnv.createTemporaryView("kw_table", kwTable);

//        kwTable.execute().print();

// 3. 自定义分词函数
        tableEnv.createTemporaryFunction("kw_split", KwSplit.class);

        Table keywordTable = tableEnv.sqlQuery("select " +
                " keyword, " +
                " et " +
                "from kw_table " +
                "join lateral table(kw_split(kw)) on true ");
        tableEnv.createTemporaryView("keyword_table", keywordTable);

//        keywordTable.execute().print();

// 4. 开窗聚和 tvf
        Table result = tableEnv.sqlQuery("select " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                " date_format(window_start, 'yyyyMMdd') cur_date, " +
                " keyword," +
                " count(*) keyword_count " +
                "from table( tumble(table keyword_table, descriptor(et), interval '5' second ) ) " +
                "group by window_start, window_end, keyword ");
        result.execute().print();


        // 5. 写出到 doris 中http://10.39.48.33:8030/
//        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
//                "  stt string, " +  // 2023-07-11 14:14:14
//                "  edt string, " +
//                "  cur_date string, " +
//                "  keyword string, " +
//                "  keyword_count bigint " +
//                ")with(" +
//                " 'connector' = 'doris'," +
//                " 'fenodes' = '10.39.48.33:8030'," +
//                "  'table.identifier' = 'dev_v1_danyu_shi.dws_traffic_source_keyword_page_view_window'," +
//                "  'username' = 'admin'," +
//                "  'password' = 'zh1028,./', " +
//                "  'sink.properties.format' = 'json', " +
//                "  'sink.buffer-count' = '4', " +
//                "  'sink.buffer-size' = '4086'," +
////                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
//                "  'sink.properties.read_json_by_line' = 'true' " +
//                ")");
//
//        result.executeInsert("dws_traffic_source_keyword_page_view_window");

//        env.execute();

    }
}
