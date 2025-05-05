package com.flink.realtime.dws.app;

import com.flink.realtime.dws.function.KeywordUDTF;
import com.struggle.flink.realtime.common.base.BaseSQLApp;
import com.struggle.flink.realtime.common.constant.Constant;
import com.struggle.flink.realtime.common.util.SQLUtil;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package flink.realtime.dws.app.DwsTrafficSourceKeywordPageViewWindow
 * @Author guo.jia.hui
 * @Date 2025/4/16 20:42
 * @description: 搜素关键词聚合统计
 * zk、kafka、flume、doris、
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                1
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        tableEnv.createTemporaryFunction("ik_analyze", KeywordUDTF.class);
        tableEnv.executeSql("create table page_log(\n" +
                "\tcommon map<string,string>,\n" +
                "\tpage map<string,string>,\n" +
                "\tts bigint,\n" +
                "\tet as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "\tWATERMARK FOR et AS et - INTERVAL '1' SECOND\n" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE, "dws_traffic_source_keyword_page_view_window"));
        //过滤出搜索行为
        Table searchTable = tableEnv.sqlQuery("select common['ch'] AS fullword,et from page_log");
        //注册临时表名称
        tableEnv.createTemporaryView("search_table", searchTable);
        Table splitTable = tableEnv.sqlQuery("SELECT keyword,et\n" +
                "FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        //splitTable.execute().print();
        tableEnv.createTemporaryView("split_table", splitTable);

        // 检查数据分配到哪个窗口
        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "     date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "     date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "     date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "     keyword,\n" +
                "     count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second))\n" +
                "  GROUP BY window_start, window_end,keyword");
        resTable.execute().print();

//        //将聚合的结果写到Doris中
//        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
//                "  stt string, " +  // 2023-07-11 14:14:14
//                "  edt string, " +
//                "  cur_date string, " +
//                "  keyword string, " +
//                "  keyword_count bigint " +
//                ")with(" +
//                " 'connector' = 'doris'," +
//                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
//                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
//                "  'username' = 'root'," +
//                "  'password' = '', " +
//                "  'sink.properties.format' = 'json', " +
//                "  'sink.buffer-count' = '4', " +
//                "  'sink.buffer-size' = '4086'," +
//                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
//                "  'sink.properties.read_json_by_line' = 'true' " +
//                ")");
//        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}