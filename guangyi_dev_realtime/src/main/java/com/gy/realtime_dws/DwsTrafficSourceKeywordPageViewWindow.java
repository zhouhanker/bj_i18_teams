package com.gy.realtime_dws;


import com.gy.Base.BasesqlApp;
import com.gy.constat.constat;
import com.gy.realtime_dws.function.KeywordUDTF;
import com.gy.utils.Sqlutil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package realtime_dws.function.DwsTrafficSourceKeywordPageViewWindow
 * @Author guangyi_zhou
 * @Date 2025/4/14 19:53
 * @description: 过滤搜索词
 */

public class DwsTrafficSourceKeywordPageViewWindow extends BasesqlApp
{
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10009,4,"dws_traffic_source_keyword_page_view_window");
    }
    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table page_log(\n" +
                "     common map<string,string>,\n" +
                "     page map<string,string>,\n" +
                "     ts bigint,\n" +
                "     et as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "     WATERMARK FOR et AS et\n" +
                ")" + Sqlutil.getKafkaDDL(constat.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));
//   tableEnv.sqlQuery("select * from page_log").execute().print();
        tableEnv.createTemporarySystemFunction("KeywordUDTF", KeywordUDTF.class);
        Table searchTable = tableEnv.sqlQuery("select \n" +
                "   page['item']  fullword,\n" +
                "   et\n" +
                "from page_log\n" +
                "where page['last_page_id'] = 'search' " +
                "and page['item_type'] ='keyword' and page['item'] is not null");
        //searchTable.execute().print();
        tableEnv.createTemporaryView("search_table",searchTable);
        Table t0 = tableEnv.sqlQuery(
                "SELECT keyword,et " +
                        "FROM search_table, LATERAL TABLE(KeywordUDTF(fullword)) t(keyword)");
        tableEnv.createTemporaryView("t0",t0);

//       tableEnv.sqlQuery("select * from t0").execute().print();
//        | +I |                           轻薄 | 2025-04-07 22:01:29.185 |


        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "     date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "     date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "     keyword,\n" +
                "     count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE t0, DESCRIPTOR(et), INTERVAL '1' seconds))\n" +
                "  GROUP BY window_start,keyword");

//        resTable.execute().print();



        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + constat.DORIS_FE_NODES + "'," +
                "  'table.identifier' = '" + constat.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                "  'username' = 'admin'," +
                "  'password' = 'admin', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '2', " +
                "  'sink.buffer-size' = '2048'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");
//    resTable.execute().print();

    }

}
