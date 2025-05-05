package com.jl.dws;



import com.jl.constant.Constant;
import com.jl.function.KeywordUDTF;
import com.jl.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.jl.DwsTrafficSourceKeywordPageViewWindow
 * @Author jia.le
 * @Date 2025/4/18 18:53
 * @description:
 */

public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(5000L);

        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        tableEnv.executeSql("create table page_log(\n" +
                "     common map<string,string>,\n" +
                "     page map<string,string>,\n" +
                "     ts TIMESTAMP(3) METADATA FROM 'timestamp', \n" +
                "     WATERMARK FOR ts AS ts - INTERVAL '3' SECOND \n" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));
//        tableEnv.executeSql("select * from page_log").print();

        Table searchTable = tableEnv.sqlQuery("select \n" +
                "   page['item']  fullword,\n" +
                "   ts \n" +
                " from page_log\n" +
                " where page['last_page_id'] = 'search' and page['item_type'] ='keyword' and page['item'] is not null");
        tableEnv.createTemporaryView("search_table",searchTable);
//        searchTable.execute().print();


        Table splitTable = tableEnv.sqlQuery("SELECT keyword,ts FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        tableEnv.createTemporaryView("split_table",splitTable);
//        tableEnv.executeSql("select * from split_table").print();

        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "  date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "  date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "  date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "  keyword,\n" +
                "  count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "  TUMBLE(TABLE split_table, DESCRIPTOR(ts), INTERVAL '10' second))\n" +
                "  GROUP BY window_start, window_end, keyword");
//        resTable.execute().print();

        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
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


        env.execute("DwsTrafficSourceKeywordPageViewWindow");
    }
}
