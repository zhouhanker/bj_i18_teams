package com.ytx.realtime.dws.sourcekey.app;


import com.ytx.base.BaseSQLApp;
import com.ytx.constant.Constant;
import com.ytx.realtime.dws.sourcekey.function.KeywordUDTF;
import com.ytx.util.Sqlutil;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                "dws_traffic_source_keyword_page_view_window"

        );

    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        // 注册自定义函数到表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        // 从页面日志事实表中读取数据 创建动态表  并指定Watermark的生成策略以及提取事件时间字段
        tableEnv.executeSql("create table page_log(\n" +
                "     common map<string,string>,\n" +
                "     page map<string,string>,\n" +
                "     ts bigint,\n" +
                "     et as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "     WATERMARK FOR et AS et\n" +
                ")" + Sqlutil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"my_group"));
//        tableEnv.executeSql("select * from page_log").print();
        // 过滤出搜索行为
        Table searchTable = tableEnv.sqlQuery("select \n" +
                "   page['item']  fullword,\n" +
                "   et\n" +
                "from page_log\n" +
                "where page['last_page_id'] = 'search' and page['item_type'] ='keyword' and page['item'] is not null");
///        searchTable.execute().print();
        tableEnv.createTemporaryView("search_table",searchTable);
        // 调用自定义函数完成分词   并和原表的其它字段进行join
        Table splitTable = tableEnv.sqlQuery("SELECT keyword,et FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        tableEnv.createTemporaryView("split_table",splitTable);
    tableEnv.executeSql("select * from split_table").print();

        // 分组、开窗、聚合
        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "     date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "     date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "     keyword,\n" +
                "     count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second))\n" +
                "  GROUP BY window_start,keyword");
   resTable.execute().print();
        // 将聚合的结果写到Doris中
       tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 2023-07-11 14:14:14
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
//     resTable.executeInsert("dws_traffic_source_keyword_page_view_window");


    }
}
