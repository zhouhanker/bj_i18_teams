package com.bg.realtime_dws.App;


import com.bg.common.base.BaseSQLApp;
import com.bg.common.constant.Constant;
import com.bg.common.util.SQLUtil;
import com.bg.realtime_dws.function.KeywordUDTF;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @Package com.bg.realtime_dws.App.DwsTrafficSourceKeywordPageViewWindow
 * @Author Chen.Run.ze
 * @Date 2025/4/14 11:29
 * @description: 搜索关键词聚合统计
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {

    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021, 1, "dws_traffic_source_keyword_page_view_window");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv) {
        //TODO 注册自定义函数到表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //TODO 从页面日志事实表中读取数据 创建动态表  并指定Watermark的生成策略以及提取事件时间字段
        tableEnv.executeSql("create table page_log(\n" +
                "     common map<string,string>,\n" +
                "     page map<string,string>,\n" +
                "     ts bigint,\n" +
                "     et as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "     WATERMARK FOR et AS et\n" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));

        //+----+--------------------------------+--------------------------------+----------------------+-------------------------+
        //| op |                         common |                           page |                   ts |                      et |
        //+----+--------------------------------+--------------------------------+----------------------+-------------------------+
        //| +I | {sid=2d062ec0-fc6b-41f2-9b9... | {item_type=sku_id, item=18,... |        1744771062000 | 2025-04-16 10:37:42.000 |
        //| +I | {sid=2c00ceb2-2a52-433f-b42... | {item_type=sku_id, item=26,... |        1744771062000 | 2025-04-16 10:37:42.000 |
        //| +I | {sid=158a90bd-6444-4a58-96a... | {during_time=10892, page_id... |        1744771062000 | 2025-04-16 10:37:42.000 |
//        tableEnv.executeSql("select * from page_log").print();

        //TODO 过滤出搜索行为
        Table searchTable = tableEnv.sqlQuery("select \n" +
                "   page['item']  fullword,\n" +
                "   et\n" +
                "from page_log\n" +
                "where page['last_page_id'] = 'search' and page['item_type'] ='keyword' and page['item'] is not null");

        //+----+--------------------------------+-------------------------+
        //| op |                       fullword |                      et |
        //+----+--------------------------------+-------------------------+
        //| +I |                           衬衫 | 2025-04-16 10:37:43.000 |
        //| +I |                         轻薄本 | 2025-04-16 10:37:47.000 |
        //| +I |                           小米 | 2025-04-16 10:37:46.000 |
//        searchTable.execute().print();
        tableEnv.createTemporaryView("search_table",searchTable);
        //TODO 调用自定义函数完成分词   并和原表的其它字段进行join
        Table splitTable = tableEnv.sqlQuery("SELECT keyword,et FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        tableEnv.createTemporaryView("split_table",splitTable);

        //+----+--------------------------------+-------------------------+
        //| op |                        keyword |                      et |
        //+----+--------------------------------+-------------------------+
        //| +I |                           衬衫 | 2025-04-16 10:37:43.000 |
        //| +I |                           小米 | 2025-04-16 10:37:46.000 |
        //| +I |                           轻薄 | 2025-04-16 10:37:47.000 |
//        tableEnv.executeSql("select * from split_table").print();
        //TODO 分组、开窗、聚合
        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "     date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "     date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "     date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "     keyword,\n" +
                "     count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '1' second))\n" +
                "  GROUP BY window_start, window_end,keyword");

        //+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+
        //| op |                            stt |                            edt |                       cur_date |                        keyword |        keyword_count |
        //+----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+
        //| +I |            2025-04-16 10:37:43 |            2025-04-16 10:37:44 |                     2025-04-16 |                           衬衫 |                    2 |
        //| +I |            2025-04-16 10:37:43 |            2025-04-16 10:37:44 |                     2025-04-16 |                             抽 |                    2 |
        //| +I |            2025-04-16 10:37:43 |            2025-04-16 10:37:44 |                     2025-04-16 |                           小米 |                    1 |
//        resTable.execute().print();
        //TODO 将聚合的结果写到Doris中
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

        //TODO 写入
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");

    }
}

