package realitime.dws.app;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import realitime.dws.function.KeywordUDTF;
import realtime.common.base.BaseSQLApp;
import realtime.common.constant.Constant;
import realtime.common.util.SQLutil;

/**
 * @Package realitime.dws.app.DwsTrafficSourceKeywordPageViewWindow
 * @Author zhaohua.liu
 * @Date 2025/4/20.19:51
 * @description: 热搜关键词,
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) throws Exception {
        new DwsTrafficSourceKeywordPageViewWindow().start(20012,4, Constant.TOPIC_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW);
    }
    @Override
    public void handle(StreamTableEnvironment tEnv) {
        //注册自定义函数到表执行环境中
        tEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);
        //建kafka映射表,读取页面日志
        tEnv.executeSql(
                "create table topic_log(\n" +
                        "    common map<string,string>,\n" +
                        "    page map<string,string>,\n" +
                        "    ts bigint,\n" +
                        "    et as to_timestamp_ltz(ts,3),\n" +
                        "    watermark for et as et\n" +
                        ")" + SQLutil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE, Constant.TOPIC_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));


        //注册动态查询结果
        Table searchResultTable = tEnv.sqlQuery(
                "select\n" +
                        "    `page`['item'] fullword,\n" +
                        "    et\n" +
                        "from topic_log\n" +
                        "where `page`['last_page_id']='search'\n" +
                        "and `page`['item_type']='keyword'\n" +
                        "and `page`['item'] is not null ;"
        );
        tEnv.createTemporaryView("search_table",searchResultTable);

        //Table Function调用自定义方法
        Table splitTable = tEnv.sqlQuery(
                "SELECT\n" +
                        "   keyword, et\n" +
                        "FROM search_table,\n" +
                        "LATERAL TABLE(ik_analyze(fullword)) t(keyword)"
        );
        tEnv.createTemporaryView("split_table",splitTable);

        //开窗分组计数
        Table resTable = tEnv.sqlQuery(
                "SELECT\n" +
                        "    date_format(window_start,'yyyy-MM-dd HH:mm:ss') stt,\n" +
                        "    date_format(window_start, 'yyyy-MM-dd') cur_date," +
                        "    keyword,\n" +
                        "    count(*) keyword_count\n" +
                        "FROM TABLE(\n" +
                        "             TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second ))\n" +
                        "GROUP BY window_start,keyword;"
        );
//      创建doris映射表
        tEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 2023-07-11 14:14:14
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                "  'username' = 'root'," +
                "  'password' = ''," +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        //写入doris
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");

    }
}
