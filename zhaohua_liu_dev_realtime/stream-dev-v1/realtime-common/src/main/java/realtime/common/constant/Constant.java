package realtime.common.constant;

/**
 * @Package realtime.common.constant.constant
 * @Author zhaohua.liu
 * @Date 2025/4/9.14:29
 * @description: 数仓常量
 */
public class Constant {
    public static final String MYSQL_HOST = "cdh03";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "root";
    public static final String MYSQL_URL = "jdbc:mysql://cdh03:3306?useSSL=false";





    public static final String KAFKA_BROKERS = "cdh01:9092,cdh02:9092,cdh03:9092";




    public static final String TOPIC_ODS_INITIAL = "ods_initial";
    public static final String TOPIC_LOG = "topic_log";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAYS = "dwd_traffic_display";
    public static final String TOPIC_DWD_TRAFFIC_ACTIONS = "dwd_traffic_action";
    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL = "dwd_trade_order_cancel_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_PAY_SUC_DETAIL = "dwd_trade_order_pay_suc_detail";
    public static final String TOPIC_DWD_TRADE_REFUND_PAY_SUC_DETAIL = "dwd_trade_refund_pay_suc_detail";
    public static final String TOPIC_DWD_BASE_DB = "dwd_base_db";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    public static final String TOPIC_DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW = "dws_traffic_source_keyword_page_view_window";
    public static final String DORIS_FE_NODES = "cdh02:8034";
    public static final String DORIS_DATABASE = "realtime";

    public static final String HBASE_NAMESPACE = "e_commerce";




}
