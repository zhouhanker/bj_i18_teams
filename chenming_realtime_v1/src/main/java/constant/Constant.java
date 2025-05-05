package constant;

/**
 * @Package com.cm.constant.Constant
 * @Author chen.ming
 * @Date 2025/5/2 14:30
 * @description: Constant
 */

public class Constant {
    public static final String KAFKA_BROKERS = "cdh01:9092,cdh02:9092,cdh03:9092";

    public static final String TOPIC_DB = "chenming_db";
    public static final String TOPIC_LOG = "chenming_log";

    public static final String MYSQL_HOST = "10.160.60.17";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "Zh1028,./";
    public static final String HBASE_NAMESPACE = "ns_chenming";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://10.160.60.17:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start_chenming";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err_chenming";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page_chenming";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action_chenming";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display_chenming";
    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info_chenming";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add_chenming";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail_chenming";
    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel_chenming";
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success_chenming";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund_chenming";
    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success_chenming";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register_chenming";


    public static final String DORIS_FE_NODES = "10.160.60.14:8030";

    public static final String DORIS_DATABASE = "dev_chen_ming_v1";
}
