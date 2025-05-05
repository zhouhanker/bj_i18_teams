package com.common.utils;

public class Constat {
    public static final String KAFKA_BROKERS = "cdh02:9092";

    public static final String TOPIC_DB = "xuanye_chang_Business";
    public static final String TOPIC_LOG = "xuanye_chang_log";

    public static final String MYSQL_HOST = "cdh03";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "Zh1028,./";
    public static final String HBASE_NAMESPACE = "xuanye_chang";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://10.160.60.17:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "xuanye_dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "xuanye_dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "xuanye_dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "xuanye_dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "xuanye_dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    public static final String DORIS_FE_NODES = "10.39.48.33:8030";

    public static final String DORIS_DATABASE = "realtime_v1_xuanye_chang";

}
