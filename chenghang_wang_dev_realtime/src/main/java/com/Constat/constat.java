package com.Constat;

/**
 * @Package realtime_dim.bean.constat.constat
 * @Author chenghang.wang
 * @Date 2025/4/11 9:54
 * @description: 电商实时数仓常量类
 */
public class constat {
    public static final String KAFKA_BROKERS = "cdh01:9092";

    public static final String TOPIC_DB = "realtime_v1_table_all_mysql";
    public static final String TOPIC_LOG = "realtime_log_v1";

    public static final String MYSQL_HOST = "cdh03";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "root";
    public static final String HBASE_NAMESPACE = "realtime_v1";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://cdh03:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_Dwd_Trade_Order_PaySuc_Detail = "dwd_Trade_Order_PaySuc_Detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel_detail";
    //    dwd_interaction_comment_info_v1
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    public static final String DORIS_FE_NODES = "cdh03:8110";
    public static final String TOPIC_dwd_base_db = "dwd_base_db";

    public static final String DORIS_DATABASE = "realtime_v1";
}
