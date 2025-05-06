package com.realtime_dim.Utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import com.realtime_dim.Bean.Constat.constat;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package realtime_Dim.utils.JdbsUtils
 * @Author ayang
 * @Date 2025/4/10 14:01
 * @description: 通过JDBC操作MySQL数据库
 */
public class JdbsUtils<t> {
    public static Connection getMySQLConnection() throws Exception {
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        Connection conn = DriverManager.getConnection(constat.MYSQL_URL, constat.MYSQL_USER_NAME, constat.MYSQL_PASSWORD);
        return conn;
    }
    //关闭链接
    public static void closeMySQLConnection(Connection conn) throws SQLException {
        if(conn != null && !conn.isClosed()){
            conn.close();
        }
    }
    //   //从数据库表中查询数据
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz, boolean... isUnderlineToCamel) throws Exception {
        List<T> resList = new ArrayList<>();
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()){
            //通过反射创建一个对象，用于接收查询结果
            T obj = clz.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                //给对象的属性赋值
                if(defaultIsUToC){
                    columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                BeanUtils.setProperty(obj,columnName,columnValue);
            }
            resList.add(obj);
        }
        return  resList;
    }
}
