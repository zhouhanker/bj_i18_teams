package com.common.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package com.stream.common.utils.JdbcUtils
 * @Author zhou.han
 * @Date 2024/12/20 08:51
 * @description: MySQL Utils
 */
public class JdbcUtils {

    public static Connection getMySQLConnection() throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        return DriverManager.getConnection(Constat.MYSQL_URL,Constat.MYSQL_USER_NAME, Constat.MYSQL_PASSWORD);
    }


    public static void closeMySQLConnection(Connection conn) throws SQLException {
        if(conn != null && !conn.isClosed()){
            conn.close();
        }
    }

    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz, boolean... isUnderlineToCamel) throws Exception {
        List<T> resList = new ArrayList<>();
        boolean defaultIsUToC = false;

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
