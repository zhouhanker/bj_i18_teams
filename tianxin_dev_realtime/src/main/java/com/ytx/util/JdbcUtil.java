package com.ytx.util;




import com.google.common.base.CaseFormat;

import com.ytx.constant.Constant;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    public static Connection getMySQLConnection() throws Exception{
        Class.forName("com.mysql.cj.jdbc.Driver");
//               建立连接
        Connection conn= DriverManager.getConnection(Constant.MYSQL_URL,Constant.MYSQL_USER_NAME,Constant.MYSQL_PASSWORD);
        return conn;
    }
    public static void closeMySQLConnection(Connection conn) throws SQLException {
        if (conn!=null && !conn.isClosed()){
            conn.close();

        }
    }

    public static <T>List<T> queryList(Connection conn,String sql,Class<T> clz,boolean... isUnderlineToCamel) throws Exception {
        List<T> reList=new ArrayList<>();
        boolean defaultIsUToC=false;

        if (isUnderlineToCamel.length>0){
            defaultIsUToC=isUnderlineToCamel[0];
        }
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()){
            T obj = clz.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getCatalogName(i);
                Object columnValue = rs.getObject(i);
                if (defaultIsUToC){
                    columnName=CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                BeanUtils.setProperty(obj,columnName,columnValue);
            }
            reList.add(obj);
        }
        return reList;
    }
}
