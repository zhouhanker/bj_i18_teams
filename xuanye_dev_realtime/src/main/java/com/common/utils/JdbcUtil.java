package com.common.utils;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/*
通过JDBC操作MYsql数据库
 */
public class JdbcUtil {

    //获取Mysql连接
    public static Connection getMySQLConnection() throws Exception {
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        Connection conn = DriverManager.getConnection(Constat.MYSQL_URL, Constat.MYSQL_USER_NAME, Constat.MYSQL_PASSWORD);
        return conn;
    }

    //关闭Mysql连接
    public static void coloseMYSQLConnection(Connection conn) throws SQLException {
        if (conn!=null && !conn.isClosed()){
            conn.close();
        }
    }

    //从数据库表中查询数据
    public static <T>List<T> queryList(Connection conn,String sql,Class<T> clz,boolean... isUnderLineToCamel) throws Exception {
        ArrayList<T> resListt = new ArrayList<>();
        boolean defaultIsUToC = false;
        if (isUnderLineToCamel.length>0){
            defaultIsUToC = isUnderLineToCamel[0];
        }
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()){
            //通过反射创建一个对象，用于接受查询结果
            T obj= clz.newInstance();
            for (int  i = 1;i<=metaData.getColumnCount();i++){
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                //给对象的属性赋值
                if (defaultIsUToC){
                    columnName= CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                }
                BeanUtils.setProperty(obj,columnName,columnValue);
            }
            resListt.add(obj);
        }
        return resListt;
    }
}
