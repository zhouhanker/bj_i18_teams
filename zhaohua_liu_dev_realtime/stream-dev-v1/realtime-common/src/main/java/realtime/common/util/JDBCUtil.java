package realtime.common.util;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import realtime.common.bean.base_category1;
import realtime.common.constant.Constant;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package realtime.common.util.JDBCUtil
 * @Author zhaohua.liu
 * @Date 2025/4/11.10:51
 * @description: 通过JDBC操作mysql数据库
 */
public class JDBCUtil {

    //todo 获取mysql连接
    public static Connection getMySQLConnection(){
        //抛出异常（throws）：把异常抛给方法调用者
        //捕获并处理异常（try-catch）：在当前方法处理异常
        try {
            //注册驱动,在maven中找依赖
            Class.forName("com.mysql.cj.jdbc.Driver");
            //建立连接
            Connection mysqlConn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
            return mysqlConn;
        } catch (SQLException e) {
            throw new RuntimeException("获取mysql连接失败",e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("未找到驱动",e);
        }
    }

    //todo 关闭mysql连接
    public static void closeMySQLConnect(Connection conn){
        try {
            if (conn!=null && !conn.isClosed()){
                conn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("未能获取到连接状态",e);
        }
    }

    //todo 从数据库中查询数据
    public static <T>List<T> queryList(Connection mysqlConn,String sql,Class<T> clz,boolean... isUnderlineToCamel){
        List<T> resList = new ArrayList<>();
        //默认不执行下划线转驼峰
        boolean defaultIsUToC = false;

        if (isUnderlineToCamel.length > 0){
            defaultIsUToC = isUnderlineToCamel[0];
        }

        try {
            //预编译sql语句,避免了重复编译 SQL 语句的开销，提高了执行效率
            //使用占位符 ? 可以有效防止 SQL 注入攻击
            PreparedStatement ps = mysqlConn.prepareStatement(sql);
            // 执行查询
            ResultSet rs = ps.executeQuery();
            //获取结果集的元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            //逐行遍历 ResultSet 中的数据
            while (rs.next()){
                //通过反射创建一个对象，用于接收查询结果
                T obj = clz.newInstance();
                //遍历元数据
                for (int i = 1; i <= metaData.getColumnCount(); i++){
                    //获取列名
                    String columnName = metaData.getColumnName(i);
                    //根据索引值获取对应列的值
                    Object columnValue = rs.getObject(i);
                    //判断是否进行下划线驼峰转化
                    if (defaultIsUToC){
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    //设置实体类对象的值
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                //实体类加入列表
                resList.add(obj);
            }
        } catch (SQLException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        //返回实体类列表
        return resList;
    }


    //todo 测试工具类
    public static void main(String[] args) throws SQLException {
        Connection mySQLConnection = getMySQLConnection();
        List<base_category1> base_category3s = queryList(mySQLConnection, "select * from `e_commerce`.base_category3", base_category1.class);
        for (base_category1 base_category3 : base_category3s) {
            System.out.println(base_category3);
        }
        System.out.println(base_category3s.size());
        closeMySQLConnect(mySQLConnection);
    }
}
