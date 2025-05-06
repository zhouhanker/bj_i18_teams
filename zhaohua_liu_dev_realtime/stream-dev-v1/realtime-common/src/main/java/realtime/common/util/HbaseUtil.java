package realtime.common.util;

import org.apache.hadoop.hbase.NamespaceDescriptor;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import realtime.common.bean.TestBean;
import realtime.common.bean.base_category1;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @Package realtime.common.util.HbaseUtil
 * @Author zhaohua.liu
 * @Date 2025/4/10.14:38
 * @description: 操作Hbase的工具类
 */
public class HbaseUtil {

    //todo 获取hbase集群的连接
    public static Connection getHbaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","cdh01,cdh02,cdh03");
        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        return hbaseConn;
    }

    //todo 关闭hbase连接
    public static void closeHbaseConnection(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()){
            hbaseConn.close();
        }
    }


    //todo 获取异步操作Hbase的连接对象
    public static AsyncConnection getHbaseAsyncConnection(){
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","cdh01,cdh02,cdh03");
//        get() 方法会让程序暂停，直到异步任务完成并返回结果
        try {
            AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(conf).get();
            return asyncConnection;
        } catch (Exception e) {
            throw new RuntimeException("创建 HBase 异步连接失败", e);
        }
    }


    //todo 关闭异步操作hbase的连接对象
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConnection){
        if (asyncConnection != null){
            try {
                asyncConnection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

//    //todo 判断命名空间是否存
//    public static boolean isExist(Connection HbaseConn,String nameSpace) {
//        try(Admin admin = HbaseConn.getAdmin()) {
//            NamespaceDescriptor isExist = admin.getNamespaceDescriptor(nameSpace);
//            return isExist!=null;
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    //todo 判断表是否存在
//    public static boolean isExist(Connection hbaseConn,String nameSpace,String tableName){
//        try(Admin admin = hbaseConn.getAdmin()) {
//            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
//            return admin.tableExists(tableNameObj);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

    //todo 建命名空间
    public static void createHbaseNameSpace(Connection HbaseConn,String nameSpace){
        try(Admin admin = HbaseConn.getAdmin()) {
//            //判断命名空间是否存在
//            if (isExist(HbaseConn,nameSpace)){
//                System.out.println("命名空间" + nameSpace + "已存在");
//                return;
//            }
            //创建命名空间
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
            admin.createNamespace(namespaceDescriptor);
            System.out.println("命名空间" + nameSpace + "创建成功");
        } catch (IOException e) {
            System.out.println("命名空间" + nameSpace + "创建失败");
            throw new RuntimeException(e);
        }
    }




    //todo 列出表
    public static void showHbaseTable(Connection HbaseConn,String nameSpace){
//        if (!isExist(HbaseConn,nameSpace)){
//            System.out.println("命名空间"+nameSpace+"不存在");
//            return;
//        }
        try(Admin admin = HbaseConn.getAdmin();) {
            List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
            for (TableDescriptor tableDescriptor : tableDescriptors) {
                System.out.println("表名:" + tableDescriptor.getTableName());
                System.out.print("列族:");
                for (ColumnFamilyDescriptor columnFamily : tableDescriptor.getColumnFamilies()) {
                    System.out.println(columnFamily+"\t");
                }

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    //todo 建表
    public static void createHbaseTable(Connection HbaseConn,String namespace,String tableName,String... families){
//        if (!isExist(HbaseConn,namespace)){
//            System.out.println("命名空间" + namespace + "不存在");
//            return;
//        }
//        if (isExist(HbaseConn,namespace,tableName)){
//            System.out.println("表" + tableName + "已存在");
//            return;
//        }
        if (families.length < 1) {
            System.out.println("至少需要一个列族");
            return;
        }
//        try-with-resources 的主要作用是简化资源管理，避免资源泄漏，让代码更加简洁、安全。
//        它会自动调用实现了 AutoCloseable 接口的资源的 close() 方法
        try(Admin admin = HbaseConn.getAdmin()) {
//            valueOf类型转换
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            //判断表是否存在
            if (admin.tableExists(tableNameObj)){
                System.out.println("命名空间" + namespace + "下的表" + tableName + "已存在");
                return;
            }
            // 创建一个 TableDescriptorBuilder 对象，用于构建表的描述符
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                //列族名 转换为字节数组,HBase 内部使用字节数组存储数据
//                构建器类会提供一系列的方法来设置对象的属性，不过这些属性设置操作仅仅是在收集配置信息，并不会立即创建目标对象
//                build() 方法的任务是整合这些配置信息，
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                // 将构建好的列族描述符添加到表描述符构建器中
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }
            // 调用 Admin 对象的 createTable 方法，根据构建好的表描述符创建表
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("表空间" + namespace + "下的表" + tableName + "创建成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }




    //todo 删除表
    public static void dropHbaseTable(Connection hbaseConn,String nameSpace,String tableName){
        try(Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
            if (!admin.tableExists(tableNameObj)){
                System.out.println("命名空间" + nameSpace + "下的表" + tableName + "不存在");
                return;
            }
            //表存在则删除
            //删除表时先禁用表再删除表是为了保证数据一致性、集群稳定性和操作安全。
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("删除命名空间" + nameSpace + "下的表" + tableName + "");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 向表中put数据
     * @param hbaseConn 连接对象
     * @param nameSpace 表空间
     * @param tableName 表名
     * @param rowKey rowkey
     * @param family 列族
     * @param jsonObj 要put的数据
     */
    //todo 插入数据
    public static void putRow(Connection hbaseConn, String nameSpace, String tableName, String rowKey,
                              String family, JSONObject jsonObj){
        TableName tableNameObj = TableName.valueOf(nameSpace, tableName);
        //通过 HBase 连接对象获取指定表的 Table 对象。
        try(Table table = hbaseConn.getTable(tableNameObj)) {
            //创建一个 Put 对象，用于封装要插入的行数据，
            Put put = new Put(Bytes.toBytes(rowKey));
            //获取 JSON 对象中的所有键
            Set<String> columns = jsonObj.keySet();
            for (String column : columns) {
                String value = jsonObj.getString(column);
                if (StringUtils.isNoneEmpty(value)) {
                    put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向命名空间" + nameSpace + "下的表" + tableName + "中put数据" + rowKey + "成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    //todo 删除数据
    public static void delRow(Connection hbaseConn,String namespace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("从表空间" + namespace + "下的表" + tableName + "中删除数据"+rowKey+"成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    /**
     * 根据rowkey从Hbase表中查询一行数据
     * @param hbaseConn             hbase连接对象
     * @param namespace             表空间
     * @param tableName             表名
     * @param rowKey                rowkey
     * @param clz                   将查询的一行数据 封装的类型
     * @param isUnderlineToCamel    是否将下划线转换为驼峰命名
     * @return
     * @param <T>
     */

    // <T> 在方法名前定义了一个泛型参数 T，而返回值类型也是 T
    //lass<T> clz 中的 T 是一个类型参数，代表任意具体类型；Class<T> 则是代表这个类型的运行时对象；clz 是该对象的变量名
    //todo 读取一行数据
    public static <T>T getRow(Connection hbaseConn, String namespace, String tableName, String rowKey,Class<T> clz,boolean... isUnderlineToCamel){
        // 默认不执行表名的下划线转驼峰
        boolean defaultIsUToC  = false;
        if (isUnderlineToCamel.length >= 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj);){
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
//            从 Result 对象中提取所有的单元格数据，并将这些单元格数据存储在一个 List<Cell> 集合中。
//            遍历获取每个单元格的详细信息，比如行键、列族、列限定符、时间戳和值等。
            List<Cell> cells = result.listCells();

            //封装查询结果到 Java 对象
            if (cells != null && cells.size() > 0 ) {
                //通过反射创建该类型的实例
                T obj = clz.newInstance();
                for (Cell cell : cells) {
                    //获取单元格的列名。转为字符串
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    //获取单元格的列值 转为字符串
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if (defaultIsUToC) {
                        //如果 defaultIsUToC 为 true，则使用 Guava 的 CaseFormat 将列名从下划线格式转换为驼峰格式
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //将列名和对应的值设置到 obj 对象的属性中。
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                // 返回java对象
                return obj;
            }
        } catch (IOException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        //如果查询结果为空或发生异常，则返回 null。
        return null;
    }


    /**
     * 以异步的方式 从HBase维度表中查询维 度数据
     * @param asyncConn     异步操作HBase的连接
     * @param namespace     表空间
     * @param tableName     表名
     * @param rowKey        rowkey
     * @return
     */
    //todo 以异步的方式 从HBase维度表中查询维 度数据
    public static JSONObject readDimAsync(AsyncConnection asyncConn,String namespace, String tableName, String rowKey){
        try {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            AsyncTable<AdvancedScanResultConsumer> table = asyncConn.getTable(tableNameObj);
            Get get = new Get(Bytes.toBytes(rowKey));
            //第一个get用于发起异步查询请求，第二个用于等待异步操作完成并获取最终的查询结果
            Result result = table.get(get).get();
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                JSONObject jsonObj = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObj.put(columnName,columnValue);
                }
                return jsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException("异步读取数据失败", e);
        }
        return null;
    }


//    main 方法使得工具类可以独立运行，为了方便测试、快速验证功能
    public static void main(String[] args) throws IOException {


        String nameSpace = "e_commerce";
        String tableName = "testTableName";
        String[] FamilyList = {"info","grade"};
        String jsonStr = "{\"id\":1,\"name\":\"张三\"}";
        JSONObject jsonObj = JSONObject.parseObject(jsonStr);


        Connection hbaseConnection = getHbaseConnection();
        //测试创建命名空间
//        createHbaseNameSpace(hbaseConnection,nameSpace);

        //建表
//        createHbaseTable(hbaseConnection,nameSpace,tableName,FamilyList);

        //列出表
        showHbaseTable(hbaseConnection,nameSpace);
        showHbaseTable(hbaseConnection,"e_commerce");

        //插入数据
//        putRow(hbaseConnection,nameSpace,tableName,"1","info",jsonObj);

        //查询数据
//        System.out.println(getRow(hbaseConnection, nameSpace, tableName, "1", TestBean.class, false));
//        System.out.println(getRow(hbaseConnection, "e_commerce", "dim_base_category1", "1", base_category1.class, false));

        //异步读取
//        AsyncConnection hbaseAsyncConnection = getHbaseAsyncConnection();
//        System.out.println(readDimAsync(hbaseAsyncConnection, nameSpace, tableName, "1"));

        //删除数据
//        delRow(hbaseConnection,nameSpace,tableName,"1");
//        System.out.println(getRow(hbaseConnection, nameSpace, tableName, "1", TestBean.class, false));

        //删除表
//        dropHbaseTable(hbaseConnection,nameSpace,tableName);
//        showHbaseTable(hbaseConnection,nameSpace);


    }




}
