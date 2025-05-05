package com.bg.common.util;

import com.alibaba.fastjson.JSONObject;
import com.bg.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;



/**
 * @Package com.bg.common.util.HBaseUtil
 * @Author Chen.Run.ze
 * @Date 2025/4/8 13:56
 * @description: 操作HBase工具类
 */
public class HBaseUtil {
    //获取Hbase连接
    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");

        return ConnectionFactory.createConnection(conf);
    }

    //关闭Hbase连接
    public static void closeHBaseConnection(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    //获取异步操作HBase的连接对象
    public static AsyncConnection getHBaseAsyncConnection(){
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");

        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    //关闭异步操作HBase的连接对象
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if(asyncConn != null && !asyncConn.isClosed()){
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //建表
    public static void createHBaseTable(Connection hbaseConn, String namespace, String tableName, String... families) {
        if (families.length < 1) {
            System.out.println("至少需要一个列族");
            return;
        }

        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)) {
                System.out.println("表空间" + namespace + "下的表" + tableName + "已存在");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            }

            admin.createTable(tableDescriptorBuilder.build());

            System.out.println("表空间" + namespace + "下的表" + tableName + "创建成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //删除表
    public static void dropHBaseTable(Connection hbaseConn, String namespace, String tableName) {

        try (Admin admin = hbaseConn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            //判断要删除的表是否存在
            if (!admin.tableExists(tableNameObj)) {
                System.out.println("要删除的表空间" + namespace + "下的表" + tableName + "不存在");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("删除的表空间" + namespace + "下的表" + tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 用于向 HBase 目标表写入数据
     * @param conn HBase 连接对象
     * @param nameSpace HBase 命名空间
     * @param table HBase 表名
     * @param rowKey 数据的 row_key
     * @param family 数据所属的列族
     * @param data 待写出的数据
     */
    public static void putRow(Connection conn, String nameSpace, String table, String rowKey, String family, JSONObject data) {
        // 1. 获取 table 对象
        TableName tableName = TableName.valueOf(nameSpace, table);
        try(Table t = conn.getTable(tableName)){
            // 2. 创建 put 对象
            Put put = new Put(Bytes.toBytes(rowKey));
            // 3. 把每列放入 put 对象中
            for (String key : data.keySet()) {
                String value = data.getString(key);
                if (value != null) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value));
                }
            }
            // 4. 向 table 对象中 put 数据
            t.put(put);
            t.close();
            System.out.println("从表空间" + nameSpace + "下的表" + tableName + "中put数据成功");
        }catch (IOException e){
            throw new RuntimeException(e);
        }




    }

    /**
     * 根据 row_key 删除 HBase 目标表指定数据
     * @param conn HBase 连接对象
     * @param namespace HBase 命名空间
     * @param table HBase 表名
     * @param rowKey HBase row_key
     */
    public static void delRow(Connection conn, String namespace, String table, String rowKey) {
        TableName tableName = TableName.valueOf(namespace, table);
        try(Table t = conn.getTable(tableName)){
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            // 删除整行
            t.delete(delete);
            t.close();
            System.out.println("从表空间" + namespace + "下的表" + tableName + "中删除数据成功");
        }catch (IOException e){
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
    public static <T>T getRow(Connection hbaseConn, String namespace, String tableName, String rowKey,Class<T> clz,boolean... isUnderlineToCamel){
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                //定义一个对象，用于封装查询出来的一行数据
                T obj = clz.newInstance();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    if(defaultIsUToC){
                        columnName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName);
                    }
                    BeanUtils.setProperty(obj,columnName,columnValue);
                }
                return obj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * 以异步的方式 从HBase维度表中查询维度数据
     * @param asyncConn     异步操作HBase的连接
     * @param namespace     表空间
     * @param tableName     表名
     * @param rowKey        rowkey
     * @return
     */
    public static JSONObject readDimAsync(AsyncConnection asyncConn,String namespace, String tableName, String rowKey){
        try {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConn.getTable(tableNameObj);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if(cells != null && cells.size() > 0){
                JSONObject jsonObj = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObj.put(columnName,columnValue);
                }
                return jsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        Connection hBaseConnection = getHBaseConnection();
        JSONObject jsonObj = getRow(hBaseConnection, Constant.HBASE_NAMESPACE, "dim_base_trademark", "1", JSONObject.class);
        System.out.println(jsonObj);
        closeHBaseConnection(hBaseConnection);
    }


}
