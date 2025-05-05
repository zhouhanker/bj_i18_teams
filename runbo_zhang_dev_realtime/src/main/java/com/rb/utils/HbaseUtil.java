package com.rb.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @Package com.rb.utils.HbaseUtil
 * @Author runbo.zhang
 * @Date 2025/4/9 8:46
 * @description:
 */
public class HbaseUtil {
    //todo 连接hbase  获取连接
    public static Connection getHbaseConnection() throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(conf);
        return connection;
    }
    /***
     * @author:
     * @description: 异步连接hbase
     * @params: []
     * @return: org.apache.hadoop.hbase.client.AsyncConnection
     * @date: 2025/4/22 16:52
     */
    public static AsyncConnection getHbaseAsyncCon(){
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
        try {

            AsyncConnection connection = ConnectionFactory.createAsyncConnection(conf).get();
            return connection;
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    /**
     * 关闭异步连接
     * @param asyncConnection
     */
    public static void closeHbaseAsyncCon(AsyncConnection asyncConnection){
        if (asyncConnection!=null && !asyncConnection.isClosed()){
            try {
                asyncConnection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            ;
        }

    }

    //todo 关闭连接
    public static void closeHbaseConnection(Connection connection) throws IOException {

        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    //todo 建表
    public static void createTable(Connection connection, String namespace, String tableName, String... families) {
        if (families.length < 1) {
            System.out.println("至少要有一个列族");
            return;
        }
        try (Admin admin = connection.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)) {
                System.out.println(namespace + "." + tableName + "表已经存在");
                return;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);

            for (String family : families) {//添加列族
                ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(build);
            }
            //建表
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //todo 删除表
    public static void dropTable(Connection connection, String namespace, String tableName) {
        try (Admin admin = connection.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (!admin.tableExists(tableNameObj)) {
                System.out.println("要删除的" + namespace + "." + tableName + "表不存在");
                return;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("要删除的" + namespace + "." + tableName + "表删除成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //todo put数据

    /**
     * @param connection 连接
     * @param namespace  库
     * @param tableName  表名
     * @param rowKey     列编号
     * @param family     列族
     * @param jsonData   数据
     */
    public static void putRow(Connection connection, String namespace, String tableName, String rowKey, String family, String jsonData) {
        //合成库名.表名
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try {
            //连接表
            Table table = connection.getTable(tableNameObj);
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(tableNameObj)){
                createTable(connection, namespace, tableName, family);
            }
            //生成put对象
            Put put = new Put(Bytes.toBytes(rowKey));
            JSONObject data = JSON.parseObject(jsonData);
            Set<String> keys = data.keySet();
            for (String key : keys) {
                String value = data.getString(key);
                if (StringUtils.isNotEmpty(value)) {
                    //value不为空就把数据添加进put
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("数据添加到" + namespace + "." + tableName + "表成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //todo 删除数据
    public static void delRow(Connection connection, String namespace, String tableName, String rowKey) {
        //合成库名.表名
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try(Table table = connection.getTable(tableNameObj)) {

            Delete delete = new Delete(Bytes.toBytes(rowKey));

            table.delete(delete);


            System.out.println("删除" + namespace + "." + tableName + "表的数据成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *  异步查询
     * @param asyncConnection 连接
     * @param namespace 库
     * @param table 表
     * @param rowKey rowKey
     * @return
     */
    public static JSONObject readDimAsync(AsyncConnection asyncConnection,String namespace ,String table , String rowKey){


        try {
            //获取{库名：表名}
            TableName tableName = TableName.valueOf(namespace, table);
            //获取表对象
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConnection.getTable(tableName);

            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();

            List<Cell> cells = result.listCells();

            if (cells!=null && cells.size()>0  ){
                JSONObject object = new JSONObject();
                for (Cell cell : cells) {

                    String name = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    object.put(name,value);

                }
                return object;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }



}
