package com.flink.realtime.common.util;

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

/**
 * @Package com.struggle.flink.realtime.common.util.HbaseUtil
 * @Author guo.jia.hui
 * @Date 2025/4/9 8:38
 * @description: hbase工具类
 */
public class HbaseUtil {
    //获取Hbase连接
    public static Connection getHbaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");
        return ConnectionFactory.createConnection(conf);
    }

    //关闭Hbase连接
    public static void closeHbaseConnection(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "cdh01,cdh02,cdh03");

        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null && !asyncConn.isClosed()) {
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
    public static void dropHBaseTable(Connection hbaseConn, String namespace, String tableName) throws IOException {
        Admin admin = hbaseConn.getAdmin();
        try {
            TableName tableNameNameObj = TableName.valueOf(namespace, tableName);
            //判断要删除的表是否存在
            if (!admin.tableExists(tableNameNameObj)) {
                System.out.println("要删除的表空间" + namespace + "下的表" + tableName + "不存在");
                return;
            }
            admin.disableTable(tableNameNameObj);
            admin.deleteTable(tableNameNameObj);
            System.out.println("删除的表空间" + namespace + "下的表" + tableName + "");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param hbaseConn 连接对象
     * @param namespace 表空间
     * @param tableName 表明
     * @param rowKey    rowkey
     * @param family    列表
     * @param jsonObj   要put的数据
     */
    //向表中put数据
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> keys = jsonObj.keySet();
            for (String column : keys) {
                String value = jsonObj.getString(column);
                if (StringUtils.isNotEmpty(value)) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间" + namespace + "下的表" + tableName + "中put数据" + rowKey + "成功");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //从表中删除数据
    public static void delRow(Connection hbaseConn, String namespace, String tableName, String rowKey) {
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("从表空间" + namespace + "下的表" + tableName + "中删除数据" + rowKey + "成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static JSONObject readDimAsync(AsyncConnection asyncConn, String namespace, String tableName, String rowKey) {
        try {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            AsyncTable<AdvancedScanResultConsumer> asyncTable = asyncConn.getTable(tableNameObj);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();
            if (cells != null && cells.size() > 0) {
                JSONObject jsonObj = new JSONObject();
                for (Cell cell : cells) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                    jsonObj.put(columnName, columnValue);
                }
                return jsonObj;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
