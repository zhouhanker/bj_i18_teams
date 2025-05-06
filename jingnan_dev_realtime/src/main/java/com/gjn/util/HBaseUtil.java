package com.gjn.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @Package com.gjn.util.HBaseUtil
 * @Author jingnan.guo
 * @Date 2025/4/9 15:30
 * @description:
 */
public class HBaseUtil {

    //获取hbase 连接方法

    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","cdh01,cdh02,cdh03");
//        conf.set("hbase.zookeeper.property.clientPort","2181");
        Connection hbaseConn = ConnectionFactory.createConnection(conf);
        return hbaseConn;
    }

    //关闭连接方法
    public static void closeHBaseConnection( Connection hbaseConn) throws IOException {
        if(hbaseConn!=null && !hbaseConn.isClosed()){
            hbaseConn.close();
        }
    }
    //建表
    public static void createHBaseTable(Connection hbaseConn,String namespace,String tableName,String ... families){

        if(families.length < 1){
            System.out.println("至少需要一个列族");

        }
        try (Admin admin = hbaseConn.getAdmin();){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)){
                System.out.println("要删除的表空间"+namespace+"下的表"+tableName+"已存在");
                return ;
            }
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family:families){
                ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(build);
            }

            admin.createTable(tableDescriptorBuilder.build());

            System.out.println("要删除的表空间"+namespace+"下的表"+tableName+"创建成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //删除表

    public static void dropHBaseTable(Connection hbaseConn,String namespace,String tableName){
        //判断删除表是否存在
        try (Admin admin = hbaseConn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if(admin.tableExists(tableNameObj)){
                System.out.println("要删除的表空间"+namespace+"下的表"+tableName+"不存在");
                return ;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
            System.out.println("要删除的表空间"+namespace+"下的表"+tableName+" ");

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //向表中put数据
    public static void putRow(Connection hbaseConn, String namespace, String tableName, String rowKey, String family, JSONObject jsonObj){

        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){
            Put put = new Put(Bytes.toBytes(rowKey));
            Set<String> columns = jsonObj.keySet();
            for(String column:columns){
                String value = jsonObj.getString(column);
                if(StringUtils.isNoneEmpty(value)){
                    put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
                }
            }
            table.put(put);
            System.out.println("向表空间"+namespace+"下的表"+tableName+"中put数据成功");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //从表中删除数据
    public static void delRow(Connection hbaseConn, String namespace, String tableName, String rowKey){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = hbaseConn.getTable(tableNameObj)){

            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            System.out.println("向表空间"+namespace+"下的表"+tableName+"中删除数据成功");
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
    }

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
}
