package com.gjn.function;

import com.alibaba.fastjson.JSONObject;
import com.gjn.base.TableProcessDim;
import com.gjn.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package com.gjn.function.HbaseSinkFunciton
 * @Author jingnan.guo
 * @Date 2025/4/14 10:45
 * @description:将流中的数据同步到hbase表中
 */
public class HbaseSinkFunciton extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbaseConn;
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn= HBaseUtil.getHBaseConnection();

    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hbaseConn);
    }

    //将流中数据写到hbase
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) throws Exception {
        JSONObject jsonObj = tup.f0;
        TableProcessDim tableProcessDim = tup.f1;
        String type = jsonObj.getString("type");
        jsonObj.remove("type");

        //获取操作hbase的表明
        String sinkTable = tableProcessDim.getSinkTable();
        //获取rowkey
        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());
        //判断对我们业务数据库  维度表进行了什么操作
        if("delete".equals(type)){
            //从业务数据库维度表中做了删除操作      需要将hbase维度表中对应的记录删除掉
            HBaseUtil.delRow(hbaseConn,"ns_jingnan_guo",sinkTable,rowKey);
        }else{
            //如果不是delete，可能类型有insert、update、bootstrap-insert，上述操作对应的 都是向hbase表中put数据
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hbaseConn,"ns_jingnan_guo",sinkTable,rowKey,sinkFamily,jsonObj);
        }
    }
}
