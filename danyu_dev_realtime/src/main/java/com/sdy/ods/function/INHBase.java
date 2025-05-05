package com.sdy.ods.function;


import com.alibaba.fastjson.JSONObject;
import com.sdy.bean.HBaseUtil;
import com.sdy.bean.TableProcessDim;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;


/**
 * @Package com.sdy.retail.v1.realtime.dim.function.INHBase
 * @Author danyu-shi
 * @Date 2025/4/30 8:46
 * @description:
 */
public class INHBase extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbaseConn;
    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hbaseConn);
    }

    //将流中数据写入HBase中
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObj = value.f0;
        TableProcessDim tableProcessDim = value.f1;
        String op = jsonObj.getString("op");
        jsonObj.remove("op");

        //获取操作的HBase的表名
        String sinkTable = tableProcessDim.getSinkTable();
        //获取row_key
        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());

        //判断对业务数据维度表进行来了什么操作
//        if ("d".equals(op)){
//            //从业务数据库维度表中做了删除操作  需要将HBase维度表中对应的记录也删除掉
//            HBaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
//        }else {
//            //如果不是delete,可能的存在
//            String sinkFamily = tableProcessDim.getSinkFamily();
//            HBaseUtil.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,jsonObj);
//        }

    }
}
