package com.zpy.function;

import com.alibaba.fastjson.JSONObject;
import com.zpy.bean.TableProcessDim;
import com.zpy.constant.Constant;
import com.zpy.utils.HBaseUtil;
import com.zpy.utils.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

/**
 * @Package com.zpy.function.HBaseSinkFunction
 * @Author pengyu_zhu
 * @Date 2025/4/21 9:41
 * @description: HBaseSinkFunction
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbaseConn;
    private Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hbaseConn);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) {
        JSONObject jsonObj = tup.f0;
        TableProcessDim tableProcessDim = tup.f1;
        String type = jsonObj.getString("op");
        jsonObj.remove("op");

        String sinkTable = tableProcessDim.getSinkTable();

        String rowKey = jsonObj.getString(tableProcessDim.getSinkRowKey());

        if("d".equals(type)){
            HBaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
        }else{
            String sinkFamily = tableProcessDim.getSinkFamily();
            HBaseUtil.putRow(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,jsonObj);
        }
        if("u".equals(type)||"d".equals(type)){
            String key = RedisUtil.getKey(sinkTable, rowKey);
            jedis.del(key);
        }
    }


}
