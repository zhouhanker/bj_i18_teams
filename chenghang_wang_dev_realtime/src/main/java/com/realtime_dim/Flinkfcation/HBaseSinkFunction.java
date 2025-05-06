package com.realtime_dim.Flinkfcation;

import com.Constat.constat;
import com.alibaba.fastjson.JSONObject;
import com.realtime_dim.Utils.Hbaseutli;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import com.realtime_dim.Bean.TableProcessDim;


/**
 * @Package realtime_dim.Flinkfcation.HBaseSinkFunction
 * @Author chenghang.wang
 * @Date 2025/5/5 20:17
 * @description:
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = Hbaseutli.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        Hbaseutli.closeHBaseConnection(hbaseConn);
    }

    //将流中数据写入HBase中
    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObj = value.f0;
        TableProcessDim tableProcessDim = value.f1;
        String op = jsonObj.getString("op");
        jsonObj.remove("op");

        //获取操作的HBase的表名
        String sinkTable = tableProcessDim.getSink_table();
        //获取row_key
        String rowKey = jsonObj.getString(tableProcessDim.getSink_row_key());

        //判断对业务数据维度表进行来了什么操作
        if ("d".equals(op)) {
            //从业务数据库维度表中做了删除操作  需要将HBase维度表中对应的记录也删除掉
            Hbaseutli.delRow(hbaseConn, constat.HBASE_NAMESPACE, sinkTable, rowKey);
        } else {
            //如果不是delete,可能的存在
            String sinkFamily = tableProcessDim.getSink_family();
            Hbaseutli.putRow(hbaseConn, constat.HBASE_NAMESPACE, sinkTable, rowKey, sinkFamily, jsonObj);
        }

    }

}
