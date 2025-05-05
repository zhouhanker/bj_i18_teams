package realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import org.mortbay.util.ajax.JSON;
import realtime.common.bean.TableProcessDim;
import realtime.common.constant.Constant;
import realtime.common.util.HbaseUtil;

/**
 * @Package realtime.dim.function.HBaseSinkFunction
 * @Author zhaohua.liu
 * @Date 2025/4/14.9:37
 * @description: 将配置信息写入hbase表中
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbaseConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hbaseConn = HbaseUtil.getHbaseConnection();
    }

    @Override
    public void close() throws Exception {
        HbaseUtil.closeHbaseConnection(hbaseConn);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject DataJsonObj = value.f0;
        TableProcessDim tableProcessDim = value.f1;
        String op = DataJsonObj.getString("op");
        //删除op字只剩下数据json
        DataJsonObj.remove("op");

        //获取要操作的表名
        String sinkTable = tableProcessDim.getSinkTable();
        //获取rowKey
        String rowKey = DataJsonObj.getString(tableProcessDim.getSinkRowKey());

        //根据op字段对数据进行增删改查
        if ("d".equals(op)){
            HbaseUtil.delRow(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable,rowKey);
        }else {
            //获取列族
            String sinkFamily = tableProcessDim.getSinkFamily();
            HbaseUtil.putRow(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,rowKey,sinkFamily,DataJsonObj);
        }
    }
}
