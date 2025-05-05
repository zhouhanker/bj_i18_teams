package com.ytx.realtime.dim.function;


import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.JdbcUtils;
import com.ytx.bean.TableProcessDim;
import com.ytx.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    private MapStateDescriptor<String,TableProcessDim> mapStateDescriptor;
    private Map<String, TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //            @Override
    public void open(Configuration parameters) throws Exception {
//        Class.forName("com.mysql.cj.jdbc.Driver");
////               建立连接
//        java.sql.Connection conn= DriverManager.getConnection(Constant.MYSQL_URL,Constant.MYSQL_USER_NAME,Constant.MYSQL_PASSWORD);
//        String sql="select * from realtime_v1_config.table_process_dim";
//        PreparedStatement ps = conn.prepareStatement(sql);
////                执行sql语句
//        ResultSet rs = ps.executeQuery();
//        ResultSetMetaData metaData = rs.getMetaData();
////             处理结果集
//        while (rs.next()){
//            JSONObject jsonObj = new JSONObject();
//            for (int i = 1; i < metaData.getColumnCount(); i++) {
//                String catalogName = metaData.getCatalogName(i);
//                Object columnValue = rs.getObject(i);
//                jsonObj.put(catalogName,columnValue);
//            }
//            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
//            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
//        }
//        rs.close();
//        ps.close();
//        conn.close();

        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtils.queryList(mySQLConnection, "select * from realtime_v1_config.table_process_dim", TableProcessDim.class);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtils.closeMySQLConnection(mySQLConnection);

    }

    //            主流数据
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDim> state = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String table = jsonObject.getJSONObject("source").getString("table");
        TableProcessDim tableProcessDim = state.get(table);
        if (tableProcessDim != null) {
            JSONObject after = jsonObject.getJSONObject("after");
            String op = jsonObject.getString("op");
            after.put("op",op);
            collector.collect(Tuple2.of(after,tableProcessDim));

        }
    }        //广播流
    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        String op = tp.getOp();
//                获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//              维度表名称
        String sourceTable = tp.getSourceTable();
        if ("d".equals(op)) {
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            broadcastState.put(sourceTable, tp);
            configMap.put(sourceTable, tp);
        }
    }
}

