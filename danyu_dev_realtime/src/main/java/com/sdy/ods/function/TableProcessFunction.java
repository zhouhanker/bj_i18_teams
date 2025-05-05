package com.sdy.ods.function;

import com.alibaba.fastjson.JSONObject;
import com.sdy.bean.TableProcessDim;
import com.sdy.domain.Constant;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;
/**
 * @Package com.sdy.retail.v1.realtime.dim.function.TableProcessFunction
 * @Author danyu-shi
 * @Date 2025/4/30 8:46
 * @description:
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    private Map<String, TableProcessDim> configMap = new HashMap<>();
    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;


    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
        String sql = "select * from realtime_v1_config.table_process_dim";

        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                jsonObj.put(columnName, columnValue);
            }
            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }

        rs.close();
        ps.close();
        conn.close();
    }

    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {

        String table = jsonObj.getJSONObject("source").getString("table");
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcessDim tableProcessDim = broadcastState.get(table);
        if (tableProcessDim != null) {
            JSONObject dataJsonObj = jsonObj.getJSONObject("after");
            String sinkColumns = tableProcessDim.getSinkColumns();

            deletenotneetclomns(dataJsonObj, sinkColumns);
            String type = jsonObj.getString("op");
            dataJsonObj.put("op", type);
            out.collect(Tuple2.of(dataJsonObj, tableProcessDim));
        }

    }

    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        String op = tp.getOp();

        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String sourceTable = tp.getSourceTable();
        if ("d".equals(op)) {
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        } else {
            broadcastState.put(sourceTable, tp);
            configMap.put(sourceTable, tp);
        }
    }

    private static void deletenotneetclomns(JSONObject after, String sinkColumns) {
        List<String> list = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entries = after.entrySet();
        entries.removeIf(e -> !list.contains(e.getKey()));
    }
}
