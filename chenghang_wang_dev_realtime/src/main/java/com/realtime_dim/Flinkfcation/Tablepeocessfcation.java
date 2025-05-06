package com.realtime_dim.Flinkfcation;


import com.alibaba.fastjson.JSONObject;
import com.utils.JdbsUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import com.realtime_dim.Bean.TableProcessDim;

import java.sql.Connection;
import java.util.*;

/**
 * @Package realtime_Dim.flinkfcation.Tablepeocessfcation
 * @Author ayang
 * @Date 2025/4/10 10:55
 * @description: aa
 */
public class Tablepeocessfcation extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>{

    private Map<String, TableProcessDim> configMap = new HashMap<>();
    private  MapStateDescriptor<String, TableProcessDim> tableMapStateDescriptor;

    public Tablepeocessfcation(MapStateDescriptor<String, TableProcessDim> tableMapStateDescriptor) {
        this.tableMapStateDescriptor = tableMapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection mySQLConnection = JdbsUtils.getMySQLConnection();
        List<TableProcessDim> TableProcessDims = JdbsUtils.queryList(mySQLConnection, "select * from realtime.table_process_dim", TableProcessDim.class);
        for (TableProcessDim TableProcessDim : TableProcessDims) {
            configMap.put(TableProcessDim.getSource_table(),TableProcessDim);
        }
        JdbsUtils.closeMySQLConnection(mySQLConnection);

    }

    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {

        String table = jsonObj.getJSONObject("source").getString("table");
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(tableMapStateDescriptor);
        TableProcessDim tableProcessDim = broadcastState.get(table);
        if (tableProcessDim != null) {
            JSONObject dataJsonObj = jsonObj.getJSONObject("after");
            String sinkColumns = tableProcessDim.getSink_columns();

            deletenotneetclomns(dataJsonObj, sinkColumns);
            String type = jsonObj.getString("op");
            dataJsonObj.put("op", type);
            out.collect(Tuple2.of(dataJsonObj, tableProcessDim));
        }

    }

    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
        String op = tp.getOp();

        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(tableMapStateDescriptor);
        String sourceTable = tp.getSource_table();
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
