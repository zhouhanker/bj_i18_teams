package com.gy.realtime_dim.flinkfcation;


import com.alibaba.fastjson.JSONObject;
import com.gy.bean.CommonTable;
import com.gy.utils.JdbsUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * @Package realtime_Dim.flinkfcation.Tablepeocessfcation
 * @Author guangyi_zhou
 * @Date 2025/4/10 10:55
 * @description: aa
 */
public class Tablepeocessfcation extends BroadcastProcessFunction<JSONObject, CommonTable, Tuple2<JSONObject, CommonTable>>{

    private Map<String, CommonTable> configMap = new HashMap<>();
    private  MapStateDescriptor<String, CommonTable> tableMapStateDescriptor;

    public Tablepeocessfcation(MapStateDescriptor<String, CommonTable> tableMapStateDescriptor) {
        this.tableMapStateDescriptor = tableMapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection mySQLConnection = JdbsUtils.getMySQLConnection();
        List<CommonTable> commonTables = JdbsUtils.queryList(mySQLConnection, "select * from stream_retail_config.table_process_dim", CommonTable.class);
        for (CommonTable commonTable : commonTables) {
            configMap.put(commonTable.getSourceTable(),commonTable);
        }
        JdbsUtils.closeMySQLConnection(mySQLConnection);

    }

    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, CommonTable, Tuple2<JSONObject, CommonTable>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, CommonTable>> out) throws Exception {

        String table = jsonObj.getJSONObject("source").getString("table");
        ReadOnlyBroadcastState<String, CommonTable> broadcastState = ctx.getBroadcastState(tableMapStateDescriptor);
        CommonTable tableProcessDim = broadcastState.get(table);
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
    public void processBroadcastElement(CommonTable tp, BroadcastProcessFunction<JSONObject, CommonTable, Tuple2<JSONObject, CommonTable>>.Context ctx, Collector<Tuple2<JSONObject, CommonTable>> out) throws Exception {
        String op = tp.getOp();

        BroadcastState<String, CommonTable> broadcastState = ctx.getBroadcastState(tableMapStateDescriptor);
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
