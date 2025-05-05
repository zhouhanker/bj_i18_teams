package com.sdy.dwd.function;

import com.alibaba.fastjson.JSONObject;
import com.sdy.bean.JdbcUtil;
import com.sdy.bean.TableProcessDwd;

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
 * @Package com.sdy.retail.v1.realtime.dwd.function.BaseDbTableProcessFunction
 * @Author danyu-shi
 * @Date 2025/4/10 20:38
 * @description:
 */
public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>> {

    private MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor;

    private Map<String, TableProcessDwd> configMap = new HashMap<>();

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置信息预加载到程序中
        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<TableProcessDwd> tableProcessDwdList
                = JdbcUtil.queryList(mySQLConnection, "select * from realtime_v1_config.table_process_dwd", TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
            String sourceTable = tableProcessDwd.getSourceTable();
//            String op = tableProcessDwd.getOp();
            String sourceType = tableProcessDwd.getSourceType();
            //insert  update
            if ("insert".equals(sourceType) || "update".equals(sourceType)){
                sourceType = "c";
            }else if("delete".equals(sourceType)){
                sourceType = "d";
            }else {
                sourceType = "u";
            }

            String key = getKey(sourceTable, sourceType);
//            System.out.println("key-->"+key);
//            System.out.println("sourceType-->" + sourceType);
//            System.out.println("tableProcessDwd-->" +tableProcessDwd);
            configMap.put(key, tableProcessDwd);


        }
        JdbcUtil.closeMySQLConnection(mySQLConnection);
    }
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        //主流 获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDwd> state = readOnlyContext.getBroadcastState(mapStateDescriptor);

//                System.out.println("processElement" + jsonObj);
//        Thread.sleep(1000);
        //获取主流内的table表名
        String table = jsonObj.getJSONObject("source").getString("table");
        //获取操作类型
        String op = jsonObj.getString("op");
        //拼接key
        String key = getKey(table, op);

        //根据表名先到广播状态中获取对应的配置信息，如果没有找到对应的配置，再尝试到configMap中获取
        TableProcessDwd tableProcessDim = null;

//                for (Map.Entry<String, TableProcessDwd> entry : state.immutableEntries()) {
//                    String key1 = entry.getKey();
//                    TableProcessDwd value1 = entry.getValue();
//                    System.out.println("Key: " + key1 + ", Value: " + value1);
//                }

        if ((tableProcessDim = state.get(key)) != null
                ||(tableProcessDim = configMap.get(key)) != null) {

            // 将维度数据继续向下游传递(只需要传递data属性内容即可)
            JSONObject dataJsonObj = jsonObj.getJSONObject("after");

            //在向下游传递数据前，过滤掉不需要传递的属性
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj, sinkColumns);

            //在向下游传递数据前， 将ts事件时间补充到data对象上
            Long ts_ms = jsonObj.getLong("ts_ms");
            dataJsonObj.put("ts_ms", ts_ms);

            out.collect(Tuple2.of(dataJsonObj, tableProcessDim));
        }


    }

    @Override
    public void processBroadcastElement(TableProcessDwd tp, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        //广播流
//                System.out.println("processBroadcastElement" + tp);
        //获取 op 操作状态
        String op = tp.getOp();
        //获取 要写入状态算子的 数据
        String sourceTable = tp.getSourceTable();
        //获取 状态算子
        BroadcastState<String, TableProcessDwd> state = context.getBroadcastState(mapStateDescriptor);

        //获取业务数据库的表对应的操作类型
        String sourceType = tp.getSourceType();
        //拼接key
        String key = getKey(sourceTable, op);
        //判断 是否是删除操作
        if ("d".equals(op)) {
            //删除 状态算子 里对应的key的数据
            state.remove(sourceTable);
            configMap.remove(key);
        } else {
            //从配置表中读取数据或者添加、更新了数据  需要将最新的这条配置信息放到广播状态以及configMap中
            state.put(key, tp);
            configMap.put(key,tp);
        }
    }
    private static String getKey(String sourceTable, String sourceType) {
        String key = sourceTable + ":" + sourceType;
        return key;
    }
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));

    }
}
