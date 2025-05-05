package com.zsf.retail_v1.realtime.dim;

import com.alibaba.fastjson.JSONObject;
import com.zsf.retail_v1.realtime.util.JdbcUtil;
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
 * @Package com.zsf.retail_v1.realtime.dim.TableProcessFunction
 * @Author zhao.shuai.fei1
 * @Date 2025/4/23 20:38
 * @description:
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, JSONObject, Tuple2<JSONObject,JSONObject>> {
    private MapStateDescriptor<String, JSONObject> mapStateDescriptor;
    private Map<String,JSONObject> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, JSONObject> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表中的配置信息预加载到程序configMap中
        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<JSONObject> JSONObjectList = JdbcUtil.queryList(mySQLConnection, "select * from sx_002_v2.table_process_dim", JSONObject.class, true);
        for (JSONObject JSONObject : JSONObjectList) {
            configMap.put(JSONObject.getString("sourceTable"),JSONObject);
        }
        JdbcUtil.closeMySQLConnection(mySQLConnection);
    }

    //处理主流业务数据               根据维度表名到广播状态中读取配置信息，判断是否为维度
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, JSONObject, Tuple2<JSONObject,JSONObject>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,JSONObject>> out) throws Exception {
        //主流 状态变量
        ReadOnlyBroadcastState<String, JSONObject> state = ctx.getBroadcastState(mapStateDescriptor);

        //获取主流内的table表名
        String table = jsonObj.getJSONObject("source").getString("table");

        //根据表名先到广播状态中获取对应的配置信息，如果没有找到对应的配置，再尝试到configMap中获取
        JSONObject JSONObject = null;

        if ((JSONObject = state.get(table)) != null
                || (JSONObject =  configMap.get(table)) != null) {
            //如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据

            // 将维度数据继续向下游传递(只需要传递data属性内容即可)
            JSONObject dataJsonObj = jsonObj.getJSONObject("after");

            //在向下游传递数据前，过滤掉不需要传递的属性
            String sinkColumns = JSONObject.getString("sink_columns");
            deleteNotNeedColumns(dataJsonObj, sinkColumns);

            //在向下游传递数据前，补充对维度数据的操作类型属性
            String op = jsonObj.getString("op");
            dataJsonObj.put("type", op);

            out.collect(Tuple2.of(dataJsonObj, JSONObject));
        }
    }

    //处理广播流配置信息    将配置数据放到广播状态中或者从广播状态中删除对应的配置  k:维度表名     v:一个配置对象
    @Override
    public void processBroadcastElement(JSONObject tp, BroadcastProcessFunction<JSONObject, JSONObject, Tuple2<JSONObject,JSONObject>>.Context ctx, Collector<Tuple2<JSONObject,JSONObject>> out) throws Exception {
        //获取 op 操作状态
        String op = tp.getString("op");
        //获取 要写入状态算子的 数据
        String sourceTable = tp.getString("source_table");
        //获取 状态算子
        BroadcastState<String, JSONObject> state = ctx.getBroadcastState(mapStateDescriptor);
        //判断 是否是删除操作
        if ("d".equals(op)) {
            //删除 状态算子 里对应的key的数据
            state.remove(sourceTable);
        } else {
            //非删除 状态算子 里添加对应的数据
            state.put(sourceTable, tp);
            configMap.put(sourceTable,tp);
        }
    }

    //过滤掉不需要传递的字段
    //dataJsonObj  {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"555","id":1}
    //sinkColumns  id,tm_name
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        if(sinkColumns!=null){
            List<String> columnList = Arrays.asList(sinkColumns.split(","));

            Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

            entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
        }
    }
}
