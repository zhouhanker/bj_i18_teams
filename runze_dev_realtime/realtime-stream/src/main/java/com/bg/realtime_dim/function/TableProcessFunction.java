package com.bg.realtime_dim.function;

import com.alibaba.fastjson.JSONObject;
import com.bg.common.bean.TableProcessDim;
import com.bg.common.util.JdbcUtil;
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
 * @Package com.bg.realtime_dim.function.TableProcessFunction
 * @Author Chen.Run.ze
 * @Date 2025/4/9 14:08
 * @description: 处理主流业务数据和广播流配置数据
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>> {

    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    private Map<String,TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表中的配置信息预加载到configMap中
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mysqlConnection, "select * from gmall2024_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeMysqlConnection(mysqlConnection);
    }

    //处理主流业务数据根据维度表名到广播状态中读取配置信息，判断是否为维度
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {
        //获取处理数据的表明
        String table = jsonObject.getJSONObject("source").getString("table");

        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

        //根据表明到广播状态中获取相应的配置
        TableProcessDim tableProcessDim = broadcastState.get(table);

        if (tableProcessDim!=null){
            //如果根据表明获取到了配置信息,说明当前处理的是维度数据

            //将维度数据继续向下游传递
            JSONObject data = jsonObject.getJSONObject("after");

            //在向下游传递数据,过滤不需要传递的属性
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(data,sinkColumns);

            //在向下游传递数据,不充维度数据的操作类型属性
            String type = jsonObject.getString("op");
            data.put("op",type);

            collector.collect(Tuple2.of(data,tableProcessDim));
        }
    }
    //处理广播流配智信息 v:一个配理对象将配胃数据放到广播状会中 k:维度表名
    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context context, Collector<Tuple2<JSONObject,TableProcessDim>> collector) throws Exception {
        //获取操作状态
        String op = tp.getOp();

        //获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);

        //获取维度表的名称
        String sourceTable = tp.getSourceTable();

        if ("d".equals(op)){
            //从配置表中删除了一条数据，将对应的配置信息从广播状态中删除
            broadcastState.remove(sourceTable);
        }else {
            //对配置表进行了读取,添加,更新, 将最新的配置信息放到广播状态中
            broadcastState.put(sourceTable,tp);
        }
    }

    //过滤掉不需要传递的字段
    //data {"tm_name":"Redmi", "create_time":"2021-12-14 00:00:00","1ogo_ur":"555","id":1}
    //sinkColumnist,tm_name
    private static void deleteNotNeedColumns(JSONObject data, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = data.entrySet();

        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));

    }
}
