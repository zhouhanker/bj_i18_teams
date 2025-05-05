package realtime.dim.function;

import cn.hutool.core.collection.ListUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import realtime.common.bean.TableProcessDim;
import realtime.common.util.HbaseUtil;
import realtime.common.util.JDBCUtil;

import java.sql.Connection;
import java.util.*;

/**
 * @Package realtime.dim.function.TableProcessFunction
 * @Author zhaohua.liu
 * @Date 2025/4/11.10:03
 * @description:处理主流业务数据和广播流配置数据关联后的逻辑
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>> {
    //成员变量
    private MapStateDescriptor<String,TableProcessDim> mapStateDescriptor;
    private Map<String,TableProcessDim> configMap = new HashMap<>();

    //构造方法
    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        //类型为string,TableProcessDim,使用string标记广播流中的数据,对广播状态进行增删改
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //todo open方法
    //将mysql配置表中的配置信息预加载到程序configMap中
    //open:将配置信息预加载到程序中，避免主流数据先到，广播流数据后到，丢失数据的情况
    @Override
    public void open(Configuration parameters) throws Exception {
        Connection mySQLConn = JDBCUtil.getMySQLConnection();
        List<TableProcessDim> dimList = JDBCUtil.queryList(mySQLConn, "select * from `e_commerce_config`.`table_process_dim`", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : dimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JDBCUtil.closeMySQLConnect(mySQLConn);
    }




    //todo 处理数据流
    //processElement 方法根据表名先到广播状态中获取对应的配置信息，如果没有找到对应的配置，再尝试到configMap中获取
    @Override
    public void processElement(JSONObject jsonObject,
                               BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext,
                               Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        //获取e_commerce数据的表名
        String table = jsonObject.getJSONObject("source").getString("table");
        //获取广播状态,准备根据string获取TableProcessDim
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        //e_commerce数据的表名,获取e_commerce_config中的数据
        //根据表名先到广播状态中获取对应的配置信息，如果没有找到对应的配置，再尝试到configMap中获取
        TableProcessDim tableProcessDim = null;
        //判断是否取值成功,有一个取值成功就行
        //如果根据表名获取到了对应的配置信息，说明当前处理的是维度数据
        JSONObject dataJsonObj = null;
        if (
                (tableProcessDim = configMap.get(table))!=null
                || (tableProcessDim = broadcastState.get(table))!=null
        ) {
            // 将维度数据继续向下游传递(只需要传递data属性内容即可)
            String op = jsonObject.getString("op");
            if ("d".equals(op)) {
                dataJsonObj = jsonObject.getJSONObject("before");
            } else {
                dataJsonObj = jsonObject.getJSONObject("after");
            }

            //过滤掉维度数据中不需要的列
            String sinkColumns = tableProcessDim.getSinkColumns();
            List<String> colList = Arrays.asList(sinkColumns.split(","));
            Set<Map.Entry<String, Object>> entries = dataJsonObj.entrySet();
            entries.removeIf(entry -> !colList.contains(entry.getKey()));

            //在向下游传递数据前，补充对维度数据的操作类型属性
            dataJsonObj.put("op", op);

            //返回(维度数据+操作类型,对应的TableProcessDim)
            collector.collect(Tuple2.of(dataJsonObj, tableProcessDim));
        }
    }


    //todo 处理广播流
    //更新hashmap和广播状态中的维度数据
    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim,
                                        BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context,
                                        Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        //获取op字段
        String op = tableProcessDim.getOp();
        //获取mysql维度表名
        String table = tableProcessDim.getSourceTable();
        //获取广播流状态
        BroadcastState<String, TableProcessDim> broadcastState = context.getBroadcastState(mapStateDescriptor);
        //根据op字段进行判断,对广播流内的TableProcessDim进行增删改
        if ("d".equals(op)){
            //从配置表中删除了一条数据，将对应的配置信息也从广播状态中删除
            broadcastState.remove(table);
            //hashmap中也删除
            configMap.remove(table);
        }else {
            //对配置表进行了读取、添加或者更新操作，将最新的配置信息放到广播状态中
            broadcastState.put(table,tableProcessDim);
            //hashmap也同步操作
            configMap.put(table,tableProcessDim);
        }
    }
}



