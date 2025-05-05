package function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import bean.TableProcessDim;
import utils.JdbcUtil;

import java.sql.Connection;
import java.util.*;

/**
 * @Package v1.realtime.constant.Constant
 * @Author yinshi
 * @Date 2025/5/3 14:09
 * @description: Constant
 */

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>> {

    private MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    private Map<String,TableProcessDim> configMap = new HashMap<>();

    public TableProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        Connection mySQLConnection = JdbcUtil.getMySQLConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mySQLConnection, "select * from realtime_v2.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        JdbcUtil.closeMySQLConnection(mySQLConnection);
    }

    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {

        String table = jsonObj.getJSONObject("source").getString("table");

        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        TableProcessDim tableProcessDim = null ;

        if((tableProcessDim =  broadcastState.get(table)) != null
                || (tableProcessDim =  configMap.get(table)) != null){
            JSONObject dataJsonObj = jsonObj.getJSONObject("after");
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj,sinkColumns);
            String type = jsonObj.getString("op");
            dataJsonObj.put("op",type);
            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));
        }
    }

    //处理广播流配置信息    将配置数据放到广播状态中或者从广播状态中删除对应的配置  k:维度表名     v:一个配置对象
    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
        //获取对配置表进行的操作的类型
        String op = tp.getOp();
        //获取广播状态
        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //获取维度表名称
        String sourceTable = tp.getSourceTable();
        if("d".equals(op)){
            //从配置表中删除了一条数据，将对应的配置信息也从广播状态中删除
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        }else{
            //对配置表进行了读取、添加或者更新操作，将最新的配置信息放到广播状态中
            broadcastState.put(sourceTable,tp);
            configMap.put(sourceTable,tp);
        }
    }

    //过滤掉不需要传递的字段
    //dataJsonObj  {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"555","id":1}
    //sinkColumns  id,tm_name
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));

    }
}
