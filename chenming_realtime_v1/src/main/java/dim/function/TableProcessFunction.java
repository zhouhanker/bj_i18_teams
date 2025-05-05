package dim.function;

import com.alibaba.fastjson.JSONObject;
import bean.TableProcessDim;
import constant.Constant;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.*;

/**
 * @Package com.cm.dim.function.TableProcessFunction
 * @Author  chen.ming
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
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
        String sql = "select * from realtime_v1_config.table_process_dim";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()){
            JSONObject jsonObj = new JSONObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                jsonObj.put(columnName,columnValue);
            }
            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }

        rs.close();
        ps.close();
        conn.close();
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
            deleteNeedColumns(data,sinkColumns);

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
    //dataJsonObj  {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"555","id":1}
    //sinkColumns  id,tm_name
    private static void deleteNeedColumns(JSONObject date, String sinkColumns) {
        List<String> coulumlist = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entries = date.entrySet();
        entries.removeIf(entry->!coulumlist.contains(entry.getKey()));
    }
}
