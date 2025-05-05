package realtime.dwd.db.split.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import realtime.common.bean.TableProcessDwd;
import realtime.common.util.JDBCUtil;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Package realtime.dwd.db.split.app.function.BaseDbTableProcessFunction
 * @Author zhaohua.liu
 * @Date 2025/4/17.15:03
 * @description:
 */
public class BaseDbTableProcessFunction extends  BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>> {
    //定义成员变量
    private MapStateDescriptor<String,TableProcessDwd> mapStateDescriptor;
    private Map<String,TableProcessDwd> configMap = new HashMap<>();

    //定义构造函数
    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        //类型为string,TableProcessDwd,使用string来标记广播流中
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //open方法
    //使用jdbc直接读取mysql,将配置信息预加载到程序的HashMap中
    @Override
    public void open(Configuration parameters) throws Exception {
        Connection mysqlConn = JDBCUtil.getMySQLConnection();
        List<TableProcessDwd> tableProcessDwdList = JDBCUtil.queryList(mysqlConn, "select * from e_commerce_config.table_process_dwd", TableProcessDwd.class, true);
        //拼接来源表和操作类型
        for (TableProcessDwd tableProcessDwd : tableProcessDwdList) {
            String key = tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType();
            configMap.put(key,tableProcessDwd);
        }
        //关闭jdbc连接
        JDBCUtil.closeMySQLConnect(mysqlConn);

    }


    //处理数据流
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        //获取业务数据的表名
        String table = jsonObject.getJSONObject("source").getString("table");
        //获取业务数据进行的操作
        String op = jsonObject.getString("op");
        //拼接表面和op字段为key
        String key = table + ":" + op;
        //获取广播状态
        TableProcessDwd tableProcessDwd = null;

        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        //如果从HashMap或者广播流读取到key对应的value
        if((tableProcessDwd=broadcastState.get(key))!=null
        || (tableProcessDwd=configMap.get(key))!=null){
            //因为key中包含了op字段,所以我们向下游传播时,实现了对业务数据的op字段的筛选
            //比如读取order_info表操作类型为c的数据
            //op=d时,传递before,其他的传递after,
            JSONObject dataJson = null;
            if ("d".equals(op)){
                dataJson = jsonObject.getJSONObject("before");
            }else {
                dataJson = jsonObject.getJSONObject("after");
            }
            Long ts = jsonObject.getLong("ts_ms");
            dataJson.put("ts",ts);
            //返回带有ts字段的数据,op=d时,传递before,其他的传递after,以及tableProcessDwd
            collector.collect(Tuple2.of(dataJson,tableProcessDwd));
        }


    }
    //处理广播流
    @Override
    public void processBroadcastElement(TableProcessDwd tableProcessDwd, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context context, Collector<Tuple2<JSONObject, TableProcessDwd>> collector) throws Exception {
        //获取op字段,用于操作广播状态
        String op = tableProcessDwd.getOp();
        //获取广播状态
        BroadcastState<String, TableProcessDwd> broadcastState = context.getBroadcastState(mapStateDescriptor);

        String sourceTable = tableProcessDwd.getSourceTable();
        String sourceType = tableProcessDwd.getSourceType();
        String key = sourceTable + ":" + sourceType;

        //根据实体类的op字段对广播状态和HashMap进行更新
        if ("d".equals(op)){
            broadcastState.remove(key);
            configMap.remove(key);
        }else {
            broadcastState.put(key,tableProcessDwd);
            configMap.put(key,tableProcessDwd);
        }

    }
}
