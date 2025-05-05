package com.ytx.realtime.dwd.basedb.function;

import com.alibaba.fastjson.JSONObject;

import com.ytx.bean.TableProcessDwd;
import com.ytx.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseDbTableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>> {
  private MapStateDescriptor<String,TableProcessDwd> mapStateDescriptor;
  private Map<String,TableProcessDwd> configMap=new HashMap<>();

    public BaseDbTableProcessFunction(MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        this.mapStateDescriptor=mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        将配置信息加载到程序中
      Connection mySQLConnection = JdbcUtil.getMySQLConnection();
      List<TableProcessDwd> tableProcessDwds =
              JdbcUtil.queryList(mySQLConnection, "select * from realtime_v1_config.table_process_dwd", TableProcessDwd.class, true);
      for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
        String sourceTable = tableProcessDwd.getSourceTable();
        String sourceType = tableProcessDwd.getSourceType();
         String key=getKey(sourceTable,sourceType);
         configMap.put(key,tableProcessDwd);
      }
      JdbcUtil.closeMySQLConnection(mySQLConnection);

    }

  private String getKey(String sourceTable, String sourceType) {
   String key=sourceTable+":"+sourceType;
    return key;

  }

  //    处理主流业务数据
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
        String table = jsonObj.getJSONObject("source").getString("table");
        String op = jsonObj.getString("op");
        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        TableProcessDwd tableProcessDim = broadcastState.get(table);
        if (tableProcessDim != null){
            JSONObject dataJsonObj = jsonObj.getJSONObject("after");

            Long ts = jsonObj.getLong("ts_ms");
            dataJsonObj.put("ts_ms",ts);
            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));

        }
    }
//  处理广播流配置数据
    @Override
    public void processBroadcastElement(TableProcessDwd tp, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
      String op = tp.getOp();
//      获取广播流状态
      BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//      获取业务数据表名
        String sourceTable = tp.getSourceTable();

        if ("d".equals(op)){
        broadcastState.remove(sourceTable);
        configMap.remove(sourceTable);
      }else {
            broadcastState.put(sourceTable,tp);
            configMap.put(sourceTable,tp);
      }
    }

}
