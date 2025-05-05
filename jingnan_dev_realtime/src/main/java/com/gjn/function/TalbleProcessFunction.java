package com.gjn.function;

import com.alibaba.fastjson.JSONObject;
import com.gjn.base.TableProcessDim;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

/**
 * @Package com.gjn.function.TalbleProcessFunction
 * @Author jingnan.guo
 * @Date 2025/4/14 9:54
 * @description:处理主流业务数据和广播流配置数据关联后的逻辑
 */
public class TalbleProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>> {

    private  MapStateDescriptor<String, TableProcessDim> mapStateDescriptor;
    private Map<String,TableProcessDim> configMap = new HashMap<>();

    public TalbleProcessFunction(MapStateDescriptor<String, TableProcessDim> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //将配置表中的配置信息预加载到我们的程序configMap中
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        //建立连接
        java.sql.Connection conn = DriverManager.getConnection("jdbc:mysql://cdh03:3306?useSSL=false", "root", "root");
        //获取数据库操作对象
        String sql="select * from gmall2024_config.table_process_dim";
        PreparedStatement ps = conn.prepareStatement(sql);
        //执行sql语句
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        //处理结果集
        while (rs.next()){
            //定义 一个json 对象 用于接受遍历出来的数据
            JSONObject jsonObj = new JSONObject();
            for (int i=1;i<= metaData.getColumnCount();i++){
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(i);
                jsonObj.put(columnName,columnValue);
            }
            //将jsonObj转换为实体类对象，并放到configmap中
            TableProcessDim tableProcessDim = jsonObj.toJavaObject(TableProcessDim.class);
            configMap.put(tableProcessDim.getSourceTable(),tableProcessDim);
        }
        //释放资源
        rs.close();
        ps.close();
        conn.close();
    }

    // 处理主流业务数据
    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
        String table = jsonObj.getJSONObject("source").getString("table");
        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //根据表明先到广播状态中获取对应的配置信息  如果没有找到对应的配置 在尝试到 configMap中获取
        TableProcessDim tableProcessDim = null;


        if ((tableProcessDim  =  broadcastState.get(table))!=null || (tableProcessDim  =  configMap.get(table))!=null){
            //如果根据表明获取到了对应的配置信息，说明当前处理的是维度数据    将维度数据继续向下游专递

            //将维度数据继续向下游传递（只需要传递data属性内容即可）
            JSONObject dataJsonObj = jsonObj.getJSONObject("after");

            //在向下游传递之前，过滤掉不需要传递的属性
            String sinkColumns = tableProcessDim.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj,sinkColumns);

            //向下游传递数据前，补充对维度数据的操作类型属性
            String type = jsonObj.getString("op");
            dataJsonObj.put("op",type);

            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));
        }
    }

    // 处理广播流中的配置信息
    @Override
    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject,TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject,TableProcessDim>> out) throws Exception {
        String op = tp.getOp();

        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //获取维度表的名称
        String sourceTable = tp.getSourceTable();
        if("d".equals(op)){
            //从配置表中删除了一条配置信息  也从广播状态中进行删除
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        }else{
            //对配置信息进行了读取或者添加  更新操作  将配置信息放到广播流状态中
            broadcastState.put(sourceTable,tp);
            configMap.put(sourceTable,tp);
        }
    }
    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        String[] columnArr = sinkColumns.split(",");
        JSONObject newJsonObj = new JSONObject();
        for(String column:columnArr){
            newJsonObj.put(column,dataJsonObj.getString(column));
        }
    }
}


