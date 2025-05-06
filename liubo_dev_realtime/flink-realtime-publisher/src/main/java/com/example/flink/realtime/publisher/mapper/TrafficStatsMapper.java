package com.example.flink.realtime.publisher.mapper;

import com.example.flink.realtime.publisher.bean.TrafficUvCt;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @version 1.0
 * @Package com.example.flink.realtime.publisher.mapper.TrafficStatsMapper
 * @Author liu.bo
 * @Date 2025/5/4 15:00
 * @description:  流量域统计mapper接口
 */
public interface TrafficStatsMapper {
    //获取某天各个渠道独立访客
    @Select("select ch,sum(uv_ct) uv_ct from dws_traffic_vc_ch_ar_is_new_page_view_window partition p#{date} " +
            "group by ch order by uv_ct desc limit #{limit}")
    List<TrafficUvCt> selectChUvCt(@Param("date") Integer date, @Param("limit") Integer limit);
}
