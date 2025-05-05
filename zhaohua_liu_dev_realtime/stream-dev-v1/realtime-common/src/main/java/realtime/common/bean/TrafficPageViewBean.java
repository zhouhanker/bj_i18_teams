package realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package realtime.common.bean.TrafficPageViewBean
 * @Author zhaohua.liu
 * @Date 2025/4/22.13:58
 * @description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TrafficPageViewBean {
    //窗口开始时间
    private String stt;
    //窗口结束时间
    private String edt;
    //当天日期
    private String cur_date;
    //app版本号
    private String vc;
    //渠道
    private String ch;
    //地区
    private String ar;
    //新老访客状态标记
    private String isNew;
    //独立访客数
    private Long uvCt;
    //会话数
    private Long svCt;
    //页面浏览数
    private Long pvCt;
    //累计访问时长
    private Long durSum;
    //时间戳
    //意味着该字段在进行 JSON 序列化时会被忽略，不会包含在生成的 JSON 字符串里。
    @JSONField(serialize = false)
    private Long ts;

}
