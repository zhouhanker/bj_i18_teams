package bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package bean.UserLoginBean
 * @Author ayang
 * @Date 2025/4/16 9:41
 * @description: 用户回流和单日独立访客
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class UserLoginBean {
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    String curDate;
    // 回流用户数
    Long backCt;
    // 独立用户数
    Long uuCt;
    // 时间戳
    @JSONField(serialize = false)
    Long ts;

}
