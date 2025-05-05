package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.cm.bean.UserRegisterBean
 * @Author chen.ming
 * @Date 2025/5/2 10:09
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    String curDate;
    // 注册用户数
    Long registerCt;
}
