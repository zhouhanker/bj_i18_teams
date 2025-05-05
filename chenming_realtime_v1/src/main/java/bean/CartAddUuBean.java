package bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.cm.bean.CartAddUuBean
 * @Author chen.ming
 * @Date 2025/5/2 10:09
 * @description:
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
