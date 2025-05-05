package bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package v1.realtime.bean.CartAddUuBean
 * @Author yinshi
 * @Date 2025/5/2 18:42
 * @description: CartAddUuBean
 */

@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
