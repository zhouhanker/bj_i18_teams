package bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * @Package v1.realtime.bean.CartAddUuBean
 * @Author yinshi
 * @Date 2025/5/2 18:42
 * @description: CartAddUuBean
 */

@Data
@AllArgsConstructor
public class CartAddUuBean implements Serializable {
    String stt;
    String edt;
    String curDate;
    Long cartAddUuCt;
}
