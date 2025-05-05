package realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package realtime.common.bean.bean
 * @Author zhaohua.liu
 * @Date 2025/4/9.23:43
 * @description: dim实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcessDim {
    String sourceTable;
    String sinkTable;
    String sinkFamily;
    String sinkColumns;
    String sinkRowKey;
    String op;
}
