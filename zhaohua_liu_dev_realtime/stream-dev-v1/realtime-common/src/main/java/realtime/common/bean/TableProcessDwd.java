package realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package realtime.common.bean.TableProcessDwd
 * @Author zhaohua.liu
 * @Date 2025/4/17.14:37
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcessDwd {
    // 来源表名
    String sourceTable;

    // 来源类型
    String sourceType;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 配置表操作类型
    String op;
}
