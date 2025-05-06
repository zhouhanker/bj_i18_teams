package com.flink.realtime.dwd.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @version 1.0
 * @Package com.flink.realtime.dwd.test.Emp
 * @Author liu.bo
 * @Date 2025/5/3 16:28
 * @description: 员工
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Emp {
    Integer empno;
    String ename;
    Integer deptno;
    Long ts;
}
