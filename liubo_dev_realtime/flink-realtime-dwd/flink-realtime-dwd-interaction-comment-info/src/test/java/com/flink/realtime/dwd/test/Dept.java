package com.flink.realtime.dwd.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ version 1.0
 * @ Package com.flink.realtime.dwd.test.Dept
 * @ Author liu.bo
 * @ Date 2025/5/3 16:28
 * @ description: 部门
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Dept {
    Integer deptno;
    String dname;
    Long ts;
}
