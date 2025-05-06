package com.dwd.realtime_dwd_interaction_comment_info;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Emp {
    Integer empno;
    String ename;
    Integer deptno;
    Long ts;
}
