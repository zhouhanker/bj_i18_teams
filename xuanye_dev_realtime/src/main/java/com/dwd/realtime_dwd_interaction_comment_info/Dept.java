package com.dwd.realtime_dwd_interaction_comment_info;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Dept {
    Integer deptno;
    String dname;
    Long ts;
}
