package com.zgq.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.zgq.bean.TableProcessDwd
 * @Author  guoqiang.zhang
 * @Date  2025/5/4 13:40
 * @description: 
*/

@Data
@NoArgsConstructor
@AllArgsConstructor
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