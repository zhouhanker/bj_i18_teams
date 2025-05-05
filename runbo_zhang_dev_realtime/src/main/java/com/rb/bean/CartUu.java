package com.rb.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.rb.bean.CartUu
 * @Author runbo.zhang
 * @Date 2025/4/15 22:00
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CartUu {

    private String start;
    private String end;
    private String curDate;
    private Long count;
}
