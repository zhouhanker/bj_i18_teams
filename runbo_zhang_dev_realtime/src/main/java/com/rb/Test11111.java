package com.rb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Package com.rb.Test11111
 * @Author runbo.zhang
 * @Date 2025/4/21 15:29
 * @description:
 */
public class Test11111 {
    public static void main(String[] args) {
        // 创建 Properties 对象
        Properties properties = new Properties();
        try (InputStream inputStream = Test11111.class.getClassLoader().getResourceAsStream("log4j.properties")) {
            if (inputStream != null) {
                // 加载属性文件
                properties.load(inputStream);
                // 遍历并输出所有属性
                for (String key : properties.stringPropertyNames()) {
                    String value = properties.getProperty(key);
                    System.out.println(key + " = " + value);
                }
            } else {
                System.err.println("未找到 log4j.properties 文件");
            }
        } catch (IOException e) {
            System.err.println("读取 log4j.properties 文件时出错: " + e.getMessage());
        }
    }
}
