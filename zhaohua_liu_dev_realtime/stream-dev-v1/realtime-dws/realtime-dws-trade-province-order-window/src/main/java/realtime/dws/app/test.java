package realtime.dws.app;

import cn.hutool.core.collection.ListUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package realtime.dws.app.test
 * @Author zhaohua.liu
 * @Date 2025/4/26.9:57
 * @description:
 */
public class test {
    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<>();
        list.add("苹果");
        list.add("香蕉");
        System.out.println(list);
        ArrayList<Serializable> aaa = ListUtil.toList(list, "榴莲");
        aaa.addAll(list);
        System.out.println(aaa);
    }
}
