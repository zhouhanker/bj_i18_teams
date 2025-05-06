import com.struggle.flink.realtime.common.util.Static_Test;

/**
 * @version 1.0
 * @Package PACKAGE_NAME.Static_Test2
 * @Author liu.bo
 * @Date 2025/5/4 14:36
 * @description: 静态方法测试
 */
public class Static_Test2 {
    public Integer e() {
        return Static_Test.b(2, 3);
    }

    public static void main(String[] args) {
        Static_Test2 a = new Static_Test2();
        Integer e = a.e();
        System.out.println(e);
    }
}
