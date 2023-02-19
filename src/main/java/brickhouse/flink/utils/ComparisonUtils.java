package brickhouse.flink.utils;

import java.util.Objects;

public class ComparisonUtils {

    public static boolean equals(Object a, Object b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Long && b instanceof Long) {
            return ((Long) a).longValue() == ((Long) b).longValue();
        }
        if (a instanceof Number && b instanceof Number) {
            return ((Number) a).doubleValue() == ((Number) b).doubleValue();
        }
        if (a.getClass().isArray() && b.getClass().isArray()) {
            return Objects.deepEquals(a, b);
        }
        return b.equals(a);
    }
}
