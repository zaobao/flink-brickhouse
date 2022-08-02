package brickhouse.flink.functions.scalar.collection;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;
import java.lang.reflect.Array;

public class ArrayContainsFunction extends ScalarFunction {

    public @Nullable Boolean eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object haystack,
                                  @DataTypeHint(inputGroup = InputGroup.ANY) Object needle) {
        try {
            if (haystack == null) {
                return null;
            }
            Class<?> setClass = haystack.getClass();
            if (setClass.isArray()) {
                int length = Array.getLength(haystack);
                for (int i = 0; i < length; i++) {
                    if (isEqual(Array.get(haystack, i), needle)) {
                        return true;
                    }
                }
            }
            return false;
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    boolean isEqual(Object a, Object b) {
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
        return b.equals(a);
    }

}
