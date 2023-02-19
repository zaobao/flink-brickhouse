package brickhouse.flink.functions.scalar.collection;

import brickhouse.flink.utils.ComparisonUtils;
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
                return false;
            }
            Class<?> setClass = haystack.getClass();
            if (setClass.isArray()) {
                int length = Array.getLength(haystack);
                for (int i = 0; i < length; i++) {
                    if (ComparisonUtils.equals(Array.get(haystack, i), needle)) {
                        return true;
                    }
                }
            }
            return false;
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

}
