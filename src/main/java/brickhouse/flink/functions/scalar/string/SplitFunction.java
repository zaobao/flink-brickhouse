package brickhouse.flink.functions.scalar.string;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class SplitFunction extends ScalarFunction {

    public String[] eval(String str) {
        return eval(str, ",");
    }

    public String[] eval(String str, String separator) {
        if (StringUtils.isEmpty(str)) {
            return new String[] {};
        }
        return StringUtils.split(str, separator);
    }

}
