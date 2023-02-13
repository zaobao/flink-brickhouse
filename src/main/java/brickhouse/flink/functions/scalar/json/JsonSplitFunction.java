package brickhouse.flink.functions.scalar.json;

import brickhouse.flink.utils.JSONUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.List;

public class JsonSplitFunction extends ScalarFunction {

    public String[] eval(String jsonStr) {
        if (jsonStr == null) {
            return null;
        }
        List<String> result = JSONUtils.splitJsonArray(jsonStr);
        if (result == null) {
            return null;
        }
        return result.toArray(new String[result.size()]);
    }

}
