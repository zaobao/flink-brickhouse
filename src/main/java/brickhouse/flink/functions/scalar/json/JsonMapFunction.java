package brickhouse.flink.functions.scalar.json;

import brickhouse.flink.utils.JSONUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.List;
import java.util.Map;

public class JsonMapFunction extends ScalarFunction {

    public Map<String, String> eval(String jsonStr) {
        if (jsonStr == null) {
            return null;
        }
        return JSONUtils.toJsonMap(jsonStr);
    }

}
