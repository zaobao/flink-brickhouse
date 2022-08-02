package brickhouse.flink.functions.scalar.json;

import brickhouse.flink.utils.JSONUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;

public class ToJsonFunction extends ScalarFunction {

    public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object obj) {
        return JSONUtils.toJson(obj);
    }
}
