package brickhouse.flink.functions.scalar.json;

import brickhouse.flink.table.types.JavaTypeExtractor;
import brickhouse.flink.utils.JSONUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;

import java.lang.reflect.Type;
import java.util.Locale;
import java.util.Optional;

public class FromJsonFunction extends ScalarFunction {

    private DataType dataType;
    private transient Type extractedType; // why flink can't serialize Type ?

    public Object eval(String json, Object obj) {
        if (obj == null) {
            return null;
        }
        if (extractedType == null) {
            extractedType = JavaTypeExtractor.fromDataType(dataType);
        }
        return JSONUtils.fromJson(json, extractedType);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(
                        InputTypeStrategies.explicit(DataTypes.STRING()), InputTypeStrategies.ANY))
                .outputTypeStrategy(callContext -> {
                    final DataType dataType = callContext.getArgumentDataTypes().get(1);
                    FromJsonFunction.this.dataType = dataType;
                    return Optional.of(dataType);
                }).build();
    }


}
