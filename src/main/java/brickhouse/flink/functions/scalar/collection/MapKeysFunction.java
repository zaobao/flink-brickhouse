package brickhouse.flink.functions.scalar.collection;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;

import java.lang.reflect.Array;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MapKeysFunction extends ScalarFunction {

    private DataType keyDataType;

    public Object eval(Map obj) {
        Set set = obj.keySet();
        Object array = Array.newInstance(keyDataType.getConversionClass(), set.size());
        int i = 0;
        for (Object key : set) {
            Array.set(array, i++, key);
        }
        return array;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(InputTypeStrategies.ANY))
                .outputTypeStrategy(callContext -> {
                    final DataType dataType = callContext.getArgumentDataTypes().get(0);
                    keyDataType = ((KeyValueDataType)dataType).getKeyDataType();
                    return Optional.of(DataTypes.ARRAY(keyDataType));
                }).build();
    }
}
