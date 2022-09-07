package brickhouse.flink.functions.scalar.collection;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MapValuesFunction extends ScalarFunction {

    private DataType valueDataType;

    public Object eval(Map obj) {
        Collection values = obj.values();
        Object array = Array.newInstance(valueDataType.getConversionClass(), values.size());
        int i = 0;
        for (Object value : values) {
            Array.set(array, i++, value);
        }
        return array;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(InputTypeStrategies.ANY))
                .outputTypeStrategy(callContext -> {
                    final DataType dataType = callContext.getArgumentDataTypes().get(0);
                    valueDataType = ((KeyValueDataType)dataType).getValueDataType();
                    return Optional.of(DataTypes.ARRAY(valueDataType));
                }).build();
    }
}
