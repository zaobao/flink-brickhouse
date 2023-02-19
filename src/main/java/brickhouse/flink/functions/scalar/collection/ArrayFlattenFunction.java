package brickhouse.flink.functions.scalar.collection;

import brickhouse.flink.utils.ComparisonUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ArrayFlattenFunction extends ScalarFunction {

    private DataType elementDataType;

    public Object eval(Object[] nestedArray) {
        if (nestedArray == null) {
            return null;
        }
        List list = new ArrayList() {};
        for (Object array : nestedArray) {
            if (array == null) {
                continue;
            }
            int length = Array.getLength(array);
            for (int i = 0; i < length; i++) {
                list.add(Array.get(array, i));
            }
        }
        Object array = Array.newInstance(elementDataType.getConversionClass(), list.size());
        int i = 0;
        for (Object key : list) {
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
                    elementDataType = ((CollectionDataType)dataType).getElementDataType();
                    elementDataType = ((CollectionDataType)elementDataType).getElementDataType();
                    return Optional.of(DataTypes.ARRAY(elementDataType));
                }).build();
    }
}
