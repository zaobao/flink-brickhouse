package brickhouse.flink.functions.scalar.collection;

import brickhouse.flink.utils.ComparisonUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;

import java.lang.reflect.Array;
import java.util.*;

public class ArrayUnionFunction extends ScalarFunction {

    private DataType elementDataType;

    public Object eval(Object... arrays) {
        if (arrays == null) {
            return null;
        }
        List set = new ArrayList() {};
        for (Object array : arrays) {
            if (array == null) {
                continue;
            }
            int length = Array.getLength(array);
            i_array: for (int i = 0; i < length; i++) {
                Object newObj = Array.get(array, i);
                for (int j = 0; j < set.size(); j++) {
                    if (ComparisonUtils.equals(set.get(j), newObj)) {
                        continue i_array;
                    }
                }
                set.add(newObj);
            }
        }
        Object array = Array.newInstance(elementDataType.getConversionClass(), set.size());
        int i = 0;
        for (Object key : set) {
            Array.set(array, i++, key);
        }
        return array;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.varyingSequence(InputTypeStrategies.ANY))
                .outputTypeStrategy(callContext -> {
                    final DataType dataType = callContext.getArgumentDataTypes().get(0);
                    final int argNum = callContext.getArgumentDataTypes().size();
                    elementDataType = ((CollectionDataType)dataType).getElementDataType();
                    if (argNum == 1) {
                        elementDataType = ((CollectionDataType)elementDataType).getElementDataType();
                    }
                    return Optional.of(DataTypes.ARRAY(elementDataType));
                }).build();
    }
}
