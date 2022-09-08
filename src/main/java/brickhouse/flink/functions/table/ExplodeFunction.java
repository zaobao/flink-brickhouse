package brickhouse.flink.functions.table;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;

import java.lang.reflect.Array;
import java.util.Optional;

public class ExplodeFunction extends TableFunction {

    public void eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object array) {
        if (array != null) {
            int length = Array.getLength(array);
            for (int i = 0; i < length; i++) {
                collect(Array.get(array, i));
            }
        }
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(InputTypeStrategies.ANY))
                .outputTypeStrategy(callContext -> {
                    final DataType dataType = callContext.getArgumentDataTypes().get(0);
                    return Optional.ofNullable(((CollectionDataType) dataType).getElementDataType());
                }).build();
    }

}
