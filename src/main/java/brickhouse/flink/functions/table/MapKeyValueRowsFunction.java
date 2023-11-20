package brickhouse.flink.functions.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MapKeyValueRowsFunction extends TableFunction {

    public void eval(Map map) {
        if (map != null) {
            Set<Map.Entry> set = map.entrySet();
            set.stream().forEach(entry -> collect(Row.of(entry.getKey(), entry.getValue())));
        }
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.sequence(InputTypeStrategies.ANY))
                .outputTypeStrategy(callContext -> {
                    final KeyValueDataType mapDataType = (KeyValueDataType) callContext.getArgumentDataTypes().get(0);
                    return Optional.ofNullable(DataTypes.ROW(mapDataType.getKeyDataType(), mapDataType.getValueDataType()));
                }).build();
    }

}
