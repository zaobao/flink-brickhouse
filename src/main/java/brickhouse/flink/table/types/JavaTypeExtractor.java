package brickhouse.flink.table.types;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.lang.reflect.Type;
import java.util.List;

public class JavaTypeExtractor {

    public static Type fromDataType(DataType dataType) {
        Class<?> clazz = dataType.getConversionClass();
        if (clazz.equals(Row.class) || clazz.isArray()) {
            return clazz;
        }
        List<DataType> childTypes = dataType.getChildren();
        if (CollectionUtils.isEmpty(childTypes)) {
            return clazz;
        }
        return TypeUtils.parameterize(clazz,
                childTypes.stream().map(JavaTypeExtractor::fromDataType).toArray(Type[]::new));
    }
}
