package brickhouse.flink.functions.table;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.Expressions.$;

public class ExplodeFunctionTest {

    @Test
    void testBigIntArray() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Lists.newArrayList(
                                        Row.of(1, 2, 3),
                                        Row.of(2, 3, 3),
                                        Row.of(null, 3, 31241234))),
                        $("id1"),
                        $("id2"),
                        $("id"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("t", t);
        tEnv.createFunction("explode", ExplodeFunction.class);
        tEnv.executeSql(
                        "select id, exploded_id from t, lateral table(explode(ARRAY[CAST(id1 AS BIGINT), CAST(id2 AS BIGINT)])) AS exploded(exploded_id)")
                .print();
    }

}
