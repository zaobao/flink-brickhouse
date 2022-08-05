package brickhouse.flink.functions.scalar;

import brickhouse.flink.functions.scalar.json.FromJsonFunction;
import brickhouse.flink.functions.scalar.json.ToJsonFunction;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.Expressions.$;

public class FromJsonFuntionTest {

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
        tEnv.createFunction("to_json", ToJsonFunction.class);
        tEnv.createFunction("from_json", FromJsonFunction.class);
        tEnv.executeSql(
                        "select from_json(to_json(ARRAY[CAST(id1 AS BIGINT), CAST(id2 AS BIGINT)]), ARRAY[CAST(1 AS BIGINT)]) AS ids from t")
                .print();
    }

    @Test
    void testMap() {
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
        tEnv.createFunction("to_json", ToJsonFunction.class);
        tEnv.createFunction("from_json", FromJsonFunction.class);

        tEnv.executeSql(
                        "select from_json(to_json(MAP['id1', id1, 'id2', id2]), MAP['', 1]) AS id_map, from_json(to_json(MAP['id1', id1, 'id2', id2]), MAP['', 1])['id2'] AS id2_from_map from t")
                .print();
    }
}
