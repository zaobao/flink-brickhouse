package brickhouse.flink.functions.scalar.collection;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.Expressions.$;

public class ArrayContainsFunctionTest {

    @Test
    void testNull() {
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
        tEnv.createFunction("ARRAY_CONTAINS", ArrayContainsFunction.class);

        tEnv.executeSql(
                        "select ARRAY_CONTAINS(null, id) AS contains_id from t")
                .print();
    }

    @Test
    void testIntArray() {
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
        tEnv.createFunction("ARRAY_CONTAINS", ArrayContainsFunction.class);

        tEnv.executeSql(
                        "select id1, id2, ARRAY[id1, id2] AS ids, id, ARRAY_CONTAINS(ARRAY[id1, id2], id) AS contains_id from t")
                .print();
    }

    @Test
    void testStringArray() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Lists.newArrayList(
                                        Row.of("a", "b", "c"),
                                        Row.of("a", "b", "a"),
                                        Row.of(null, "b", null))),
                        $("id1"),
                        $("id2"),
                        $("id"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("t", t);
        tEnv.createFunction("ARRAY_CONTAINS", ArrayContainsFunction.class);

        tEnv.executeSql(
                        "select id1, id2, ARRAY[id1, id2] AS ids, id, ARRAY_CONTAINS(ARRAY[id1, id2], id) AS contains_id from t")
                .print();
    }

    @Test
    void testNestedArray() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Lists.newArrayList(
                                        Row.of("a", "b", "c"),
                                        Row.of("a", "b", "a"),
                                        Row.of(null, "b", null))),
                        $("id1"),
                        $("id2"),
                        $("id3"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("t", t);
        tEnv.createFunction("ARRAY_CONTAINS", ArrayContainsFunction.class);

        tEnv.executeSql(
                        "select ARRAY[ARRAY[id1, id2], ARRAY[id1, id3]] AS ids, ARRAY_CONTAINS(ARRAY[ARRAY[id1, id2], ARRAY[id1, id3]], ARRAY[id1, id3]) AS contains_id, ARRAY[id1, id2] = ARRAY[id1, id2] AS equals_test from t")
                .print();
    }
}
