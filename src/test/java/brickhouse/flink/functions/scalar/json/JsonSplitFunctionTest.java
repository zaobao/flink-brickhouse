package brickhouse.flink.functions.scalar.json;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.Expressions.$;

public class JsonSplitFunctionTest {

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
        tEnv.createFunction("to_json", ToJsonFunction.class);
        tEnv.createFunction("json_split", JsonSplitFunction.class);

        tEnv.executeSql(
                        "select to_json(json_split(null)) AS id_map_list_split_json from t")
                .print();
    }

    @Test
    void testNullJson() {
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
        tEnv.createFunction("json_split", JsonSplitFunction.class);

        tEnv.executeSql(
                        "select to_json(json_split(to_json(null))) AS id_map_list_split_json from t")
                .print();
    }

    @Test
    void testEmptyString() {
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
        tEnv.createFunction("json_split", JsonSplitFunction.class);

        tEnv.executeSql(
                        "select to_json(json_split('')) AS id_map_list_split_json from t")
                .print();
    }

    @Test
    void testNullInArray() {
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
        tEnv.createFunction("json_split", JsonSplitFunction.class);
        tEnv.executeSql(
                        "select to_json(json_split(to_json(ARRAY[null, CAST(id2 AS BIGINT)]))) AS split_ids_json from t")
                .print();
    }

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
        tEnv.createFunction("json_split", JsonSplitFunction.class);
        tEnv.executeSql(
                        "select to_json(json_split(to_json(ARRAY[CAST(id1 AS BIGINT), CAST(id2 AS BIGINT)]))) AS split_ids_json from t")
                .print();
    }

    @Test
    void testMapArray() {
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
        tEnv.createFunction("json_split", JsonSplitFunction.class);

        tEnv.executeSql(
                        "select to_json(json_split(to_json(ARRAY[MAP['id1', id1], MAP['id2', id2], MAP['id1', id1, 'id2', id2]]))) AS id_map_list_split_json from t")
                .print();
    }

    @Test
    void testMapNestedArray() {
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
        tEnv.createFunction("json_split", JsonSplitFunction.class);

        tEnv.executeSql(
                        "select to_json(json_split(to_json(ARRAY[ARRAY[MAP['id1', id1], MAP['id2', id2], MAP['id1', id1, 'id2', id2]]]))) AS id_map_list_split_json from t")
                .print();
    }

    @Test
    void testNestedMapArray() {
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
        tEnv.createFunction("json_split", JsonSplitFunction.class);

        tEnv.executeSql(
                        "select to_json(json_split('[{1:{\"2\":1}},{2:2},3]')) AS id_map_list_split_json from t")
                .print();
    }
}
