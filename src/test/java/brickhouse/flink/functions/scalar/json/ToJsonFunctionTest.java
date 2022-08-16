package brickhouse.flink.functions.scalar.json;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.Expressions.$;

public class ToJsonFunctionTest {

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
        tEnv.executeSql(
                        "select id1, id2, to_json(id) as id_json, to_json(ARRAY[CAST(id1 AS BIGINT), CAST(id2 AS BIGINT)]) AS ids_json from t")
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

        tEnv.executeSql(
                        "select id1, id2, to_json(MAP['id1', id1, 'id2', id2]) AS id_map_json from t")
                .print();
    }

    @Test
    void testBigIntRow() {
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

        tEnv.executeSql(
                        "select id1, id2, unnamed_row, named_row, to_json(unnamed_row), to_json(named_row) from (" +
                                "select id1, id2, ROW(CAST(id1 AS BIGINT), CAST(id2 AS BIGINT)) AS unnamed_row, CAST((id1, id2) AS ROW<id1 INT, id2 INT>) AS named_row from t" +
                                ") t")
                .print();
    }

    @Test
    void testTimestamp() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Lists.newArrayList(
                                        Row.of(TimestampData.fromEpochMillis(0), TimestampData.fromEpochMillis(1)),
                                        Row.of(TimestampData.fromEpochMillis(1), TimestampData.fromEpochMillis(2)),
                                        Row.of(null, TimestampData.fromEpochMillis(3)))),
                        $("timestamp1"),
                        $("timestamp2"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("t", t);
        tEnv.createFunction("to_json", ToJsonFunction.class);

        tEnv.executeSql(
                        "select timestamp1, to_json(timestamp1) AS timestamp1_json, timestamp2, to_json(timestamp2) AS timestamp2_json, to_json(NOW()) AS now_json, to_json(TIMESTAMP '1994-09-27 13:14:15') AS constant_timestamp_json from t")
                .print();
    }

}
