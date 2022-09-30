package brickhouse.flink.functions.scalar.string;

import brickhouse.flink.functions.scalar.json.ToJsonFunction;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.Expressions.$;

public class SplitFunctionTest {

    @Test
    public void test() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Lists.newArrayList(
                                        Row.of(1, "1,2,3", "4|5|6"),
                                        Row.of(2, "1,,3,", null),
                                        Row.of(3, null, ""))),
                        $("id"),
                        $("str1"),
                        $("str2"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("t", t);
        tEnv.createFunction("split", SplitFunction.class);
        tEnv.createFunction("to_json", ToJsonFunction.class);

        tEnv.executeSql(
                        "select id, to_json(split(str1)) AS str1_array, to_json(split(str2, '|')) AS str2_array from t")
                .print();
    }
}
