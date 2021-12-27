package cn.edu.whu.glink.examples.sql;

import cn.edu.whu.glink.core.serialize.GlinkSerializerRegister;
import cn.edu.whu.glink.sql.GlinkSQLRegister;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * A simple example shows how to do spatial filter with glink sql.
 *
 * <p>The input is a text file. <code>resources/data/example_point.txt</code> is an example.
 * @author Yu Liebing
 */
public class SpatialFilterExample {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    String path = params.get("path");

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    GlinkSerializerRegister.registerSerializer(env);
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // register a table in the catalog
    tEnv.executeSql(
            "CREATE TABLE Points (\n" +
                    " id STRING,\n" +
                    " x DOUBLE,\n" +
                    " y DOUBLE\n" +
                    ") WITH (\n" +
                    " 'connector' = 'filesystem',\n" +
                    " 'path' = 'file://" + path + "',\n" +
                    " 'format' = 'csv'\n" +
                    ")");

    // define a dynamic query
    final Table result = tEnv.sqlQuery("SELECT * FROM Points WHERE " +
            "ST_Contains(" +
            "ST_GeomFromText('POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10))'), ST_Point(x, y)" +
            ")");

    // print the result to the console
    tEnv.toAppendStream(result, Row.class).print();

    env.execute("Spatial SQL Filter Example");
  }
}
