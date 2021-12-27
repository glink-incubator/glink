package cn.edu.whu.glink.examples.sql.geomesa;

import cn.edu.whu.glink.core.serialize.GlinkSerializerRegister;
import cn.edu.whu.glink.sql.GlinkSQLRegister;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * A simple example of how to use glink sql to perform spatial dimension join.
 *
 * <p>How to run the example:
 * <ul>
 *   <li>
 *     Create a geomesa table with cmd:
 *     <code>
 *       geomesa-hbase create-schema -c beijing-district -s "pid:String,name:String,area:Polygon" -f beijing-district
 *     </code>
 *   </li>
 *   <li>
 *     Ingest the beijing district data into geomesa:
 *     <code>
 *       geomesa-hbase ingest -c beijing-district -f beijing-district -C /resources/data/beijing_district.conf /resources/data/beijing_district.csv
 *     </code>
 *   </li>
 *   <li>Run this class</li>
 * </ul>
 *
 * @author Yu Liebing
 * */
public class GeoMesaSQLJoinExample {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) throws Exception {
    ParameterTool params = ParameterTool.fromArgs(args);
    String csvPath = params.get("path");

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    GlinkSerializerRegister.registerSerializer(env);
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // create a source table from csv
    tEnv.executeSql(
            "CREATE TABLE CSV_TDrive (\n" +
                    "id STRING,\n" +
                    "dtg TIMESTAMP(0),\n" +
                    "lng DOUBLE,\n" +
                    "lat DOUBLE," +
                    "proctime AS PROCTIME())\n" +
                    "WITH (\n" +
                    "  'connector' = 'filesystem',\n" +
                    "  'path' = '" + csvPath + "',\n" +
                    "  'format' = 'csv'\n" +
                    ")");

    // register a table in the catalog
    tEnv.executeSql(
            "CREATE TABLE beijing_district (\n" +
                    "    pid STRING,\n" +
                    "    name STRING,\n" +
                    "    area STRING,\n" +
                    "    PRIMARY KEY (pid) NOT ENFORCED)\n" +
                    "WITH (\n" +
                    "    'connector' = 'geomesa',\n" +
                    "    'geomesa.data.store' = 'hbase',\n" +
                    "    'geomesa.schema.name' = 'beijing-district',\n" +
                    "    'geomesa.spatial.fields' = 'area:Polygon',\n" +
                    "    'geomesa.temporal.join.predict' = 'I',\n" +
                    "    'hbase.zookeepers' = 'localhost:2181',\n" +
                    "    'hbase.catalog' = 'beijing-district'\n" +
                    ")");

    Table result = tEnv.sqlQuery("SELECT A.id AS point_id, A.dtg, ST_AsText(ST_Point(A.lng, A.lat)) AS point, B.pid AS area_id\n" +
            "    FROM CSV_TDrive AS A\n" +
            "    LEFT JOIN beijing_district FOR SYSTEM_TIME AS OF A.proctime AS B\n" +
            "    ON ST_AsText(ST_Point(A.lng, A.lat)) = B.area");

    tEnv.toAppendStream(result, Row.class).print();

    env.execute("GeoMesa SQL Join Example");
  }
}
