package cn.edu.whu.glink.sql;

import cn.edu.whu.glink.sql.udf.standard.constructor.*;
import cn.edu.whu.glink.sql.udf.standard.output.ST_AsText;
import cn.edu.whu.glink.sql.udf.standard.relationship.*;
import cn.edu.whu.glink.sql.udf.standard.output.ST_AsBinary;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Yu Liebing
 */
public class GlinkSQLRegister {

  public static void registerUDF(StreamTableEnvironment tEnv) {
    // geometry constructors
    tEnv.registerFunction("ST_GeomFromText", new ST_GeomFromText());
    tEnv.registerFunction("ST_GeomFromWKB", new ST_GeomFromWKB());
    tEnv.registerFunction("ST_GeomFromWKT", new ST_GeomFromWKT());
    tEnv.registerFunction("ST_LineFromText", new ST_LineFromText());
    tEnv.registerFunction("ST_MakeLine", new ST_MakeLine());
    tEnv.registerFunction("ST_MakePoint", new ST_MakePoint());
    tEnv.registerFunction("ST_MakePointM", new ST_MakePointM());
    tEnv.registerFunction("ST_MakePolygon", new ST_MakePolygon());
    tEnv.registerFunction("ST_MLineFromText", new ST_MLineFromText());
    tEnv.registerFunction("ST_MPointFromText", new ST_MPointFromText());
    tEnv.registerFunction("ST_MPolyFromText", new ST_MPolyFromText());
    tEnv.registerFunction("ST_Point", new ST_Point());
    tEnv.registerFunction("ST_PointFromText", new ST_PointFromText());
    tEnv.registerFunction("ST_PointFromWKB", new ST_PointFromWKB());
    tEnv.registerFunction("ST_Polygon", new ST_Polygon());
    tEnv.registerFunction("ST_PolygonFromText", new ST_PolygonFromText());

    // geometry outputs
    tEnv.registerFunction("ST_AaBinary", new ST_AsBinary());
    tEnv.registerFunction("ST_AsText", new ST_AsText());

    // geometry relationships
    tEnv.registerFunction("ST_Contains", new ST_Contains());
    tEnv.registerFunction("ST_Covers", new ST_Covers());
    tEnv.registerFunction("ST_Disjoint", new ST_Disjoint());
    tEnv.registerFunction("ST_Equals", new ST_Equals());
    tEnv.registerFunction("ST_Intersects", new ST_Intersects());
    tEnv.registerFunction("ST_Overlaps", new ST_Overlaps());
    tEnv.registerFunction("ST_Touches", new ST_Touches());
    tEnv.registerFunction("ST_Transform", new ST_Transform());
    tEnv.registerFunction("ST_Within", new ST_Within());
  }
}
