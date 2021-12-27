package cn.edu.whu.glink.sql.udf.standard.relationship;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

@SuppressWarnings("checkstyle:TypeName")
public class ST_Transform extends ScalarFunction {

  private transient String preSourceEpsgCRSCode;
  private transient String preTargetEpsgCRSCode;
  private transient MathTransform transform;

  public @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Geometry eval(
          @DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Geometry geom,
          String sourceEpsgCRSCode, String targetEpsgCRSCode)
          throws Exception {
    if (sourceEpsgCRSCode.equals(preSourceEpsgCRSCode) && targetEpsgCRSCode.equals(preTargetEpsgCRSCode)) {
      if (transform == null) {
        CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
        CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
        transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
      }
    } else {
      preSourceEpsgCRSCode = sourceEpsgCRSCode;
      preTargetEpsgCRSCode = targetEpsgCRSCode;
      CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
      CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
      transform = CRS.findMathTransform(sourceCRS, targetCRS, false);
    }
    return JTS.transform(geom, transform);
  }
}
