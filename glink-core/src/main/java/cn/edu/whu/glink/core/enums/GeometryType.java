package cn.edu.whu.glink.core.enums;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.locationtech.jts.geom.*;

import java.io.Serializable;

/**
 * @author Yu Liebing
 */
public enum GeometryType implements Serializable {

  POINT,
  POLYGON,
  LINESTRING,
  MULTIPOINT,
  MULTIPOLYGON,
  MULTILINESTRING,
  GEOMETRYCOLLECTION,
  CIRCLE,
  RECTANGLE;

  public <T extends Geometry> TypeInformation<T> getTypeInformation() {
    switch (this) {
      case POINT:
        return (TypeInformation<T>) TypeInformation.of(Point.class);
      case POLYGON:
        return (TypeInformation<T>) TypeInformation.of(Polygon.class);
      case LINESTRING:
        return (TypeInformation<T>) TypeInformation.of(LineString.class);
      case MULTIPOINT:
        return (TypeInformation<T>) TypeInformation.of(MultiPoint.class);
      case MULTIPOLYGON:
        return (TypeInformation<T>) TypeInformation.of(MultiPolygon.class);
      case MULTILINESTRING:
        return (TypeInformation<T>) TypeInformation.of(MultiLineString.class);
      case GEOMETRYCOLLECTION:
        return (TypeInformation<T>) TypeInformation.of(GeometryCollection.class);
      case CIRCLE:
        // TODO
        return null;
      case RECTANGLE:
        // TODO
        return null;
      default:
        return null;
    }
  }

  /**
   * Gets the GeometryType.
   *
   * @param str the str
   * @return the GeometryType
   */
  public static GeometryType getGeometryType(String str) {
    for (GeometryType me : GeometryType.values()) {
      if (me.name().equalsIgnoreCase(str)) {
        return me;
      }
    }
    throw new IllegalArgumentException("[" + GeometryType.class + "] Unsupported geometry type:" + str);
  }
}
