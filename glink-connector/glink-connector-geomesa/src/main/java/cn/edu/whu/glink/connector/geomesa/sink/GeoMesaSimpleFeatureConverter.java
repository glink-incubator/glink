package cn.edu.whu.glink.connector.geomesa.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;

import java.io.Serializable;

/**
 * A converter used to convert the input record into GeoMesa
 * {@link org.opengis.feature.simple.SimpleFeature}.
 * @param <T> type of input record.
 *
 * @author Yu Liebing
 */
@PublicEvolving
public interface GeoMesaSimpleFeatureConverter<T> extends Serializable {

  /**
   * Initialization method for the function. It is called once before conversion method.
   */
  void open();

  /**
   * Converts the input record into Geomesa {@link SimpleFeature}.
   */
  SimpleFeature convertToSimpleFeature(T record);
}
