package cn.edu.whu.glink.demo.nyc.hbase;

import cn.edu.whu.glink.connector.geomesa.sink.GeoMesaSimpleFeatureConverter;
import cn.edu.whu.glink.core.tile.TileResult;

/**
 * @author Xu Qi
 */
public interface TileResultToSimpleFeatureConverter extends GeoMesaSimpleFeatureConverter<TileResult<Double>> {

  void resultListToOutputFormat(TileResult<Double> result);

}
