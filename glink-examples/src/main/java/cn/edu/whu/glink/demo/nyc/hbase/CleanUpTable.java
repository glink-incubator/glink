package cn.edu.whu.glink.demo.nyc.hbase;

/**
 * @author Xu Qi
 */
public class CleanUpTable {
  public static final String CATALOG_NAME = "Xiamen";
  public static final String TILE_SCHEMA_NAME = "Heatmap";
  public static final String POINTS_SCHEMA_NAME = "JoinedPoints";

  public static void main(String[] args) {
    // Drop old tables in HBase
    new HBaseCatalogCleaner(HeatmapSinkHbase.ZOOKEEPERS).deleteTable(CATALOG_NAME, TILE_SCHEMA_NAME);
    new HBaseCatalogCleaner(HeatmapSinkHbase.ZOOKEEPERS).deleteTable(CATALOG_NAME, POINTS_SCHEMA_NAME);
  }
}
