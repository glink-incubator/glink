package cn.edu.whu.glink.connector.geomesa.util;

/**
 * Enumeration of temporal table join predicts.
 *
 * @author Yu Liebing
 * */
public enum TemporalJoinPredict {
  RADIUS("R"),
  INTERSECTS("I"),
  P_CONTAINS("+C"),
  N_CONTAINS("-C");

  private final String name;

  TemporalJoinPredict(String name) {
    this.name = name;
  }

  public static TemporalJoinPredict getTemporalJoinPredict(String name) {
    TemporalJoinPredict[] predicts = TemporalJoinPredict.values();
    for (TemporalJoinPredict predict : predicts) {
      if (predict.name.equals(name)) return predict;
    }
    throw new IllegalArgumentException("Unsupported predict type: " + name);
  }
}
