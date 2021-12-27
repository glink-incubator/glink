package cn.edu.whu.glink.core.enums;

/**
 * Enumeration of topology type.
 *
 * @author Yu Liebing
 * */
public enum TopologyType {

  CONTAINS,
  CROSSES,
  DISJOINT,
  EQUAL,
  INTERSECTS,
  OVERLAPS,
  TOUCH,
  WITHIN,

  WITHIN_DISTANCE,
  BUFFER,

  P_CONTAINS,
  N_CONTAINS,

  P_WITHIN,
  N_WITHIN,

  P_BUFFER,
  N_BUFFER;

  private double distance;

  public TopologyType distance(double distance) {
    if (this != WITHIN_DISTANCE)
      throw new IllegalArgumentException("Only WITHIN_DISTANCE type can assign distance");

    this.distance = distance;
    return this;
  }

  public double getDistance() {
    if (this != WITHIN_DISTANCE)
      throw new IllegalArgumentException("Only WITHIN_DISTANCE type can get distance");

    return distance;
  }
}
