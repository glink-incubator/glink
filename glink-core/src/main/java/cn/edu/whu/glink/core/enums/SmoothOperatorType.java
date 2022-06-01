package cn.edu.whu.glink.core.enums;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Xu Qi
 */
public enum SmoothOperatorType {
  GaussianBlur,
  MaRixNull;
  private static int length;

  public static int getLength() {
    return length;
  }

  public static Map<Tuple2<Integer,Integer>,Double> getMapOperator(SmoothOperatorType smoothoperatorType) {
    HashMap<Tuple2<Integer,Integer>,Double> hashMap = new HashMap<>();
    switch (smoothoperatorType) {
      case GaussianBlur:
        length = 5;
        for (int i = -2; i <= 2; i++) {
          for (int j = -2; j <= 2; j++) {
            double distance = Math.pow(i, 2) + Math.pow(j, 2);
            if (distance == 0) {
              hashMap.put(new Tuple2<>(i, j), 0.44);
            } else if (distance == 1) {
              hashMap.put(new Tuple2<>(i, j), 0.11);
            } else if (distance == 2) {
              hashMap.put(new Tuple2<>(i, j), 0.03);
            } else if (distance == 4) {
              hashMap.put(new Tuple2<>(i, j), 2 * Math.pow(10, -3));
            } else if (distance == 5) {
              hashMap.put(new Tuple2<>(i, j), 4 * Math.pow(10, -4));
            } else if (distance == 8) {
              hashMap.put(new Tuple2<>(i, j), 7 * Math.pow(10, -6));
            }
          }
        }
        return hashMap;
      case MaRixNull:
      default:
        throw new IllegalArgumentException("Illegal SmoothOperator type");
    }
  }
}
