package io.github.spycsh.hesse.util;

import io.github.spycsh.hesse.types.VertexActivity;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class Utils {
  // recover direct neighbours without weight of a vertex by using its filtered activity log
  public static HashSet<String> recoverStateByLog(List<VertexActivity> activityLog) {
    HashSet<String> neighbourIds = new HashSet<>();
    for (VertexActivity activity : activityLog) {
      if (activity.getActivityType().equals("add") && !activity.isIngoing()) {
        neighbourIds.add(activity.getDstId());
      }
    }
    return neighbourIds;
  }

  // recover out-going direct neighbours with weight of a vertex by using its filtered activity log
  // if the input vertex activities do not have weights, let the weight of every edge be 1.0
  public static HashMap<String, String> recoverWeightedStateByLog(
      List<VertexActivity> activityLog) {
    HashMap<String, String> neighbourIdsWithWeight = new HashMap<>();
    for (VertexActivity activity : activityLog) {
      if (activity.getActivityType().equals("add") && !activity.isIngoing()) {
        if (activity.getWeight() != null) {
          neighbourIdsWithWeight.put(activity.getDstId(), activity.getWeight());
        } else {
          neighbourIdsWithWeight.put(activity.getDstId(), "1.0");
        }
      }
    }
    return neighbourIdsWithWeight;
  }

  // recover in-degree with filtered activity log
  public static int recoverInDegreeByLog(List<VertexActivity> activityLog) {
    HashSet<String> inGoingNeighbours = new HashSet<>();

    for (VertexActivity activity : activityLog) {
      if (activity.getActivityType().equals("add") && activity.isIngoing()) {
        inGoingNeighbours.add(activity.getSrcId());
      }
    }
    return inGoingNeighbours.size();
  }

  public static int generateNewStackHash(ArrayDeque<String> stack) {
    StringBuilder sb = new StringBuilder();
    for (String s : stack) {
      sb.append(s).append(" ");
    }
    return sb.toString().hashCode();
  }
}
