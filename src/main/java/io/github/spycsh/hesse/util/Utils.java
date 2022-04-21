package io.github.spycsh.hesse.util;

import io.github.spycsh.hesse.types.VertexActivity;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Utils {
    public static HashSet<String> recoverStateByTimeRegion(List<VertexActivity> activityLog) {
        HashSet<String> neighbourIds = new HashSet<>();
        for(VertexActivity activity:activityLog){
            if(activity.getActivityType().equals("add")) {
                // TODO now only do with unweighted graph, the state is all the neighbours at T
                if(activity.getWeight() == null){
                    neighbourIds.add(activity.getDstId());
                }
            }
        }
        return neighbourIds;
    }

    public static int generateNewStackHash(ArrayDeque<String> stack) {
        StringBuilder sb = new StringBuilder();
        for(String s:stack){
            sb.append(s).append(" ");
        }
        return sb.toString().hashCode();
    }
}
