package io.github.spycsh.hesse.util;

import java.util.Comparator;

public class CustomizedComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
        String s1 = (String) o1;
        String s2 = (String) o2;
        Integer i1, i2;
        if(s1.startsWith("bucket")){
            String ss1 = s1.replace("bucket", "");
            String ss2 = s2.replace("bucket", "");
            i1 = Integer.parseInt(ss1);
            i2 = Integer.parseInt(ss2);
        } else{
            i1 = Integer.parseInt((String) o1);
            i2 = Integer.parseInt((String) o2);
        }

        return i1.compareTo(i2);
    }
}
