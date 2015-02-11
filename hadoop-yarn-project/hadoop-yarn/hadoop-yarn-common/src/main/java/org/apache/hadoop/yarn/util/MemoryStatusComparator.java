package org.apache.hadoop.yarn.util;

import org.apache.hadoop.yarn.api.records.ContainerMemoryStatus;
import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;

import java.util.Comparator;

/**
 * Created by steven on 1/27/15.
 */
public class MemoryStatusComparator implements Comparator<ContainerSqueezeUnit> {

    @Override
    public int compare(ContainerSqueezeUnit o1, ContainerSqueezeUnit o2) {
        // priority queue will use this comparator, the minimum memory usage will
        // be at top
        if ( o1.getPriority()< o2.getPriority())
            return -1;
        if ( o1.getPriority() > o2.getPriority())
            return 1;
        return 0;
    }
}
