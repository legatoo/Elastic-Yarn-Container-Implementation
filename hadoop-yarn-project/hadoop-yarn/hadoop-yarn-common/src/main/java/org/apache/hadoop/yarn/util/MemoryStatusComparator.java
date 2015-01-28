package org.apache.hadoop.yarn.util;

import org.apache.hadoop.yarn.api.records.ContainerMemoryStatus;

import java.util.Comparator;

/**
 * Created by steven on 1/27/15.
 */
public class MemoryStatusComparator implements Comparator<ContainerMemoryStatus> {

    @Override
    public int compare(ContainerMemoryStatus o1, ContainerMemoryStatus o2) {
        // priority queue will use this comparator, the minimum memory usage will
        // be at top
        if ( o1.getPhysicalMemUsage()< o2.getPhysicalMemUsage())
            return -1;
        if ( o1.getPhysicalMemUsage() > o2.getPhysicalMemUsage())
            return 1;
        return 0;
    }
}
