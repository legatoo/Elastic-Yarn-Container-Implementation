package org.apache.hadoop.yarn.server.resourcemanager.periodicservice;

import org.apache.hadoop.yarn.api.records.ContainerMemoryStatus;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.List;

/**
 * Created by steven on 1/20/15.
 */
public class PeriodicSchedulerStatusEvent extends PeriodicSchedulerEvent {
    private final List<ContainerMemoryStatus> containerMemoryStatuses;

    public PeriodicSchedulerStatusEvent(NodeId nodeId, List<ContainerMemoryStatus> containerMemoryStatuses) {
        super(nodeId, PeriodicSchedulerEventType.MEMORY_STATUSES_UPDATE);
        this.containerMemoryStatuses = containerMemoryStatuses;
    }

    public List<ContainerMemoryStatus> getContainerMemoryStatuses(){ return this.containerMemoryStatuses;}
}
