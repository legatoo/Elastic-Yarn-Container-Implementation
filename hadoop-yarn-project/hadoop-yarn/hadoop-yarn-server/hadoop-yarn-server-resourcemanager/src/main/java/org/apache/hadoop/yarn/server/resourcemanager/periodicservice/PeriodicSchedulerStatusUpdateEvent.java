package org.apache.hadoop.yarn.server.resourcemanager.periodicservice;

import org.apache.hadoop.yarn.api.records.ContainerMemoryStatus;
import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.List;

/**
 * Created by steven on 1/20/15.
 */
public class PeriodicSchedulerStatusUpdateEvent extends PeriodicSchedulerEvent {
    private final List<ContainerSqueezeUnit> containerMemoryStatuses;

    public PeriodicSchedulerStatusUpdateEvent(NodeId nodeId, List<ContainerSqueezeUnit> containerMemoryStatuses) {
        super(nodeId, PeriodicSchedulerEventType.MEMORY_STATUSES_UPDATE);
        this.containerMemoryStatuses = containerMemoryStatuses;
    }

    public List<ContainerSqueezeUnit> getContainerMemoryStatuses(){ return this.containerMemoryStatuses;}
}
