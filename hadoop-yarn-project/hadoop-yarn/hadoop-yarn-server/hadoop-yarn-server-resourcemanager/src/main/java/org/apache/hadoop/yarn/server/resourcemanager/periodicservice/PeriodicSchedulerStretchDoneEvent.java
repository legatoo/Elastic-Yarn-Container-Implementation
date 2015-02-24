package org.apache.hadoop.yarn.server.resourcemanager.periodicservice;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.List;

/**
 * Created by steven on 2/24/15.
 */
public class PeriodicSchedulerStretchDoneEvent extends PeriodicSchedulerEvent {
    private final List<ContainerId> containersToStretch;

    public PeriodicSchedulerStretchDoneEvent(NodeId nodeId,
               List<ContainerId> squeezedContainers) {
        super(nodeId, PeriodicSchedulerEventType.STRETCH_DONE);
        this.containersToStretch = squeezedContainers;
    }

    public List<ContainerId> getStretchedContainers(){ return this.containersToStretch;}
}
