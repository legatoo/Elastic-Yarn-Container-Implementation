package org.apache.hadoop.yarn.server.resourcemanager.periodicservice;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerMemoryStatus;
import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.List;

/**
 * Created by steven on 2/5/15.
 */
public class PeriodicSchedulerSqueezeDoneEvent extends PeriodicSchedulerEvent {
    private final List<ContainerSqueezeUnit> squeezedContainers;

    public PeriodicSchedulerSqueezeDoneEvent(NodeId nodeId, List<ContainerSqueezeUnit> squeezedContainers) {
        super(nodeId, PeriodicSchedulerEventType.SQUEEZE_DONE);
        this.squeezedContainers = squeezedContainers;
    }

    public List<ContainerSqueezeUnit> getSqueezedContainers(){ return this.squeezedContainers;}
}
