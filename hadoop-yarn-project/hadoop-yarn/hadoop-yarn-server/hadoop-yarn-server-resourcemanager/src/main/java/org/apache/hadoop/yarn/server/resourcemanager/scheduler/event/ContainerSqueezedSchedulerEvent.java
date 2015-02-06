package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.state.Graph;

import java.util.List;

/**
 * Created by steven on 2/6/15.
 */
public class ContainerSqueezedSchedulerEvent extends SchedulerEvent {
    private final List<ContainerSqueezeUnit> containersAreSqueezed;
    private final NodeId nodeId;

    public ContainerSqueezedSchedulerEvent(SchedulerEventType type,
            List<ContainerSqueezeUnit> containersAreSqueezed, NodeId nodeId) {
        super(SchedulerEventType.CONTAINER_SQUEEZED);
        this.containersAreSqueezed = containersAreSqueezed;
        this.nodeId = nodeId;
    }

    public List<ContainerSqueezeUnit> getContainersAreSqueezed(){
        return this.containersAreSqueezed;
    }

    public NodeId getNodeId(){
        return this.nodeId;
    }

}
