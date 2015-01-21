package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.List;

/**
 * Created by steven on 1/20/15.
 */
public class RMNodeSqueezeEvent extends RMNodeEvent{
    private final List<ContainerSqueezeUnit> containersToBeSqueeze;

    public RMNodeSqueezeEvent(NodeId nodeId,
          List<ContainerSqueezeUnit> containersToBeSqueeze) {
        super(nodeId, RMNodeEventType.NODE_SQUEEZE);
        this.containersToBeSqueeze = containersToBeSqueeze;
    }

    public List<ContainerSqueezeUnit> getContainersToBeSqueeze() { return this.containersToBeSqueeze; }
}
