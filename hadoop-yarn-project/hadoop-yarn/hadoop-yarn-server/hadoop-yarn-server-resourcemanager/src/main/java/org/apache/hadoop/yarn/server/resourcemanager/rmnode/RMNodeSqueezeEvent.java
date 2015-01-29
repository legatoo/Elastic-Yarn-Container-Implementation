package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.Collection;
import java.util.List;

/**
 * Created by steven on 1/20/15.
 */
public class RMNodeSqueezeEvent extends RMNodeEvent{
    private final Collection<ContainerSqueezeUnit> containersToBeSqueeze;

    public RMNodeSqueezeEvent(NodeId nodeId,
          Collection<ContainerSqueezeUnit> containersToBeSqueeze) {
        super(nodeId, RMNodeEventType.NODE_SQUEEZE);
        this.containersToBeSqueeze = containersToBeSqueeze;
    }

    public Collection<ContainerSqueezeUnit> getContainersToBeSqueeze() { return this.containersToBeSqueeze; }
}
