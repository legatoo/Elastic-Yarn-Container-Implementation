
/**
 * Created by steven on 1/18/15.
 */

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

public class RMNodeContainerSqueezeEvent extends RMNodeEvent{
    private ContainerId contId;

    public RMNodeContainerSqueezeEvent(NodeId nodeId, ContainerId contId) {
        super(nodeId, RMNodeEventType.NODE_SQUEEZE);
        this.contId = contId;
    }

    public ContainerId getContainerId() {
        return this.contId;
    }
}
