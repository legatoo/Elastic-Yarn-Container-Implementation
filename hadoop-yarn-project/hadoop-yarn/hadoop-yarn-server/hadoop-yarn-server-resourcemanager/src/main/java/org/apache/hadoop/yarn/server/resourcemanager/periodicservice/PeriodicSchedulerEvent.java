package org.apache.hadoop.yarn.server.resourcemanager.periodicservice;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;

/**
 * Created by steven on 1/20/15.
 */
public class PeriodicSchedulerEvent extends AbstractEvent<PeriodicSchedulerEventType> {
    private final NodeId nodeId;
    public PeriodicSchedulerEvent(NodeId nodeId, PeriodicSchedulerEventType periodicSchedulerEventType) {
        super(periodicSchedulerEventType);
        this.nodeId = nodeId;
    }

    public NodeId getNodeId() {return this.nodeId; }
}
