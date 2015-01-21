package org.apache.hadoop.yarn.server.resourcemanager.periodicservice;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.List;
import java.util.Map;

/**
 * Created by steven on 1/20/15.
 */
public class PeriodicSchedulerOperateEvent extends PeriodicSchedulerEvent {
    private final Map<NodeId, List<ContainerId> > containersToBeSqueeze;

    public PeriodicSchedulerOperateEvent(NodeId nodeId,
           Map<NodeId, List<ContainerId>> containersToBeSqueeze) {
        super(nodeId, PeriodicSchedulerEventType.OPERATE_PERIODIC_SCHEDULE);
        this.containersToBeSqueeze = containersToBeSqueeze;
    }

    public Map<NodeId, List<ContainerId>> getSqueezeTargets(){ return this.containersToBeSqueeze; }
}
