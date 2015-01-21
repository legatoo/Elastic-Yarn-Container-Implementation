package org.apache.hadoop.yarn.server.resourcemanager.periodicservice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeSqueezeEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by steven on 1/19/15.
 */

/**
 * This calss will periodically send SchedulerEventType to resouce manager's
 * Event Dispatcher, and then update heartbeat to let node manager which container(s)
 * should be squeezed.
 */
@InterfaceAudience.LimitedPrivate("yarn")
@SuppressWarnings("unchecked")
public class PeriodicResourceSchedulerImpl extends AbstractService implements PeriodicResourceScheduler,
        EventHandler<PeriodicSchedulerEvent> {

    private static final Log LOG = LogFactory.getLog(PeriodicResourceSchedulerImpl.class);


    private final RMContext context;
    private Configuration configuration;
    private final Map<ContainerId, ContainerMemoryStatus> runningContainersMemoryStatus
            = new ConcurrentHashMap<ContainerId, ContainerMemoryStatus>();

    private final Map<ContainerId, NodeId> containerIdToNodeId = new HashMap<ContainerId, NodeId>();
    public PeriodicResourceSchedulerImpl(RMContext context) {
        super(PeriodicResourceSchedulerImpl.class.getName());
        this.context = context;
    }

    // when operating node squeeze, stop receive heartbeat information
    private AtomicBoolean operating = new AtomicBoolean(false);

    // TODO: proper algorithm to pick containers to be squeezed
    public List<ContainerSqueezeUnit> pickContainers(){
        return null;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        this.configuration = conf;
        super.serviceInit(conf);
    }

    @Override
    public List<ContainerId> getContainersToSqueezed() {
        List<ContainerId> containers = new ArrayList<ContainerId>();
        synchronized (runningContainersMemoryStatus) {
            for (ContainerId id : this.runningContainersMemoryStatus.keySet()) {
                containers.add(id);

            }
        }

        // try to send a event to RM dispatcher (and set certain flag)

        return containers;
    }

    @Override
    public void updateNodeHeartbeatResponseForSqueeze(NodeHeartbeatResponse response) {
        // TODO: fill response with containers to be squeezed
    }

    @Override
    public void handle(PeriodicSchedulerEvent event) {
        if( event.getType() == PeriodicSchedulerEventType.MEMORY_STATUSES_UPDATE){
            NodeId nodeId = event.getNodeId();
            List<ContainerMemoryStatus> containerMemoryStatuses = ((PeriodicSchedulerStatusEvent)event).getContainerMemoryStatuses();
            if ( containerMemoryStatuses.isEmpty()) {
                LOG.debug("Container memory status from " + nodeId);
            } else{
                synchronized (runningContainersMemoryStatus) {
                    for (ContainerMemoryStatus c : containerMemoryStatuses) {
                        runningContainersMemoryStatus.put(c.getContainerId(), c);
                        containerIdToNodeId.put(c.getContainerId(), nodeId);
                    }
                }
            }
        }
        else{
            LOG.debug("Unrecognized event :" + event);
        }

    }
}
