package org.apache.hadoop.yarn.server.resourcemanager.periodicservice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerMemoryStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

    public PeriodicResourceSchedulerImpl(RMContext context) {
        super(PeriodicResourceSchedulerImpl.class.getName());
        this.context = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        this.configuration = conf;
        super.serviceInit(conf);
    }

    @Override
    public List<ContainerId> getContainersToSqueezed() {
        List<ContainerId> containers = new ArrayList<ContainerId>();
        for (ContainerId id : this.runningContainersMemoryStatus.keySet()){
            containers.add(id);
        }
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
                for (ContainerMemoryStatus c : containerMemoryStatuses){
                    runningContainersMemoryStatus.put(c.getContainerId(), c);
                }
            }
        }
        else{
            LOG.debug("Unrecognized event :" + event);
        }

    }
}
