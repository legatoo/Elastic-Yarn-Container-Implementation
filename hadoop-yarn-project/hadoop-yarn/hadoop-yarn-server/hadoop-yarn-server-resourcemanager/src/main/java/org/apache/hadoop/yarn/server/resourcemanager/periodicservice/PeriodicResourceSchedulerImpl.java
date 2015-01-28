package org.apache.hadoop.yarn.server.resourcemanager.periodicservice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeSqueezeEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.MemoryStatusComparator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
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

    private Runnable squeezeOperateRunnable;
    private Thread squeezeOperator;
    private final Object timerMonitor = new Object();

    private final Comparator<ContainerMemoryStatus> comparator = new MemoryStatusComparator();
    private final PriorityBlockingQueue<ContainerMemoryStatus> runningContainerMemoryStatus = new
            PriorityBlockingQueue<ContainerMemoryStatus>(20, comparator);

    private final Map<ContainerId, NodeId> containerIdToNodeId = new HashMap<ContainerId, NodeId>();

    public PeriodicResourceSchedulerImpl(RMContext context) {
        super(PeriodicResourceSchedulerImpl.class.getName());
        this.context = context;
    }

    // when operating node squeeze, stop receive heartbeat information
    // set to true for testing purpose
    private AtomicBoolean operating = new AtomicBoolean(true);

    // TODO: proper algorithm to pick containers to be squeezed [simple priority queue for now]
    public List<ContainerSqueezeUnit> pickContainers() {
        List<ContainerSqueezeUnit> containersToBeSqueezed = new ArrayList<ContainerSqueezeUnit>();

        synchronized (runningContainerMemoryStatus) {
            for (Iterator<ContainerMemoryStatus> iter = runningContainerMemoryStatus.iterator(); iter.hasNext(); ) {
                ContainerMemoryStatus containerMemoryStatus = iter.next();
                // generate memory squeeze unit
                Resource origin = containerMemoryStatus.getOriginResource();
                Resource target = BuilderUtils.newResource(origin.getMemory() / 2, 4);
                containersToBeSqueezed.add(BuilderUtils.newContainerSqueezeUnit(
                        containerMemoryStatus.getContainerId(),
                        origin, target
                ));
            }
        }

        return containersToBeSqueezed;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        this.configuration = new Configuration(conf);
        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        try{
            super.serviceStart();
            startSqueezeOperator();
        } catch (Exception e){
            String errorMessage = "Unexpected error starting periodical scheduler.";
            LOG.error(errorMessage, e);
            throw new YarnRuntimeException(e);
        }
    }

    @Override
    protected void serviceStop() throws Exception {
        super.serviceStop();
    }

    /**
     * only for testing
     * @return
     */
    @Override
    public List<ContainerSqueezeUnit> getContainersToSqueezed() {
        List<ContainerSqueezeUnit> containers = pickContainers();
        return containers;
    }

    @Override
    public void dispatchSqueezeEvent() {
        LOG.debug("send squeeze event to nodes.");
        List<ContainerSqueezeUnit> containers = pickContainers();
        Map<NodeId, List<ContainerSqueezeUnit>> containersToBeSqueezed =
                new HashMap<NodeId, List<ContainerSqueezeUnit>>();

        // try to send a event to RM dispatcher (and set certain flag)

        for (ContainerSqueezeUnit c : containers){
//            RMNode rmNode = context.getRMNodes().get(containerIdToNodeId.get(c.getContainerId()));
            NodeId nodeId = containerIdToNodeId.get(c.getContainerId());
            containersToBeSqueezed.put(nodeId, new ArrayList<ContainerSqueezeUnit>());
            containersToBeSqueezed.get(nodeId).add(c);
        }

        for (Map.Entry<NodeId, List<ContainerSqueezeUnit>> c : containersToBeSqueezed.entrySet()){
            context.getDispatcher().getEventHandler().handle(
                    new RMNodeSqueezeEvent(c.getKey(), c.getValue())
            );
        }
    }

    @Override
    public void handle(PeriodicSchedulerEvent event) {
        if (event.getType() == PeriodicSchedulerEventType.MEMORY_STATUSES_UPDATE) {
            NodeId nodeId = event.getNodeId();
            List<ContainerMemoryStatus> containerMemoryStatuses = ((PeriodicSchedulerStatusUpdateEvent) event).getContainerMemoryStatuses();
            if (containerMemoryStatuses.isEmpty()) {
                LOG.debug("No container memory status from " + nodeId);
            } else {

                for (ContainerMemoryStatus c : containerMemoryStatuses) {
                    updateStatuses(c);
                    containerIdToNodeId.put(c.getContainerId(), nodeId);
                }
            }
        } else {
            LOG.debug("Unrecognized event :" + event);
        }

    }

    private void updateStatuses(ContainerMemoryStatus containerMemoryStatus) {
        assert (containerMemoryStatus != null);

        synchronized (runningContainerMemoryStatus) {
            if (runningContainerMemoryStatus.contains(containerMemoryStatus)) {
                runningContainerMemoryStatus.remove(containerMemoryStatus);
            }

            runningContainerMemoryStatus.add(containerMemoryStatus);
        }

    }

    public AtomicBoolean getOperating() {
        return operating;
    }

    public void setOperating(AtomicBoolean operating) {
        this.operating = operating;
    }

    protected void startSqueezeOperator(){
        squeezeOperateRunnable = new Runnable() {
            @Override
            public void run() {
                while( true) {
                    try {
                        // send squeeze event periodically
                        LOG.debug("Sending squeeze event to node");
                        dispatchSqueezeEvent();

                    } catch (Throwable e) {
                        LOG.error("Caught exception in periodical scheduler.");
                    } finally {
                        synchronized (timerMonitor) {
                            try {
                                timerMonitor.wait(YarnConfiguration.DEFAULT_PS_SQUEEZE_INTERVAL_MS);
                            } catch (InterruptedException e) {
                                // Do Nothing
                            }
                        }
                    }
                }

            }
        };

        squeezeOperator = new Thread(squeezeOperateRunnable, "Periodic scheduler");
        squeezeOperator.start();
    }
}
