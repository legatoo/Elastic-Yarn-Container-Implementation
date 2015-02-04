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
    private final double threshold;
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
        this.threshold = 0.7;
    }

    public PeriodicResourceSchedulerImpl(RMContext context, double threshold){
        super(PeriodicResourceSchedulerImpl.class.getName());
        this.context = context;
        this.threshold = threshold;
    }

    // when operating node squeeze, stop receive heartbeat information
    // set to true for testing purpose
    private AtomicBoolean operating = new AtomicBoolean(true);


    public Collection<ContainerSqueezeUnit> pickContainers() {
        Set<ContainerSqueezeUnit> containersToBeSqueezed = new HashSet<ContainerSqueezeUnit>();

        synchronized (runningContainerMemoryStatus) {
            // Ordered traverse

            // simply return all non-master containers for testing
            // TODO: proper algorithm to pick containers to be squeezed [simple priority queue for now]
            while (!runningContainerMemoryStatus.isEmpty()) {
                ContainerMemoryStatus cms = runningContainerMemoryStatus.poll();
                LOG.debug("Periodic scheduler is picking container to be squeeze: " + cms);
                // generate memory squeeze unit
//                Resource origin = cms.getOriginResource();
//                Resource target = BuilderUtils.newResource(origin.getMemory() / 2, 1);

                // Resource above threshold is available to squeeze
                Resource diff = BuilderUtils.newResource(
                        (int)((double)cms.getOriginResource().getMemory() * this.threshold),
                        1);
//                containersToBeSqueezed.add(BuilderUtils.newContainerSqueezeUnit(
//                        cms.getContainerId(),
//                        origin, target
//                ));

                containersToBeSqueezed.add(BuilderUtils.newContainerSqueezeUnit(
                        cms.getContainerId(),
                        diff
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


    @Override
    public void dispatchSqueezeEvent() {
        List<ContainerSqueezeUnit> containers = new ArrayList<ContainerSqueezeUnit>();
        containers.addAll(pickContainers());

        if (containers.isEmpty()) {
            LOG.debug("No containers to be squeeze.");
            return;
        }

        Map<NodeId, List<ContainerSqueezeUnit>> containersToBeSqueezed =
                new HashMap<NodeId, List<ContainerSqueezeUnit>>();

        // try to send a event to RM dispatcher (and set certain flag)

        for (ContainerSqueezeUnit c : containers){
//            RMNode rmNode = context.getRMNodes().get(containerIdToNodeId.get(c.getContainerId()));
            NodeId nodeId = containerIdToNodeId.get(c.getContainerId());

            if (containersToBeSqueezed.containsKey(nodeId)) {
                containersToBeSqueezed.get(nodeId).add(c);
            } else {
                containersToBeSqueezed.put(nodeId, new ArrayList<ContainerSqueezeUnit>());
                containersToBeSqueezed.get(nodeId).add(c);
            }

        }


        for (Iterator<Map.Entry<NodeId, List<ContainerSqueezeUnit>>> it =
                containersToBeSqueezed.entrySet().iterator(); it.hasNext();){

            Map.Entry<NodeId, List<ContainerSqueezeUnit>> entry = it.next();
            LOG.debug("Sending " + entry.getValue().size() + " squeeze requests to node " + entry.getKey());
            context.getDispatcher().getEventHandler().handle(
                    new RMNodeSqueezeEvent(entry.getKey(), entry.getValue())
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
                // update container memory usages
                updateStatuses(containerMemoryStatuses, nodeId);
            }
        } else {
            LOG.debug("Unrecognized event :" + event);
        }

    }

    private void updateStatuses(List<ContainerMemoryStatus> containerMemoryStatuses, NodeId nodeId) {
        assert (containerMemoryStatuses != null);

        synchronized (runningContainerMemoryStatus) {
            for (ContainerMemoryStatus containerMemoryStatus : containerMemoryStatuses) {
                if (runningContainerMemoryStatus.contains(containerMemoryStatus)) {
                    LOG.debug("Updating memory usage of container  " + containerMemoryStatus.getContainerId());
                    runningContainerMemoryStatus.remove(containerMemoryStatus);
                }

                runningContainerMemoryStatus.add(containerMemoryStatus);
                containerIdToNodeId.put(containerMemoryStatus.getContainerId(), nodeId);
            }

            LOG.debug(runningContainerMemoryStatus.size() + " of containers in PQ.");
//            for (ContainerMemoryStatus c :runningContainerMemoryStatus){
//                LOG.debug("Got memory status from heart beat " + c);
//            }
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
                        dispatchSqueezeEvent();

                    } catch (Throwable e) {
                        LOG.error("Caught exception in periodical scheduler.");
                    } finally {
                        synchronized (timerMonitor) {
                            try {
                                // for testing, send in every 10 seconds
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
