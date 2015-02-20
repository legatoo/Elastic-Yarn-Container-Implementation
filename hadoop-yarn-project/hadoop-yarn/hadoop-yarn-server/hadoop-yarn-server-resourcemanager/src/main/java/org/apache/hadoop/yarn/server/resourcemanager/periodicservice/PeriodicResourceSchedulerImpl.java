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
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeSqueezeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerSqueezedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
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

    private final Comparator<ContainerSqueezeUnit> comparator = new MemoryStatusComparator();
    private final PriorityBlockingQueue<ContainerSqueezeUnit> runningContainerMemoryStatus = new
            PriorityBlockingQueue<ContainerSqueezeUnit>(20, comparator);

    private final Map<ContainerId, NodeId> containerIdToNodeId = new HashMap<ContainerId, NodeId>();
    private final Map<ContainerId, ContainerSqueezeUnit> currentSqueezedContainers = new HashMap<ContainerId, ContainerSqueezeUnit>();
    private final Map<ContainerId, NodeId> currentSpeculativeContainers = new HashMap<ContainerId, NodeId>();

    public PeriodicResourceSchedulerImpl(RMContext context) {
        super(PeriodicResourceSchedulerImpl.class.getName());
        this.context = context;
    }

    public PeriodicResourceSchedulerImpl(RMContext context, double threshold,
            String algorithm){
        super(PeriodicResourceSchedulerImpl.class.getName());
        this.context = context;


        // TODO: use different periodically scheduler


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
            int size = (int)(runningContainerMemoryStatus.size()/4);
            int count = 0;

            while (!runningContainerMemoryStatus.isEmpty()) {
                if (count == size)
                    break;

                ContainerSqueezeUnit cms = runningContainerMemoryStatus.poll();
                ContainerId containerId = cms.getContainerId();
                NodeId nodeId = containerIdToNodeId.get(containerId);

                RMContainer rmContainer = context.getScheduler().getRMContainer(containerId);
                if ( null == rmContainer)
                    continue;

                synchronized (currentSqueezedContainers){
                    LOG.debug("debug-> running container in PS: " + cms);
                    if( !currentSqueezedContainers.containsKey(containerId)) {

                        LOG.debug("Periodic scheduler is picking container to be squeeze: " + cms);
                        // generate memory squeeze unit

                        // Resource above threshold is available to squeeze
                        Resource diff = cms.getDiff();

                        containersToBeSqueezed.add(BuilderUtils.newContainerSqueezeUnit(
                                cms.getContainerId(), cms.getDiff(), (int)cms.getPriority()
                        ));

                        count ++;
                    }
                 }


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

            // send squeeze command to corresponding nodes

            Map.Entry<NodeId, List<ContainerSqueezeUnit>> entry = it.next();
            LOG.debug("Sending " + entry.getValue().size() + " squeeze requests to node " + entry.getKey());
            context.getDispatcher().getEventHandler().handle(
                    new RMNodeSqueezeEvent(entry.getKey(), entry.getValue())
            );
        }
    }

    @Override
    public void handle(PeriodicSchedulerEvent event) {
        NodeId nodeId = event.getNodeId();
        switch (event.getType()){
            case MEMORY_STATUSES_UPDATE:
                List<ContainerSqueezeUnit> containerMemoryStatuses = ((PeriodicSchedulerStatusUpdateEvent) event).getContainerMemoryStatuses();
                if (containerMemoryStatuses.isEmpty()) {
                    LOG.debug("No container memory status from " + nodeId);
                } else {
                    // update container memory usages
                    updateStatuses(containerMemoryStatuses, nodeId);
                }
                break;
            case SQUEEZE_DONE:
                List<ContainerSqueezeUnit> squeezedContainers = ((PeriodicSchedulerSqueezeDoneEvent) event).getSqueezedContainers();
                updateSqueezedContainers(nodeId, squeezedContainers);

//                context.getDispatcher().getEventHandler().handle(
//                        new ContainerSqueezedSchedulerEvent(SchedulerEventType.CONTAINER_SQUEEZED,
//                                squeezedContainers, nodeId)
//                );
                break;
            default:
                break;

        }

    }

    private void updateStatuses(List<ContainerSqueezeUnit> containerMemoryStatuses, NodeId nodeId) {
        assert (containerMemoryStatuses != null);

        synchronized (runningContainerMemoryStatus) {
            for (ContainerSqueezeUnit containerMemoryStatus : containerMemoryStatuses) {
                ContainerId containerId = containerMemoryStatus.getContainerId();
                RMContainer rmContainer = context.getScheduler().getRMContainer(containerId);

                if ( null != rmContainer) {
                    Container container = rmContainer.getContainer();

                    // speculative container will not be squeezed again
                    if(container.getIfSpeculative()) {
                        if (!currentSpeculativeContainers.containsKey(containerId))
                            currentSpeculativeContainers.put(containerId, nodeId);
                        continue;
                    }

                    synchronized (currentSqueezedContainers) {
                        if (currentSqueezedContainers.containsKey(containerMemoryStatus.getContainerId())) {
                            continue;
                        } else {
                            if (runningContainerMemoryStatus.contains(containerMemoryStatus)) {
                                LOG.debug("Updating resource usage of container  " + containerMemoryStatus.getContainerId());
                                runningContainerMemoryStatus.remove(containerMemoryStatus);
                            }

                            runningContainerMemoryStatus.add(containerMemoryStatus);
                            if (!containerIdToNodeId.containsKey(containerMemoryStatus.getContainerId()))
                                containerIdToNodeId.put(containerMemoryStatus.getContainerId(), nodeId);
                        }
                    }
                } else {
                    LOG.debug(containerId + " is not in Scheduler any more");
                    // clean it up
                    if (runningContainerMemoryStatus.contains(containerId))
                        runningContainerMemoryStatus.remove(containerId);

                    synchronized (currentSqueezedContainers) {
                        if (currentSqueezedContainers.containsKey(containerId))
                            currentSqueezedContainers.remove(containerId);
                    }

                    synchronized (currentSpeculativeContainers) {
                        if(currentSpeculativeContainers.containsKey(containerId))
                            currentSpeculativeContainers.remove(containerId);
                    }
                }


            }

            LOG.debug(runningContainerMemoryStatus.size() + " of containers in PQ.");
//            for (ContainerMemoryStatus c :runningContainerMemoryStatus){
//                LOG.debug("Got memory status from heart beat " + c);
//            }
        }
    }

    private void updateSqueezedContainers(NodeId nodeId,
                                          List<ContainerSqueezeUnit> squeezedContainers){
        if (squeezedContainers.isEmpty())
            return ;
        synchronized (currentSqueezedContainers){
            LOG.debug("Update squeeze done containers information in PeriodicScheduler");
            for (ContainerSqueezeUnit csu : squeezedContainers){
                if (!currentSqueezedContainers.containsKey(csu.getContainerId()))
                    currentSqueezedContainers.put(csu.getContainerId(), csu);
            }

            LOG.debug("Current squeezed containers are:");
            for (ContainerId containerId: currentSqueezedContainers.keySet()){
                LOG.debug(containerId);
            }
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
