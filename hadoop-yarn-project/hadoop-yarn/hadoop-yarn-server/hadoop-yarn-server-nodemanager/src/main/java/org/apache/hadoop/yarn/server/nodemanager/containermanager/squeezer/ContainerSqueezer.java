package org.apache.hadoop.yarn.server.nodemanager.containermanager.squeezer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.*;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerSqueezeEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorUpdateAsStretchDone;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by steven on 2/3/15.
 */
public class ContainerSqueezer extends AbstractService
        implements EventHandler<ContainerSqueezerBaseEvent> {
    private static final Log LOG = LogFactory.getLog(ContainerSqueezer.class);


    private final Context context;
    private final ContainerExecutor exec;
    private final Dispatcher dispatcher;
    private final ContainerManagerImpl containerManager;
    private AtomicBoolean ifSqueeze = new AtomicBoolean(false);
    private AtomicBoolean ifStretch = new AtomicBoolean(false);
    private Resource strecthResourceSize = Resource.newInstance(0, 0);
    private List<ContainerId> containersToStretch = new ArrayList<ContainerId>();

    private final Map<ContainerId, ContainerSqueezeUnit> currentSqueezedContainers
            = Collections.synchronizedMap(new HashMap<ContainerId, ContainerSqueezeUnit>());

    private final List<ContainerSqueezeUnit> squeezedContainersInThisRound =
            new ArrayList<ContainerSqueezeUnit>();


    public ContainerSqueezer(Context context, AsyncDispatcher dispatcher,
                              ContainerExecutor exec, ContainerManagerImpl containerManager) {
        super("containers-squeezer");
        this.exec = exec;
        this.context = context;
        this.dispatcher = dispatcher;
        this.containerManager = containerManager;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
    }

    @Override
    protected  void serviceStop() throws Exception {
        super.serviceStop();
    }

    @Override
    public void handle(ContainerSqueezerBaseEvent event) {

//        ((ContainerManagerImpl)context.getContainerManager()).getContainersMonitor()
//                .informMonitor(containerSqueezeUnit);

        // Squeeze operation is HERE
        LOG.debug("Processing " + event.getType() + " in ContainerSqueezer.");
        ContainerId containerId;

        switch ( event.getType()){
            case CONTAINER_SQUEEZE:
                ContainerSqueezerEvent containerSqueezerEvent = (ContainerSqueezerEvent)event;
                ContainerSqueezeUnit containerSqueezeUnit =
                        containerSqueezerEvent.getContainerSqueezeUnit();

                containerId = containerSqueezeUnit.getContainerId();
                synchronized (currentSqueezedContainers) {
                    if (!currentSqueezedContainers.containsKey(containerId)) {
                        LOG.debug("Squeezing " + containerId + " on node " + context.getNodeId()
                                + " with resource amount of "
                                + containerSqueezeUnit.getDiff() );
                        //Container container = context.getContainers().get(containerId);


                        // keep record in ContainerSqueezer for query
                        currentSqueezedContainers.put(containerId, containerSqueezeUnit);
                        squeezedContainersInThisRound.add(containerSqueezeUnit);

                        // inform Container Monitor
                        // done in ContainerImpl

                        // update heart beat about successfully squeezed containers
                        // only if on need to stretch. if stretch is needed. no sense to
                        // squeeze
                        if (!getIfStretch())
                            setIfSqueeze(true);
                    }

                }

                break;
            case REMOVE_SQUEEZED_CONTAINER:
                CompleteSqueezedContainerEvent completeSqueezedContainerEvent =
                        (CompleteSqueezedContainerEvent)event;
                containerId =
                        completeSqueezedContainerEvent.getCompletedSqueezedContainerId();
                synchronized ( currentSqueezedContainers){
                    if (currentSqueezedContainers.containsKey(containerId)){
                        currentSqueezedContainers.remove(containerId);

                        LOG.debug("Well Done. " + containerId + " has been removed from Squeezer.");
                    }
                }
            case STRETCH_RESOURCE:
                // TODO
                ContainerStretchEvent containerStretchEvent =
                        (ContainerStretchEvent)event;

                Resource resource = containerStretchEvent.getStrecthResourceSize();
                List<ContainerId> containersNeedToStretch =
                        containerStretchEvent.getContainersNeedToStretch();
                if(resource.getMemory() != 0 &&
                        !containersNeedToStretch.isEmpty()){
                    setStrecthResourceSize(resource);
                    setContainersToStretch(containersNeedToStretch);
                    setIfStretch(true);
                }

                break;
            case STRETCH_DONE:
                LOG.debug("hakunami------>hakunami");
                ContainerStrecthDoneEvent containerStrecthDoneEvent =
                        (ContainerStrecthDoneEvent)event;


                if (containerStrecthDoneEvent.getIfDone()) {
                    setIfStretch(false);

                    // remove stretched containers from squeezing containers in monitor
                    for (ContainerId id : this.containersToStretch){
                        dispatcher.getEventHandler().handle(
                                new ContainersMonitorUpdateAsStretchDone(id)
                        );
                    }
                }
                break;

            default:
                break;

        }


    }

    public List<ContainerSqueezeUnit> getCurrentSqueezedContainers(){
        List<ContainerSqueezeUnit> returnResult = new ArrayList<ContainerSqueezeUnit>();

        if (!currentSqueezedContainers.isEmpty()) {
            returnResult.addAll(currentSqueezedContainers.values());
        }

        return returnResult;
    }

    public List<ContainerSqueezeUnit> getSqueezedContainersInThisRound(){
        List<ContainerSqueezeUnit> returnResult = new ArrayList<ContainerSqueezeUnit>();

        synchronized (squeezedContainersInThisRound){
            if (!squeezedContainersInThisRound.isEmpty()){
                returnResult.addAll(squeezedContainersInThisRound);
                squeezedContainersInThisRound.clear();
            }
        }

        // return squeezed containers in this round and
        // set squeeze flag to false

        setIfSqueeze(false);

        return returnResult;
    }

    public synchronized boolean getIfSqueeze() {
        return ifSqueeze.get();
    }

    public synchronized void setIfSqueeze(boolean ifSqueeze) {
        LOG.debug("ContainerSqueezer set squeeze flag to " + ifSqueeze);
        this.ifSqueeze.set(ifSqueeze);
    }

    public synchronized void setIfStretch(boolean ifStretch) {
        LOG.debug("Container Stretch set to " + ifSqueeze);
        this.ifStretch.set(ifStretch);
    }

    public synchronized boolean getIfStretch() {
        return ifStretch.get();
    }

    public Resource getStrecthResourceSize() {
        return strecthResourceSize;
    }

    public void setStrecthResourceSize(Resource strecthResourceSize) {
        this.strecthResourceSize = strecthResourceSize;
    }

    public List<ContainerId> getContainersToStretch() {
        return containersToStretch;
    }

    public void setContainersToStretch(List<ContainerId> containersToStretch) {
        this.containersToStretch.addAll(containersToStretch);
    }
}
