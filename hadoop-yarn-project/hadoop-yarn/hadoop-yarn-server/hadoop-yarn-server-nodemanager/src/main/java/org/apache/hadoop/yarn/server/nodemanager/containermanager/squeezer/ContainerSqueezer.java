package org.apache.hadoop.yarn.server.nodemanager.containermanager.squeezer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.*;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerSqueezeEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by steven on 2/3/15.
 */
public class ContainerSqueezer extends AbstractService
        implements EventHandler<ContainerSqueezerEvent> {
    private static final Log LOG = LogFactory.getLog(ContainerSqueezer.class);


    private final Context context;
    private final ContainerExecutor exec;
    private final Dispatcher dispatcher;
    private final ContainerManagerImpl containerManager;
    private AtomicBoolean ifSqueeze = new AtomicBoolean(false);

    private final Map<ContainerId, ContainerSqueezeUnit> currentSqueezedContainers
            = Collections.synchronizedMap(new HashMap<ContainerId, ContainerSqueezeUnit>());

    private final List<ContainerSqueezeUnit> squeezedContainersInThisRound =
            new ArrayList<ContainerSqueezeUnit>();


    public ContainerSqueezer(Context context, Dispatcher dispatcher,
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
    public void handle(ContainerSqueezerEvent event) {
        ContainerSqueezeUnit containerSqueezeUnit = event.getContainerSqueezeUnit();
        ContainerId containerId = containerSqueezeUnit.getContainerId();

//        ((ContainerManagerImpl)context.getContainerManager()).getContainersMonitor()
//                .informMonitor(containerSqueezeUnit);

        // Squeeze operation is HERE
        LOG.debug("Processing " + event.getType() + " in ContainerSqueezer.");

        switch ( event.getType()){
            case CONTAINER_SQUEEZE:
                synchronized (currentSqueezedContainers) {
                    if (!currentSqueezedContainers.containsKey(containerId)) {
                        LOG.debug("Squeezing " + containerId + " with resource amount of "
                                + containerSqueezeUnit.getDiff() );
                        //Container container = context.getContainers().get(containerId);

                        // 1. update node metrics information
                        // done in ContainerImpl

                        // 2. keep record in ContainerSqueezer for query
                        currentSqueezedContainers.put(containerId, containerSqueezeUnit);
                        squeezedContainersInThisRound.add(containerSqueezeUnit);

                        // 3. inform Container Monitor
                        // done in ContainerImpl

                        // 4. update heart beat about successfully squeezed containers
                        setIfSqueeze(true);
                    }

                }

                break;
            case CONTAINER_RECOVER:
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

        return returnResult;
    }

    public boolean getIfSqueeze() {
        return ifSqueeze.get();
    }

    public void setIfSqueeze(boolean ifSqueeze) {
        LOG.debug("ContainerSqueezer set squeeze flag to " + ifSqueeze);
        this.ifSqueeze.set(ifSqueeze);
    }
}
