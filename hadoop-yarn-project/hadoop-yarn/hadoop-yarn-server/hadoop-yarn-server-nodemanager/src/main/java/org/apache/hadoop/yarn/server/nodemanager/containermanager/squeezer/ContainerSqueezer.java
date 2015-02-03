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
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerSqueezeEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;

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
        Container container = context.getContainers().get(containerSqueezeUnit.getContainerId());

        LOG.debug("HAHAHAHA " + event.getType());


    }
}
