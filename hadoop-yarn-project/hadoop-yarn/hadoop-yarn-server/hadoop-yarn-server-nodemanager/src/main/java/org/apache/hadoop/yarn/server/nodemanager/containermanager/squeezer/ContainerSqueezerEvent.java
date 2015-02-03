package org.apache.hadoop.yarn.server.nodemanager.containermanager.squeezer;

import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;

/**
 * Created by steven on 2/3/15.
 */
public class ContainerSqueezerEvent extends AbstractEvent<ContainerSqueezerEventType> {

    private final ContainerSqueezeUnit containerSqueezeUnit;

    public ContainerSqueezerEvent(ContainerSqueezeUnit container,
                                  ContainerSqueezerEventType eventType) {
        super(eventType);
        this.containerSqueezeUnit = container;
    }

    public ContainerSqueezeUnit getContainerSqueezeUnit() {
        return containerSqueezeUnit;
    }

}
