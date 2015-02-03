package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;

/**
 * Created by steven on 2/3/15.
 */
public class ContainerSqueezeEvent extends ContainerEvent{
    private final ContainerSqueezeUnit squeezeInfo;

    public ContainerSqueezeEvent(ContainerId cID, ContainerEventType eventType,
          ContainerSqueezeUnit containerSqueezeUnit) {
        super(cID, eventType);
        this.squeezeInfo = containerSqueezeUnit;
    }

    @Override
    public ContainerId getContainerID() {
        return super.getContainerID();
    }

    public ContainerSqueezeUnit getSqueezeInfo(){
        return  this.squeezeInfo;
    }
}
