package org.apache.hadoop.yarn.server.nodemanager.containermanager.squeezer;

import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * Created by steven on 2/24/15.
 */
public class ContainerSqueezerBaseEvent extends AbstractEvent<ContainerSqueezerEventType> {

    public ContainerSqueezerBaseEvent(ContainerSqueezerEventType eventType) {
        super(eventType);
    }

}
