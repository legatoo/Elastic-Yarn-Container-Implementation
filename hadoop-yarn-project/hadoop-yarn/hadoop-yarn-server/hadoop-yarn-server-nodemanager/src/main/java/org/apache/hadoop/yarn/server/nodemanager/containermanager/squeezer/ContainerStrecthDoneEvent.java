package org.apache.hadoop.yarn.server.nodemanager.containermanager.squeezer;

import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;

/**
 * Created by steven on 2/24/15.
 */
public class ContainerStrecthDoneEvent extends ContainerSqueezerBaseEvent {

    private final boolean ifDone;

    public ContainerStrecthDoneEvent(boolean ifDone,
                                  ContainerSqueezerEventType eventType) {
        super(eventType);
        this.ifDone = ifDone;
    }

    public boolean getIfDone() {
        return this.ifDone;
    }

}