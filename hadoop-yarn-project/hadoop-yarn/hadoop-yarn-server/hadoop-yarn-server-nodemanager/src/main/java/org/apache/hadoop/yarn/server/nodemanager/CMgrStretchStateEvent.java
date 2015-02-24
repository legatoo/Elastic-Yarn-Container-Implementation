package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.yarn.api.records.ContainerSqueezeUnit;

import java.util.List;

/**
 * Created by steven on 2/24/15.
 */
public class CMgrStretchStateEvent extends ContainerManagerEvent {

    private final boolean stretchState;
    private final Reason reason;

    public CMgrStretchStateEvent(boolean stretchState,
                            Reason reason) {
        super(ContainerManagerEventType.STRETCH_STATE_FROM_RM);
        this.stretchState = stretchState;
        this.reason = reason;
    }

    public boolean getStretchState() {
        return this.stretchState;
    }

    public Reason getReason() {
        return reason;
    }

    public static enum Reason {
        /**
         * When container's memory usage is relative low for sometime
         */
        FROM_RM
    }

}
