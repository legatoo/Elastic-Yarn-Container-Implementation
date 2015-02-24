package org.apache.hadoop.yarn.server.nodemanager.containermanager.squeezer;

/**
 * Created by steven on 2/3/15.
 */
public enum ContainerSqueezerEventType {
    CONTAINER_SQUEEZE,
    CONTAINER_RECOVER,
    REMOVE_SQUEEZED_CONTAINER,
    STRETCH_RESOURCE,
    STRETCH_DONE
}
