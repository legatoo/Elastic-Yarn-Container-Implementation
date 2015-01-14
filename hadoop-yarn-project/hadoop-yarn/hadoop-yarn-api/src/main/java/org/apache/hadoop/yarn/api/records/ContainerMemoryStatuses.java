package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by steven on 1/14/15.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ContainerMemoryStatuses {

    @InterfaceAudience.Public
    @InterfaceStability.Unstable
    public static ContainerMemoryStatuses newInstance(ContainerId containerId, double virtualMemUsage, double physicalMemUsage
    ){
        ContainerMemoryStatuses containerMemStatuses = Records.newRecord(ContainerMemoryStatuses.class);
        containerMemStatuses.setContainerId(containerId);
        containerMemStatuses.setVirtualMemUsage(virtualMemUsage);
        containerMemStatuses.setPhysicalMemUsage(physicalMemUsage);
        return containerMemStatuses;
    }

    @InterfaceAudience.Public
    @InterfaceStability.Unstable
    public abstract void setContainerId(ContainerId containerId);

    @InterfaceAudience.Public
    @InterfaceStability.Stable
    public abstract ContainerId getContainerId();

    @InterfaceAudience.Public
    @InterfaceStability.Unstable
    public abstract void setVirtualMemUsage(double virtualMemUsage);

    @InterfaceAudience.Public
    @InterfaceStability.Stable
    public abstract double getVirtualMemUsage();

    @InterfaceAudience.Public
    @InterfaceStability.Unstable
    public abstract void setPhysicalMemUsage(double physicalMemUsage);

    @InterfaceAudience.Public
    @InterfaceStability.Stable
    public abstract double getPhysicalMemUsage();


}
