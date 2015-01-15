package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Created by steven on 1/14/15.
 */
@Public
@Stable
public abstract class ContainerMemoryStatus {

    @Public
    @Unstable
    public static ContainerMemoryStatus newInstance(ContainerId containerId, double virtualMemUsage, double physicalMemUsage){
        ContainerMemoryStatus containerMemStatus = Records.newRecord(ContainerMemoryStatus.class);
        containerMemStatus.setContainerId(containerId);
        containerMemStatus.setVirtualMemUsage(virtualMemUsage);
        containerMemStatus.setPhysicalMemUsage(physicalMemUsage);
        return containerMemStatus;
    }

    @Public
    @Unstable
    public abstract void setContainerId(ContainerId containerId);

    @Public
    @Stable
    public abstract ContainerId getContainerId();

    @Public
    @Unstable
    public abstract void setVirtualMemUsage(double virtualMemUsage);

    @Public
    @Stable
    public abstract double getVirtualMemUsage();

    @Public
    @Unstable
    public abstract void setPhysicalMemUsage(double physicalMemUsage);

    @Public
    @Stable
    public abstract double getPhysicalMemUsage();


}
