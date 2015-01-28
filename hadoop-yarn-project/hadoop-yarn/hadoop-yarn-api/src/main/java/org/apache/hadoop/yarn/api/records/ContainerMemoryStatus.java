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
    public static ContainerMemoryStatus newInstance(ContainerId containerId, double virtualMemUsage,
             double physicalMemUsage, Resource origin){
        ContainerMemoryStatus containerMemStatus = Records.newRecord(ContainerMemoryStatus.class);
        containerMemStatus.setContainerId(containerId);
        containerMemStatus.setVirtualMemUsage(virtualMemUsage);
        containerMemStatus.setPhysicalMemUsage(physicalMemUsage);
        containerMemStatus.setOriginResource(origin);
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
    public abstract void setOriginResource(Resource origin);

    @Public
    @Stable
    public abstract Resource getOriginResource();

    @Public
    @Unstable
    public abstract void setPhysicalMemUsage(double physicalMemUsage);

    @Public
    @Stable
    public abstract double getPhysicalMemUsage();

    @Override
    public int hashCode() {
        int result =  getContainerId().hashCode();
        return result;
    }

    //container memory status is distinguished by containerID
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ContainerMemoryStatus other = (ContainerMemoryStatus) obj;
        if (!this.getContainerId().equals(other.getContainerId()))
            return false;
        return true;
    }

//    @Override
//    public int compareTo(ContainerMemoryStatus o) {
//        // assume virtual memory limit check is disable
//        if (this.getPhysicalMemUsage() == o.getPhysicalMemUsage()){
//            if (this.getPhysicalMemLimit() < o.getPhysicalMemLimit())
//                return -1;
//            else if (this.getPhysicalMemLimit() > o.getPhysicalMemLimit())
//                return 1;
//            else
//                return 0;
//        } else {
//            if (this.getVirtualMemUsage() < o.getVirtualMemUsage())
//                return -1;
//            else if (this.getPhysicalMemUsage() > o.getVirtualMemUsage())
//                return 1;
//            else
//                return 0;
//        }
//    }

    @Override
    public String toString() {
        return super.toString();
    }
}
