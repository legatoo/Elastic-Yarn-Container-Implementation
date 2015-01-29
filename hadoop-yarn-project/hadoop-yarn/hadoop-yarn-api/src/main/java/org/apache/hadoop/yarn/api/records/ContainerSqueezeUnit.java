package org.apache.hadoop.yarn.api.records;

/**
 * Created by steven on 1/20/15.
 */

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.util.Records;

/**
 * describe the unit to add/reduce a container
 */
@Public
@Stable
public abstract class ContainerSqueezeUnit  {



    @Public
    @Stable
    public static ContainerSqueezeUnit newInstance ( ContainerId containerId, Resource origin, Resource target){
        ContainerSqueezeUnit containerSqueezeUnit = Records.newRecord(ContainerSqueezeUnit.class);
        containerSqueezeUnit.setContainerId(containerId);
        containerSqueezeUnit.setOrigin(origin);
        containerSqueezeUnit.setTarget(target);
        return containerSqueezeUnit;
    }

    @Override
    public int hashCode() {
        // Generated by IntelliJ IDEA 13.1.
        int result = (int) (getContainerId().getContainerId() ^ (getContainerId().getContainerId() >>> 32));
        result = 31 * result + getTarget().hashCode();
        result = 31 * result + getOrigin().hashCode();
        return result;
    }



    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ContainerSqueezeUnit other = (ContainerSqueezeUnit) obj;
        if (!this.getContainerId().equals(other.getContainerId()))
            return false;
        if (!this.getOrigin().equals(other.getOrigin()))
            return false;
        if (!this.getTarget().equals(other.getTarget()))
            return false;
        return true;
    }
//
//    @Override
//    public int compareTo(ContainerSqueezeUnit other) {
//        if (this.getContainerId().compareTo(
//                other.getContainerId()) == 0) {
//            return this.getTarget().compareTo(other.getTarget());
//        } else {
//            return this.getOrigin().compareTo(
//                    other.getOrigin());
//        }
//    }

    @Public
    @Stable
    public abstract void setContainerId(ContainerId containerId);

    @Public
    @Stable
    public abstract void setOrigin(Resource origin);

    @Public
    @Stable
    public abstract void setTarget(Resource target);

    @Public
    @Stable
    public abstract ContainerId getContainerId();

    @Public
    @Stable
    public abstract Resource getOrigin();

    @Public
    @Stable
    public abstract Resource getTarget();

}
