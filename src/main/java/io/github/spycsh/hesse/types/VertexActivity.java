package io.github.spycsh.hesse.types;

import io.github.spycsh.hesse.types.ingress.TemporalEdge;

public class VertexActivity implements Comparable<VertexActivity>{
    private String activityType;

    private String srcId;
    private String dstId;
    private String weight;
    private String timestamp;

    public VertexActivity() {}

    public VertexActivity(String activityType, String srcId, String dstId, String timestamp) {
        this.activityType = activityType;
        this.srcId = srcId;
        this.dstId = dstId;
        this.timestamp = timestamp;
    }

    public VertexActivity(String activityType, String srcId, String dstId, String weight, String timestamp) {
        this.activityType = activityType;
        this.srcId = srcId;
        this.dstId = dstId;
        this.weight = weight;
        this.timestamp = timestamp;
    }

    public VertexActivity(String activityType, TemporalEdge temporalEdge) {
        this.activityType = activityType;
        this.srcId = temporalEdge.getSrcId();
        this.dstId = temporalEdge.getDstId();
        this.weight = temporalEdge.getWeight();
        this.timestamp = temporalEdge.getTimestamp();
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }

    public String getWeight() {
        return weight;
    }

    public String getActivityType() {
        return activityType;
    }

    public void setActivityType(String activityType) {
        this.activityType = activityType;
    }

    public String getSrcId() {
        return srcId;
    }

    public void setSrcId(String srcId) {
        this.srcId = srcId;
    }

    public String getDstId() {
        return dstId;
    }

    public void setDstId(String dstId) {
        this.dstId = dstId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(VertexActivity o) {
        if(this.getTimestamp().equals(o.getTimestamp())){
            if(this.getSrcId().equals(o.getSrcId())){
                return this.getDstId().compareTo(o.getDstId());
            } else{
                return this.getSrcId().compareTo(o.getSrcId());
            }
        }
        return Integer.parseInt(this.getTimestamp()) - Integer.parseInt(o.getTimestamp());
    }

    @Override
    public String toString() {
        return "VertexActivity{" +
                "activityType='" + activityType + '\'' +
                ", srcId='" + srcId + '\'' +
                ", dstId='" + dstId + '\'' +
                ", weight='" + weight + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
