package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.spycsh.hesse.types.ingress.TemporalEdge;

public class VertexActivity implements Comparable<VertexActivity>{
    @JsonProperty("activity_type")
    private String activityType;
    @JsonProperty("src_id")
    private String srcId;
    @JsonProperty("dst_id")
    private String dstId;
    @JsonProperty("weight")
    private String weight;
    @JsonProperty("timestamp")
    private String timestamp;
    @JsonProperty("ingoing")
    private boolean ingoing;

    public VertexActivity() {}

    public VertexActivity(String activityType, TemporalEdge temporalEdge, boolean ingoing) {
        this.activityType = activityType;
        this.srcId = temporalEdge.getSrcId();
        this.dstId = temporalEdge.getDstId();
        this.weight = temporalEdge.getWeight();
        this.timestamp = temporalEdge.getTimestamp();
        this.ingoing = ingoing;
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

    public boolean isIngoing() {
        return ingoing;
    }

    public void setIngoing(boolean ingoing) {
        this.ingoing = ingoing;
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
