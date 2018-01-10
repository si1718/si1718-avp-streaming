package data.streaming.dto;

import java.util.List;

public class GroupDTO {
    private String name;
    private String leader;
    private List<String> components;

    public GroupDTO(){

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public List<String> getComponents() {
        return components;
    }

    public void setComponents(List<String> components) {
        this.components = components;
    }

    @Override
    public String toString() {
        return "GroupDTO{" +
                "name='" + name + '\'' +
                ", leader='" + leader + '\'' +
                ", components=" + components +
                '}';
    }
}
