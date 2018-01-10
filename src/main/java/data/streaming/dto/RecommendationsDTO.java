package data.streaming.dto;

import java.util.List;

public class RecommendationsDTO {

    private String idDissertation;
    private List<String> recommendations;

    public RecommendationsDTO(String idDissertation, List<String> recommendations) {
        this.idDissertation = idDissertation;
        this.recommendations = recommendations;
    }
    public RecommendationsDTO() {
    }

    public String getIdDissertation() {
        return idDissertation;
    }

    public void setIdDissertation(String idDissertation) {
        this.idDissertation = idDissertation;
    }

    public List<String> getRecommendations() {
        return recommendations;
    }

    public void setRecommendations(List<String> recommendations) {
        this.recommendations = recommendations;
    }

    @Override
    public String toString() {
        return "RecommendationsDTO{" +
                "idDissertation='" + idDissertation + '\'' +
                ", recommendations=" + recommendations +
                '}';
    }
}
