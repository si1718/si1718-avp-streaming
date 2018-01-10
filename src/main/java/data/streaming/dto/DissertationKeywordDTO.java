package data.streaming.dto;

import java.util.List;

public class DissertationKeywordDTO {
    private String idDissertation;
    private List<String> keywords;

    public DissertationKeywordDTO(String idDissertation, List<String> keywords) {
        this.idDissertation = idDissertation;
        this.keywords = keywords;
    }

    public DissertationKeywordDTO() {
    }

    public String getIdDissertation() {
        return idDissertation;
    }

    public void setIdDissertation(String idDissertation) {
        this.idDissertation = idDissertation;
    }

    public List<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }

    @Override
    public String toString() {
        return "DissertationKeywordDTO{" +
                "idDissertation='" + idDissertation + '\'' +
                ", keywords=" + keywords +
                '}';
    }
}
