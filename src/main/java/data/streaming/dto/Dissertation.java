package data.streaming.dto;

import java.util.List;

public class Dissertation {

    private List<Tutor> tutors;
    private String author;
    private String title;
    private Integer year;
    private String idDissertation;
    private String viewURL;
    private List<String> keywords;

    public Dissertation(List<Tutor> tutors, String author, String title, Integer year, String idDissertation, String viewURL, List<String> keywords) {
        this.tutors = tutors;
        this.author = author;
        this.title = title;
        this.year = year;
        this.idDissertation = idDissertation;
        this.viewURL = viewURL;
        this.keywords = keywords;
    }

    public List<Tutor> getTutors() {
        return tutors;
    }

    public String getAuthor() {
        return author;
    }

    public String getTitle() {
        return title;
    }

    public Integer getYear() {
        return year;
    }

    public String getIdDissertation() {
        return idDissertation;
    }

    public void setTutors(List<Tutor> tutors) {
        this.tutors = tutors;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public void setIdDissertation(String idDissertation) {
        this.idDissertation = idDissertation;
    }

    public void addTutor(Tutor tutor){
        this.tutors.add(tutor);
    }

    public String getViewURL() {
        return viewURL;
    }

    public void setViewURL(String viewURL) {
        this.viewURL = viewURL;
    }

    public List<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }

    @Override
    public String toString() {
        return "Dissertation{" +
                "tutors=" + tutors +
                ", author='" + author + '\'' +
                ", title='" + title + '\'' +
                ", year=" + year +
                ", idDissertation='" + idDissertation + '\'' +
                ", viewURL='" + viewURL + '\'' +
                '}';
    }
}
