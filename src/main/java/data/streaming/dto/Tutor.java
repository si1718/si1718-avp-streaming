package data.streaming.dto;

public class Tutor {
    private String url;
    private String name;
    private String viewURL;

    public Tutor(String url, String name, String viewURL) {
        this.url = url;
        this.name = name;
        this.viewURL = viewURL;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getViewURL() {
        return viewURL;
    }

    public void setViewURL(String viewURL) {
        this.viewURL = viewURL;
    }

    @Override
    public String toString() {
        return "Tutor{" +
                "url='" + url + '\'' +
                ", name='" + name + '\'' +
                ", viewURL='" + viewURL + '\'' +
                '}';
    }
}
