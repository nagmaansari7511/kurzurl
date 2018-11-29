package com.kurzurl.kurzurl.dto;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;


/**
 * @author nagmaansari
 *
 */
public class ShortenUrlRequest {
    @NotNull
    @Size(min = 5, max = 1024)
    private String url;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
