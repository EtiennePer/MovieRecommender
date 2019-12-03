package com.camillepradel.movierecommender.model;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Movie {

    @SerializedName("id")
    private int id;
    @SerializedName("title")
    private String title;
    private List<Genre> genres;

    public Movie(int id, String title, List<Genre> genres) {
        this.id = id;
        this.title = title;
        this.genres = genres;
    }

    public int getId() {
        return this.id;
    }

    public String getTitle() {
        return this.title;
    }

    public List<Genre> getGenres() {
        return this.genres;
    }

    public void setGenres(List<Genre> genres) {
        this.genres = genres;
    }
}
