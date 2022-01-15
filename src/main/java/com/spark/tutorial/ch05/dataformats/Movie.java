package com.spark.tutorial.ch05.dataformats;

import java.io.Serializable;
import java.util.stream.Stream;

public class Movie implements Serializable {
	private Integer movieId;
	private String title;
	private String genre;

	public Movie() {
	}

	public Movie(Integer movieId, String title, String genre) {
		this.movieId = movieId;
		this.title = title;
		this.genre = genre;
	}

	public Integer getMovieId() {
		return movieId;
	}

	public void setMovieId(Integer movieId) {
		this.movieId = movieId;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	@Override public String toString() {
		return "Movie{" +
				"movieId=" + movieId +
				", title='" + title + '\'' +
				", genre='" + genre + '\'' +
				'}';
	}

	public static Movie parseRating(String str) {
		String[] fields = str.split(",");
		if (fields.length != 3) {
			System.out.println("The elements are ::");
			Stream.of(fields).forEach(System.out::println);
			throw new IllegalArgumentException("Each line must contain 3 fields while the current line has ::" + fields.length );
		}
		Integer movieId = Integer.parseInt(fields[0]);
		String title = fields[1].trim();
		String genere = fields[2].trim();
		return new Movie(movieId, title, genere);
	}
}