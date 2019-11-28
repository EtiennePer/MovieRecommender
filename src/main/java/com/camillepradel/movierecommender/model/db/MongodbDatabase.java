package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.google.gson.Gson;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.DBObject;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.internal.connection.Time;

import static com.mongodb.client.model.Filters.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.BSONTimestamp;

public class MongodbDatabase extends AbstractDatabase {

	String login = "root";
	String password = "root";
	String host = "localhost";
	String adminDB = "admin";
	String dbName = "movie_recommender";

	ConnectionString connString = new ConnectionString(
			"mongodb://" + password + ":" + login + "@" + host + "/" + adminDB + "?w=majority");
	MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(connString).retryWrites(true)
			.build();
	MongoClient mongoClient = MongoClients.create(settings);
	MongoDatabase database = mongoClient.getDatabase(dbName);

	public MongodbDatabase() {
		// database.createCollection("test");
	}

	@Override
	public List<Movie> getAllMovies() {

		List<Movie> movies = new LinkedList<Movie>();
		MongoCollection<Document> collection = database.getCollection("movies");
		MongoCollection<Document> genres = database.getCollection("genres");
		MongoCollection<Document> mov_genres = database.getCollection("mov_genre");

		Gson gson = new Gson();

		for (Document mov : collection.find()) {
			// recherche le mov_genre
			Document mov_genre = mov_genres.find(new Document("mov_id", mov.get("id"))).first();
			Document genre = genres.find(new Document("id", mov_genre.get("genre"))).first();

			Movie movie = gson.fromJson(mov.toJson(), Movie.class);
			Genre g = gson.fromJson(genre.toJson(), Genre.class);

			movie.setGenres(new ArrayList<>());
			movie.getGenres().add(g);
			movies.add(movie);
		}

		return movies;
	}

	@Override
	public List<Movie> getMoviesRatedByUser(int userId) {

		List<Movie> movies = new LinkedList<Movie>();
		MongoCollection<Document> ratings = database.getCollection("ratings");
		List<Integer> mov_ids = new ArrayList<>();
		Gson gson = new Gson();

		BasicDBObject query = new BasicDBObject();
		query.put("user_id", userId);

		for (Document cur : ratings.find(query)) {
			mov_ids.add(cur.getInteger("mov_id", 0));

		}

		MongoCollection<Document> moviesdb = database.getCollection("movies");

		query = new BasicDBObject("id", new BasicDBObject("$in", mov_ids.toArray()));

		for (Document cur : moviesdb.find(query)) {
			System.out.println(cur.toJson());
			movies.add(gson.fromJson(cur.toJson(), Movie.class));
		}
		System.out.println(movies.size());
		return movies;
	}

	@Override
	public List<Rating> getRatingsFromUser(int userId) {

		List<Rating> ratings = new LinkedList<Rating>();
		MongoCollection<Document> ratingsdb = database.getCollection("ratings");
		MongoCollection<Document> moviesdb = database.getCollection("movies");
		Gson gson = new Gson();

		BasicDBObject query = new BasicDBObject();
		query.put("user_id", userId);

		for (Document cur : ratingsdb.find(query)) {
			Document movie = moviesdb.find(eq("id", cur.getInteger("mov_id"))).first();

			Movie m = gson.fromJson(movie.toJson(), Movie.class);
			Rating r = gson.fromJson(cur.toJson(), Rating.class);
			r.setMovie(m);
			ratings.add(r);

		}

		return ratings;

	}

	@Override
	public void addOrUpdateRating(Rating rating) {
		// TODO: add query which
		// - add rating between specified user and movie if it doesn't exist
		// - update it if it does exist
		Document newRating = new Document();
		newRating.append("user_id", rating.getUserId());
		newRating.append("mov_id", rating.getMovieId());
		newRating.append("rating", rating.getScore());
		newRating.append("timestamp", new BsonTimestamp(Time.nanoTime() / 1000));

		Document modifiedObject = new Document();
		modifiedObject.put("$set", newRating);

		MongoCollection<Document> ratingsdb = database.getCollection("ratings");
		UpdateOptions options = new UpdateOptions().upsert(true);
		ratingsdb.updateOne(and(eq("user_id", rating.getUserId()), eq("mov_id", rating.getMovieId())), modifiedObject,
				options);

	}

	@Override
	public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
		HashMap<Integer, Integer> commonRatings = new HashMap<>();
		List<Rating> recommendations = new LinkedList<Rating>();
		MongoCollection<Document> ratingsdb = database.getCollection("ratings");
		MongoCollection<Document> usersdb = database.getCollection("users");
		MongoCollection<Document> moviesdb = database.getCollection("movies");

		// ratings of the connected user
		List<Integer> userRatedMovieIds = cursorToList(
				ratingsdb.distinct("mov_id", new Document("user_id", userId), Integer.class).iterator());

		// compare user ratings with other user's ratings
		for (Document user : usersdb.find(ne("id", userId))) {
			Collection<Integer> similar = new HashSet<Integer>(userRatedMovieIds);
			List<Integer> loopUserRatedMovieIds = cursorToList(ratingsdb
					.distinct("mov_id", new Document("user_id", user.getInteger("id")), Integer.class).iterator());
			similar.retainAll(loopUserRatedMovieIds);
			commonRatings.put(user.getInteger("id"), similar.size());
		}

		// set limit depending on mode
		int limit = 1;
		if (processingMode == 1) {
			limit = 5;
		}

		List<Entry<Integer, Integer>> maxRatingsInCommon = commonRatings.entrySet().stream().sorted(Map.Entry.comparingByValue()).skip(commonRatings.size() - limit).collect(Collectors.toList());
		maxRatingsInCommon.forEach(e -> System.out.println(e.getValue()));
		// for each couple <userId, countOfCommonRating>
		for(Entry<Integer, Integer> entry: maxRatingsInCommon) {
			List<Integer> similarUserRatedMovieIds = cursorToList(
					ratingsdb.distinct("mov_id", new Document("user_id", entry.getKey()), Integer.class).iterator());

			List<Integer> different = new ArrayList<>(similarUserRatedMovieIds);
			different.removeAll(userRatedMovieIds);

			Gson gson = new Gson();
			for (Document cur : ratingsdb.find(and(in("mov_id", different.toArray()), eq("user_id", entry.getKey())))) {
				Document movie = moviesdb.find(eq("id", cur.getInteger("mov_id"))).first();

				Movie m = gson.fromJson(movie.toJson(), Movie.class);
				Rating r = gson.fromJson(cur.toJson(), Rating.class);
				r.setMovie(m);

				recommendations.add(r);

			}
		}


		return recommendations;
	}

	public List<Integer> cursorToList(MongoCursor<Integer> mongoCursor) {
		ArrayList<Integer> mArrayList = new ArrayList<Integer>();

		while (mongoCursor.hasNext()) {
			mArrayList.add(mongoCursor.next()); // add the item
		}
		return mArrayList;
	}

}
