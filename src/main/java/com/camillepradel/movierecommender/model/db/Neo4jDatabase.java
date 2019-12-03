package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import org.neo4j.driver.v1.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;


public class Neo4jDatabase extends AbstractDatabase implements AutoCloseable {
    private static String uri = "bolt://localhost:7687";
    private Driver driver;

    public Neo4jDatabase() {
        driver = GraphDatabase.driver(uri);
        System.out.println(driver.session().isOpen());
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    @Override
    public List<Movie> getAllMovies() {

    	 StatementResult result ;
    	   try (Session session = driver.session()) { 
    		    result  = session.run("MATCH (m:Movie) RETURN m.id AS id,m.title AS title ");
           }
    	   List<Movie> movies = new LinkedList<Movie>();
    	      // Each Cypher execution returns a stream of records.
           while (result.hasNext())
           {
               Record record = result.next();
               List<Genre> listeGenre=new ArrayList<>();
               for(Genre g: getListGenres(record.get("id").asInt())) {
            		 Genre genre = new Genre(g.getId(),g.getName());
            	   listeGenre.add(genre);
               }
             
            
               Movie m = new Movie(record.get("id").asInt(), record.get("title").asString(),  listeGenre);
               // Values can be extracted from a record by index or name.
               movies.add(m);
           }
           
   
           return movies;
  
    }


    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
   	 StatementResult result ;
	   try (Session session = driver.session()) { 
		    result  = session.run("MATCH(u:User{id:"+userId+"})-[rt:RATED]-(m:Movie) RETURN m.id AS id,m.title AS title ");
     }
	   List<Movie> movies = new LinkedList<Movie>();
	      // Each Cypher execution returns a stream of records.
     while (result.hasNext())
     {
         Record record = result.next();
         List<Genre> listeGenre=new ArrayList<>();
         for(Genre g: getListGenres(record.get("id").asInt())) {
        	 Genre genre = new Genre(g.getId(),g.getName());
        	  listeGenre.add(genre);
         }
        
       
         Movie m = new Movie(record.get("id").asInt(), record.get("title").asString(),  listeGenre);
         // Values can be extracted from a record by index or name.
         movies.add(m);
     }
     

     return movies;
    }
    
    public List<Genre> getListGenres(int filmId) {
      	 StatementResult result ;
  	   try (Session session = driver.session()) { 
  		    result  = session.run("MATCH(m:Movie{id:"+filmId+"})-[:CATEGORIZED_AS]->(g:Genre) RETURN g.id as genreId,g.name as genreTitle ");
       }
  	   List<Genre> listGenres = new LinkedList<Genre>();
  	      // Each Cypher execution returns a stream of records.
       while (result.hasNext())
       {
           Record record = result.next();
           Genre g = new Genre(record.get("genreId").asInt(), record.get("genreTitle").asString());
           listGenres.add(g);
       }
       return listGenres;
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
      
      	 StatementResult result ;
  	   try (Session session = driver.session()) { 
  		    result  = session.run("MATCH (u:User{id:"+userId+"})-[r:RATED]->(m:Movie) return r.note as note, r.timestamp as timestamp,m.id as id, m.title as title");
       }
  	 List<Rating> ratings = new LinkedList<Rating>();
  	      // Each Cypher execution returns a stream of records.
       while (result.hasNext())
       {
           Record record = result.next();
        
           List<Genre> listeGenre=new ArrayList<>();
           for(Genre g: getListGenres(record.get("id").asInt())) {
          	 Genre genre = new Genre(g.getId(),g.getName());
          	  listeGenre.add(genre);
           }
          
           Movie m = new Movie(record.get("id").asInt(), record.get("title").asString(),  listeGenre);
           Rating r = new Rating();
           r.setMovie(m);
           r.setScore(record.get("note").asInt());
           r.setUserId(userId);
           
           

           // Values can be extracted from a record by index or name.
           ratings.add(r);
       }
       

       return ratings;
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
    	 StatementResult result ;
    	 
    	   try (Session session = driver.session()) { 
    		    result  = session.run("MATCH (u:User{id:"+rating.getUserId()+"})-[r:RATED]->(m:Movie{id:"+rating.getMovieId()+"}) RETURN count(r) as count");
         
    		  	   if(result.hasNext() && result.next().get("count").asInt()==1) {
    	    		   result  = session.run("MATCH (u:User{id:"+rating.getUserId()+"})-[r:RATED]->(m:Movie{id:"+rating.getMovieId()+"}) SET r.note="+rating.getScore());
    	    	   }else {
    	    		   result  = session.run("MATCH (u:User{id:"+rating.getUserId()+"}),(m:Movie{id:"+rating.getMovieId()+"}) "
    	    		   		+ "CREATE (u)-[r:RATED]->(m) SET r.timestamp=TIMESTAMP()/1000 SET r.note="+rating.getScore());
    	    	   }
    	   }
  
    	  
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        // TODO: process recommendations for specified user exploiting other users ratings
        //       use different methods depending on processingMode parameter
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        List<Rating> recommendations = new LinkedList<Rating>();
        String titlePrefix;
        if (processingMode == 0) {
            titlePrefix = "0_";
        } else if (processingMode == 1) {
            titlePrefix = "1_";
        } else if (processingMode == 2) {
            titlePrefix = "2_";
        } else {
            titlePrefix = "default_";
        }
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));
        return recommendations;
    }
}
