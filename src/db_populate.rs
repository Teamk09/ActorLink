use actor_link::db;
use actor_link::tmdb_get::{get_movie_credits, get_movie_details, movie_exists, movie_exists_with_client, is_feature_film_with_client, TMDBCredit};
use rusqlite::Result;
use std::env;
use futures::stream::{self, StreamExt};
use tokio::time::{sleep, Duration};
use reqwest;

async fn is_feature_film(movie_id: u32, api_key: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let movie_details = get_movie_details(movie_id, api_key).await?;

    // Filter out adult movies
    if movie_details.adult {
        return Ok(false);
    }

    // Filter out video content
    if movie_details.video {
        return Ok(false);
    }

    // Check if it has a release date
    if movie_details.release_date.is_none() {
        return Ok(false);
    }

    // TV Movie genre ID is 10770
    // Documentary genre ID is 99
    if movie_details.genres.iter().any(|genre| genre.id == 10770 || genre.id == 99) {
        return Ok(false);
    }

    Ok(true)
}

pub async fn populate_database() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("TMDB_API_KEY")?;
    let mut conn = db::establish_connection()?;
    db::setup_database(&conn)?;

    // Create reusable client
    let client = reqwest::Client::new();
    let concurrent_requests = 10;

    // Use transaction for batch inserts
    let tx = conn.transaction()?;

    let movie_ids: Vec<u32> = (232000..262000).collect();

    let mut stream = stream::iter(movie_ids)
        .map(|movie_tmdb_id| {
            let api_key = api_key.clone();
            let client = client.clone();
            async move {
                //sleep(Duration::from_millis(10)).await;

                match movie_exists_with_client(movie_tmdb_id, &api_key, &client).await {
                    Ok(true) => {
                        match is_feature_film_with_client(movie_tmdb_id, &api_key, &client).await {
                            Ok(true) => {
                                match get_movie_credits(movie_tmdb_id, &api_key).await {
                                    Ok(movie_credits) => {
                                        println!("Processing feature film ID: {}", movie_tmdb_id);
                                        Some((movie_tmdb_id, movie_credits))
                                    }
                                    Err(e) => {
                                        eprintln!("Error fetching credits for movie ID {}: {}", movie_tmdb_id, e);
                                        None
                                    }
                                }
                            }
                            Ok(false) => {
                                println!("Skipping non-feature film ID: {}", movie_tmdb_id);
                                None
                            }
                            Err(e) => {
                                eprintln!("Error checking movie type for ID {}: {}", movie_tmdb_id, e);
                                None
                            }
                        }
                    }
                    Ok(false) => {
                        println!("Movie ID {} does not exist", movie_tmdb_id);
                        None
                    }
                    Err(e) => {
                        eprintln!("Error checking if movie {} exists: {}", movie_tmdb_id, e);
                        None
                    }
                }
            }
        })
        .buffer_unordered(concurrent_requests);

    // Process results in batches
    let mut batch = Vec::new();
    while let Some(result) = stream.next().await {
        if let Some(data) = result {
            batch.push(data);
            if batch.len() >= 50 {
                process_batch(&tx, &batch).await?;
                batch.clear();
            }
        }
    }

    // Process remaining items
    if !batch.is_empty() {
        process_batch(&tx, &batch).await?;
    }

    tx.commit()?;
    println!("Database populated with feature film and actor data.");
    Ok(())
}

async fn process_batch<'a>(tx: &'a rusqlite::Transaction<'a>, batch: &[(u32, TMDBCredit)]) -> Result<(), Box<dyn std::error::Error>> {
    for (movie_tmdb_id, movie_credits) in batch {
        let movie_details = get_movie_details(*movie_tmdb_id, &env::var("TMDB_API_KEY")?).await?;
        db::insert_movie(tx, *movie_tmdb_id, &movie_details.title)?;

        let mut stmt = tx.prepare("SELECT movie_id FROM movies WHERE tmdb_movie_id = ?")?;
        let mut rows = stmt.query([movie_tmdb_id])?;
        let movie_id: i64 = rows.next()?.unwrap().get(0)?;

        for actor in &movie_credits.cast {
            db::insert_actor(tx, actor.id, &actor.name, &actor.known_for_department)?;
            let mut actor_stmt = tx.prepare("SELECT actor_id FROM actors WHERE tmdb_actor_id = ?")?;
            let mut actor_rows = actor_stmt.query([actor.id])?;
            let actor_id: i64 = actor_rows.next()?.unwrap().get(0)?;
            db::insert_movie_actor_link(tx, movie_id, actor_id)?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    populate_database().await?;
    Ok(())
}