use dotenv::dotenv;
use std::env;
use std::io::{self, Write};
use std::path::Path;
//use tokio::time::{sleep, Duration};
mod tmdb_get;
mod db;
mod db_populate;
mod link_finder;
use rusqlite::Result;
use crate::db::{get_actor_id_by_name, get_actor_name_by_id, get_movie_titles_by_ids, get_movie_ids_for_actor};
use crate::link_finder::find_actor_link_bidirectional_bfs;
use std::collections::HashSet;

async fn ensure_database_exists() -> Result<(), Box<dyn std::error::Error>> {
    if !Path::new("actor_link.db").exists() {
        println!("Database not found. Setting up and populating database...");
        let conn = db::establish_connection()?;
        db::setup_database(&conn)?;
        crate::db_populate::populate_database().await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let _api_key = env::var("TMDB_API_KEY")?;

    // Ensure database exists and is populated
    ensure_database_exists().await?;

    let conn = db::establish_connection()?;

    // --- User Input for Actor Names ---
    print!("Enter the first actor's name: ");
    io::stdout().flush()?; // Ensure the prompt is displayed
    let mut start_actor_name = String::new();
    io::stdin().read_line(&mut start_actor_name)?;
    let start_actor_name = start_actor_name.trim();

    print!("Enter the second actor's name: ");
    io::stdout().flush()?; // Ensure the prompt is displayed
    let mut target_actor_name = String::new();
    io::stdin().read_line(&mut target_actor_name)?;
    let target_actor_name = target_actor_name.trim();

    // --- Fetch Actor IDs from Names ---
    let start_actor_id_option = get_actor_id_by_name(&conn, start_actor_name)?;
    let target_actor_id_option = get_actor_id_by_name(&conn, target_actor_name)?;

    let start_actor_id = match start_actor_id_option {
        Some(id) => id,
        None => {
            println!("Actor '{}' not found in database.", start_actor_name);
            return Ok(()); // Exit if actor not found
        }
    };

    let target_actor_id = match target_actor_id_option {
        Some(id) => id,
        None => {
            println!("Actor '{}' not found in database.", target_actor_name);
            return Ok(()); // Exit if actor not found
        }
    };

    println!("\nFinding link between '{}' and '{}'...", start_actor_name, target_actor_name);

    match find_actor_link_bidirectional_bfs(&conn, start_actor_id, target_actor_id)? {
        Some(path) => {
            println!("Actor link found:");
            if path.len() == 1 {
                println!("{} and {} are the same actor!", start_actor_name, target_actor_name);
                println!("{}", start_actor_name);
            } else {
                // Construct path with actor names and movie titles
                let mut link_count = 0;
                let mut path_string = String::new();
                for i in 0..path.len() - 1 {
                    link_count += 1;
                    let current_actor_id = path[i];
                    let next_actor_id = path[i+1];

                    let current_actor_name = get_actor_name_by_id(&conn, current_actor_id)?.unwrap_or_else(|| "Unknown Actor".to_string());
                    let next_actor_name = get_actor_name_by_id(&conn, next_actor_id)?.unwrap_or_else(|| "Unknown Actor".to_string());

                    // Find movies they both starred in
                    let current_actor_movies = get_movie_ids_for_actor(&conn, current_actor_id)?;
                    let next_actor_movies = get_movie_ids_for_actor(&conn, next_actor_id)?;
                    let connecting_movie_ids: HashSet<i64> = current_actor_movies.intersection(&next_actor_movies).cloned().collect();
                    let connecting_movies_map = get_movie_titles_by_ids(&conn, &connecting_movie_ids)?;
                    let connecting_movie_titles: Vec<&String> = connecting_movies_map.values().collect();
                    let connecting_movie_titles_str: Vec<&str> = connecting_movie_titles.iter().map(|s| s.as_str()).collect();

                    path_string.push_str(&format!("{} starred in {} with {}.\n",
                                                 current_actor_name,
                                                 connecting_movie_titles_str.join(", "), // Join movie titles
                                                 next_actor_name));
                }

                //let last_actor_name = get_actor_name_by_id(&conn, *path.last().unwrap())?.unwrap_or_else(|| "Unknown Actor".to_string());
                println!("{}", path_string);
                println!("{} has a link count of {} to {}", start_actor_name, link_count, target_actor_name);

            }
        },
        None => println!("No link found between '{}' and '{}'.", start_actor_name, target_actor_name),
    }

    Ok(())
}