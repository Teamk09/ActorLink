use actix_web::{web, App, HttpServer, Responder, HttpResponse}; // Import actix-web items
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
use std::sync::Mutex;
use crate::db::{get_actor_id_by_name, get_actor_name_by_id, get_movie_titles_by_ids, get_movie_ids_for_actor};
use crate::link_finder::find_actor_link_bidirectional_bfs;
use std::collections::HashSet;
use serde::{Serialize, Deserialize}; // Import serde for serialization

async fn ensure_database_exists() -> Result<(), Box<dyn std::error::Error>> {
    if !Path::new("actor_link.db").exists() {
        println!("Database not found. Setting up and populating database...");
        let conn = db::establish_connection()?;
        db::setup_database(&conn)?;
        crate::db_populate::populate_database().await?;
    }
    Ok(())
}

#[derive(Deserialize)] // Struct to deserialize actor names from request
struct ActorLinkRequest {
    start_actor_name: String,
    target_actor_name: String,
}

#[derive(Serialize)] // Struct to serialize the response as JSON
struct ActorLinkResponse {
    path: Option< Vec<String> >,
    link_path: Option< Vec< (String, String, String) > >,
    link_number: Option<usize>,
    error: Option<String>,
}

async fn get_actor_link(
    req: web::Json<ActorLinkRequest>,
    db_conn: web::Data<Mutex<rusqlite::Connection>>,
) -> impl Responder {
    let start_actor_name = &req.start_actor_name;
    let target_actor_name = &req.target_actor_name;

    let conn_mutex = db_conn.lock().unwrap();
    let conn = &conn_mutex;

    let start_actor_id_result = get_actor_id_by_name(conn, start_actor_name);
    let target_actor_id_result = get_actor_id_by_name(conn, target_actor_name);

    match (start_actor_id_result, target_actor_id_result) {
        (Ok(Some(start_actor_id)), Ok(Some(target_actor_id))) => {
            match find_actor_link_bidirectional_bfs(conn, start_actor_id, target_actor_id) {
                Ok(path_option) => {
                    match path_option {
                        Some(path_ids) => {
                            if path_ids.len() == 1 {
                                let actor_name = get_actor_name_by_id(conn, path_ids[0]).unwrap().unwrap();
                                HttpResponse::Ok().json(ActorLinkResponse {
                                    path: Some(vec![actor_name.clone()]),
                                    link_path: Some(vec![]),
                                    link_number: Some(0),
                                    error: None,
                                })
                            } else {
                                let mut actor_names_path: Vec<String> = Vec::new();
                                let mut link_path_details: Vec< (String, String, String) > = Vec::new(); // For detailed path
                                for i in 0..path_ids.len() { // Iterate through actor IDs path
                                    let actor_id = path_ids[i];
                                    let actor_name = get_actor_name_by_id(conn, actor_id).unwrap().unwrap();
                                    actor_names_path.push(actor_name.clone());

                                    if i > 0 {
                                        let current_actor_id = path_ids[i-1];
                                        let next_actor_id = path_ids[i];

                                        let current_actor_movies = get_movie_ids_for_actor(conn, current_actor_id).unwrap();
                                        let next_actor_movies = get_movie_ids_for_actor(conn, next_actor_id).unwrap();
                                        let connecting_movie_ids: HashSet<i64> = current_actor_movies.intersection(&next_actor_movies).cloned().collect();
                                        let connecting_movies_map = get_movie_titles_by_ids(conn, &connecting_movie_ids).unwrap();
                                        let connecting_movie_titles: Vec<&String> = connecting_movies_map.values().collect();
                                        let connecting_movie_titles_str: Vec<&str> = connecting_movie_titles.iter().map(|s| s.as_str()).collect();
                                        let movie_titles_string = connecting_movie_titles_str.join(", ");

                                        let prev_actor_name = get_actor_name_by_id(conn, current_actor_id).unwrap().unwrap();
                                        let next_actor_name = get_actor_name_by_id(conn, next_actor_id).unwrap().unwrap();
                                        link_path_details.push((prev_actor_name, movie_titles_string, next_actor_name));
                                    }
                                }
                                HttpResponse::Ok().json(ActorLinkResponse { // Return path with actor names
                                    path: Some(actor_names_path),
                                    link_path: Some(link_path_details),
                                    link_number: Some(path_ids.len() - 1),
                                    error: None,
                                })
                            }
                        },
                        None => HttpResponse::Ok().json(ActorLinkResponse { // Return no path found
                            path: None,
                            link_path: None,
                            link_number: None,
                            error: Some(format!("No link found between '{}' and '{}'", start_actor_name, target_actor_name)),
                        }),
                    }
                },
                Err(e) => HttpResponse::InternalServerError().json(ActorLinkResponse { // Return error response
                    path: None,
                    link_path: None,
                    link_number: None,
                    error: Some(format!("Error finding actor link: {}", e)),
                }),
            }
        }
        (Err(e), _) | (_, Err(e)) => HttpResponse::InternalServerError().json(ActorLinkResponse { // Return error if actor ID retrieval fails
            path: None,
            link_path: None,
            link_number: None,
            error: Some(format!("Database error when fetching actor ID: {}", e)),
        }),
        (Ok(None), _) => HttpResponse::NotFound().json(ActorLinkResponse { // Return Not Found if start actor is not in DB
            path: None,
            link_path: None,
            link_number: None,
            error: Some(format!("Actor '{}' not found in database.", start_actor_name)),
        }),
        (_, Ok(None)) => HttpResponse::NotFound().json(ActorLinkResponse { // Return Not Found if target actor is not in DB
            path: None,
            link_path: None,
            link_number: None,
            error: Some(format!("Actor '{}' not found in database.", target_actor_name)),
        }),
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    let _api_key = env::var("TMDB_API_KEY").expect("TMDB_API_KEY not set");

    // Ensure database exists and is populated
    ensure_database_exists().await.expect("Failed to ensure database exists");

    let conn = db::establish_connection().expect("Failed to connect to database");
    let db_data = web::Data::new(Mutex::new(conn));

    println!("Starting Actix Web server on port 8080 - with debug prints");
    HttpServer::new(move || {
        App::new()
            .app_data(db_data.clone()) // Share database connection with handler
            .route("/api/actor-link", web::post().to(get_actor_link)) 
    })
    .bind("127.0.0.1:8080")? // Bind server to address and port
    .run() // Run the server
    .await
}