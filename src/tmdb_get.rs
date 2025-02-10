use reqwest;
use std::time::Duration;
use std::future::Future;
use tokio::time::sleep;

#[derive(Debug, serde::Deserialize)]
pub struct TMDBPerson {
    pub id: u32,
    pub name: String,
    pub known_for_department: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct TMDBCredit {
    pub cast: Vec<TMDBPerson>,
}

#[derive(Debug, serde::Deserialize)]
pub struct Genre {
    pub id: u32,
    pub name: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct TMDBMovie {
    pub id: u32,
    pub title: String,
    #[serde(rename = "media_type")]
    pub media_type: Option<String>,
    pub adult: bool,
    // Using Option since some older movies might not have this field
    #[serde(rename = "release_date")]
    pub release_date: Option<String>,
    pub video: bool,
    pub genres: Vec<Genre>,
}

async fn debug_log_response(body_text: &str, movie_id: u32) -> Result<(), reqwest::Error> {
    eprintln!(
        "Raw response body for movie ID {}:\n{}",
        movie_id, body_text
    ); // Print the raw body
    Ok(())
}

pub async fn get_movie_details(
    movie_id: u32,
    api_key: &str,
) -> Result<TMDBMovie, Box<dyn std::error::Error>> {
    let url = format!(
        "https://api.themoviedb.org/3/movie/{}?api_key={}",
        movie_id, api_key
    );

    let response = reqwest::get(&url).await?;
    let body_text = response.text().await?;
    Ok(serde_json::from_str(&body_text)?)
}

pub async fn get_movie_credits(
    movie_id: u32,
    api_key: &str,
) -> Result<TMDBCredit, Box<dyn std::error::Error>> {  // Changed return type
    let url = format!(
        "https://api.themoviedb.org/3/movie/{}/credits?api_key={}",
        movie_id, api_key
    );

    let response = reqwest::get(&url).await?;
    let body_text = response.text().await?;

    // --- Debugging function call (can be commented out) ---
    //debug_log_response(&body_text, movie_id).await?;
    // -----------------------------------------------------

    Ok(serde_json::from_str(&body_text)?)  // Simplified error handling
}

pub async fn movie_exists(
    movie_id: u32,
    api_key: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let url = format!(
        "https://api.themoviedb.org/3/movie/{}?api_key={}",
        movie_id, api_key
    );

    let client = reqwest::Client::new();
    let response = client.head(&url).send().await?;
    Ok(response.status().is_success())
}

async fn with_retry<F, Fut, T>(f: F) -> Result<T, Box<dyn std::error::Error>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, Box<dyn std::error::Error>>>,
{
    let mut attempts = 0;
    loop {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts += 1;
                if attempts >= 3 {
                    return Err(e);
                }
                sleep(Duration::from_millis(1000)).await;
            }
        }
    }
}

pub async fn movie_exists_with_client(
    movie_id: u32,
    api_key: &str,
    client: &reqwest::Client,
) -> Result<bool, Box<dyn std::error::Error>> {
    let url = format!(
        "https://api.themoviedb.org/3/movie/{}?api_key={}",
        movie_id, api_key
    );
    let response = client.head(&url).send().await?;
    Ok(response.status().is_success())
}

pub async fn get_movie_details_with_client(
    movie_id: u32,
    api_key: &str,
    client: &reqwest::Client,
) -> Result<TMDBMovie, Box<dyn std::error::Error>> {
    let url = format!(
        "https://api.themoviedb.org/3/movie/{}?api_key={}",
        movie_id, api_key
    );
    let response = client.get(&url).send().await?;
    let body_text = response.text().await?;
    Ok(serde_json::from_str(&body_text)?)
}

pub async fn is_feature_film_with_client(
    movie_id: u32,
    api_key: &str,
    client: &reqwest::Client,
) -> Result<bool, Box<dyn std::error::Error>> {
    let movie_details = get_movie_details_with_client(movie_id, api_key, client).await?;

    if movie_details.adult || movie_details.video || movie_details.release_date.is_none() {
        return Ok(false);
    }

    if movie_details.genres.iter().any(|genre| genre.id == 10770 || genre.id == 99) {
        return Ok(false);
    }

    Ok(true)
}