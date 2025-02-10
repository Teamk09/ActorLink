use rusqlite::{Connection, Result};
use std::collections::HashMap;
use std::collections::HashSet;

pub fn establish_connection() -> Result<Connection> {
    Connection::open("actor_link.db")
}

pub fn setup_database(conn: &Connection) -> Result<()> {
    create_actor_table(conn)?;
    create_movie_table(conn)?;
    create_movie_actors_table(conn)?;
    Ok(())
}

fn create_actor_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS actors (
            actor_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            tmdb_actor_id   INTEGER UNIQUE NOT NULL,
            name            TEXT NOT NULL,
            known_for_department TEXT
        )",
        (), // empty parameters
    )?;
    Ok(())
}

fn create_movie_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS movies (
            movie_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            tmdb_movie_id   INTEGER UNIQUE NOT NULL,
            title           TEXT NOT NULL
        )",
        (), // empty parameters
    )?;
    Ok(())
}

fn create_movie_actors_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS movie_actors (
            movie_actor_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            movie_id        INTEGER NOT NULL,
            actor_id        INTEGER NOT NULL,
            FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
            FOREIGN KEY (actor_id) REFERENCES actors(actor_id)
        )",
        (), // empty parameters
    )?;
    Ok(())
}


pub fn insert_actor(conn: &Connection, tmdb_actor_id: u32, name: &str, known_for_department: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO actors (tmdb_actor_id, name, known_for_department) VALUES (?, ?, ?)",
        (tmdb_actor_id, name, known_for_department),
    )?;
    Ok(())
}

pub fn insert_movie(conn: &Connection, tmdb_movie_id: u32, title: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO movies (tmdb_movie_id, title) VALUES (?, ?)",
        (tmdb_movie_id, title),
    )?;
    Ok(())
}

pub fn insert_movie_actor_link(conn: &Connection, movie_id: i64, actor_id: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO movie_actors (movie_id, actor_id) VALUES (?, ?)",
        (movie_id, actor_id),
    )?;
    Ok(())
}

pub fn get_actor_id_by_name(conn: &Connection, actor_name: &str) -> Result<Option<i64>> {
    let mut stmt = conn.prepare("SELECT actor_id FROM actors WHERE name = ?")?;
    let mut rows = stmt.query([actor_name])?;

    if let Some(row) = rows.next()? {
        let actor_id: i64 = row.get(0)?;
        Ok(Some(actor_id))
    } else {
        Ok(None) // Actor not found
    }
}

pub fn get_actor_name_by_id(conn: &Connection, actor_id: i64) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT name FROM actors WHERE actor_id = ?")?;
    let mut rows = stmt.query([actor_id])?;

    if let Some(row) = rows.next()? {
        let actor_name: String = row.get(0)?;
        Ok(Some(actor_name))
    } else {
        Ok(None) // Actor not found
    }
}

pub fn get_movie_titles_by_ids(conn: &Connection, movie_ids: &HashSet<i64>) -> Result<HashMap<i64, String>> {
    let mut movie_titles = HashMap::new();
    for movie_id in movie_ids {
        let mut stmt = conn.prepare("SELECT title FROM movies WHERE movie_id = ?")?;
        let mut rows = stmt.query([movie_id])?;
        if let Some(row) = rows.next()? {
            movie_titles.insert(*movie_id, row.get(0)?);
        }
    }
    Ok(movie_titles)
}

// New function to get movie IDs by actor ID
pub fn get_movie_ids_for_actor(conn: &Connection, actor_id: i64) -> Result<HashSet<i64>> {
    let mut stmt = conn.prepare("SELECT movie_id FROM movie_actors WHERE actor_id = ?")?;
    let mut rows = stmt.query([actor_id])?;
    let mut movie_ids = HashSet::new();
    while let Some(row) = rows.next()? {
        movie_ids.insert(row.get(0)?);
    }
    Ok(movie_ids)
}

// New function to get actor IDs by movie ID
pub fn get_actor_ids_for_movie(conn: &Connection, movie_id: i64) -> Result<HashSet<i64>> {
    let mut stmt = conn.prepare("SELECT actor_id FROM movie_actors WHERE movie_id = ?")?;
    let mut rows = stmt.query([movie_id])?;
    let mut actor_ids = HashSet::new();
    while let Some(row) = rows.next()? {
        actor_ids.insert(row.get(0)?);
    }
    Ok(actor_ids)
}