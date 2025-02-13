use rusqlite::{Connection, Result};
use std::collections::{HashSet, VecDeque, HashMap};

// Function to get actor IDs from a movie ID
fn get_actor_ids_for_movie(conn: &Connection, movie_id: i64) -> Result<HashSet<i64>> {
    let mut stmt = conn.prepare("SELECT actor_id FROM movie_actors WHERE movie_id = ?")?;
    let mut rows = stmt.query([movie_id])?;
    let mut actor_ids = HashSet::new();
    while let Some(row) = rows.next()? {
        actor_ids.insert(row.get(0)?);
    }
    Ok(actor_ids)
}

// Function to get movie IDs from an actor ID
fn get_movie_ids_for_actor(conn: &Connection, actor_id: i64) -> Result<HashSet<i64>> {
    let mut stmt = conn.prepare("SELECT movie_id FROM movie_actors WHERE actor_id = ?")?;
    let mut rows = stmt.query([actor_id])?;
    let mut movie_ids = HashSet::new();
    while let Some(row) = rows.next()? {
        movie_ids.insert(row.get(0)?);
    }
    Ok(movie_ids)
}


pub fn find_actor_link_bidirectional_bfs(conn: &Connection, start_actor_id: i64, target_actor_id: i64) -> Result<Option<Vec<i64>>> {
    if start_actor_id == target_actor_id {
        return Ok(Some(vec![start_actor_id])); // Same actor, direct path
    }

    let mut forward_queue = VecDeque::new();
    let mut backward_queue = VecDeque::new();
    let mut forward_visited = HashSet::new();
    let mut backward_visited = HashSet::new();
    let mut forward_path = HashMap::new(); // actor_id -> parent_actor_id in forward search
    let mut backward_path = HashMap::new(); // actor_id -> parent_actor_id in backward search

    forward_queue.push_back(start_actor_id);
    backward_queue.push_back(target_actor_id);
    forward_visited.insert(start_actor_id);
    backward_visited.insert(target_actor_id);

    while !forward_queue.is_empty() && !backward_queue.is_empty() {
        // --- Forward BFS Level ---
        let forward_level_size = forward_queue.len(); // Process current level
        for _ in 0..forward_level_size {
            if let Some(current_actor_id) = forward_queue.pop_front() {
                let movie_ids = get_movie_ids_for_actor(conn, current_actor_id)?;
                for movie_id in movie_ids {
                    let actor_ids = get_actor_ids_for_movie(conn, movie_id)?;
                    for neighbor_actor_id in actor_ids {
                        if !forward_visited.contains(&neighbor_actor_id) {
                            forward_visited.insert(neighbor_actor_id);
                            forward_path.insert(neighbor_actor_id, current_actor_id);
                            forward_queue.push_back(neighbor_actor_id);

                            if backward_visited.contains(&neighbor_actor_id) {
                                // Intersection found! Construct path
                                return construct_path(neighbor_actor_id, &forward_path, &backward_path, start_actor_id, target_actor_id);
                            }
                        }
                    }
                }
            }
        }

        // --- Backward BFS Level ---
        let backward_level_size = backward_queue.len(); // Process current level
        for _ in 0..backward_level_size {
            if let Some(current_actor_id) = backward_queue.pop_front() {
                let movie_ids = get_movie_ids_for_actor(conn, current_actor_id)?;
                for movie_id in movie_ids {
                    let actor_ids = get_actor_ids_for_movie(conn, movie_id)?;
                    for neighbor_actor_id in actor_ids {
                        if !backward_visited.contains(&neighbor_actor_id) {
                            backward_visited.insert(neighbor_actor_id);
                            backward_path.insert(neighbor_actor_id, current_actor_id);
                            backward_queue.push_back(neighbor_actor_id);

                            if forward_visited.contains(&neighbor_actor_id) {
                                // Intersection found! Construct path
                                return construct_path(neighbor_actor_id, &forward_path, &backward_path, start_actor_id, target_actor_id);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(None) // No link found after exploring all reachable actors
}


fn construct_path(
    intersection_actor_id: i64,
    forward_path: &HashMap<i64, i64>,
    backward_path: &HashMap<i64, i64>,
    start_actor_id: i64,
    target_actor_id: i64,
) -> Result<Option<Vec<i64>>> {
    let mut path = Vec::new();

    // --- Construct path from start actor to intersection actor ---
    let mut current_id = intersection_actor_id;
    path.push(current_id);
    while current_id != start_actor_id {
        if let Some(parent_id) = forward_path.get(&current_id) { // Use if let to handle Option correctly
            current_id = *parent_id;
            path.push(current_id);
        } else {
            // This should not happen in a correctly constructed path, but handle error case
            return Ok(None); // Indicate path construction failure
        }
    }
    path.reverse(); // Path is constructed backwards, so reverse it

    // --- Construct path from intersection actor to target actor ---
    let mut current_id = intersection_actor_id;
    let mut backward_path_segment = Vec::new();
    while current_id != target_actor_id {
        if let Some(parent_id) = backward_path.get(&current_id) { // Use if let to handle Option correctly
            current_id = *parent_id;
            backward_path_segment.push(current_id);
        } else {
            // This should not happen in a correctly constructed path, but handle error case
            return Ok(None); // Indicate path construction failure
        }
    }

    path.extend(backward_path_segment); // Append the backward path segment

    Ok(Some(path))
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    #[test]
    fn test_get_actor_ids_for_movie() -> Result<()> {
        let conn = db::establish_connection()?;
        // Assuming movie_id 1 (Fight Club) has actors
        let actor_ids = get_actor_ids_for_movie(&conn, 1)?;
        assert!(!actor_ids.is_empty());
        Ok(())
    }

    #[test]
    fn test_get_movie_ids_for_actor() -> Result<()> {
        let conn = db::establish_connection()?;
        // Assuming actor_id 2 (Brad Pitt) has movies
        let movie_ids = get_movie_ids_for_actor(&conn, 2)?;
        assert!(!movie_ids.is_empty());
        Ok(())
    }

    #[test]
    fn test_find_actor_link_bidirectional_bfs_same_actor() -> Result<()> {
        let conn = db::establish_connection()?;
        let path = find_actor_link_bidirectional_bfs(&conn, 2, 2)?; // Brad Pitt to Brad Pitt
        assert!(path.is_some());
        assert_eq!(path.unwrap(), vec![2]);
        Ok(())
    }

    #[test]
    fn test_find_actor_link_bidirectional_bfs() -> Result<()> {
        let conn = db::establish_connection()?;
        // Assuming Brad Pitt (2) and Edward Norton (1) are linked (e.g., Fight Club)
        let path_option = find_actor_link_bidirectional_bfs(&conn, 2, 1)?;
        assert!(path_option.is_some());
        if let Some(path) = path_option {
            println!("Path found: {:?}", path);
            assert!(path.contains(&2));
            assert!(path.contains(&1));
            assert!(path.len() <= 3); // Expecting a short path
        }
        Ok(())
    }

    #[test]
    fn test_find_actor_link_specific_actors() -> Result<()> {
        let conn = db::establish_connection()?;

        // Assuming database has data for these actors and "Fight Club"
        let edward_norton_id = 1; // Replace with actual ID from your DB
        let helena_bonham_carter_id = 3; // Replace with actual ID

        // Test case: Edward Norton -> Helena Bonham Carter (both in Fight Club with Brad Pitt)
        let path = find_actor_link_bidirectional_bfs(&conn, edward_norton_id, helena_bonham_carter_id)?;
        assert!(path.is_some());
        let path = path.unwrap();
        println!("Path found: {:?}", path);
        assert!(path.contains(&edward_norton_id));
        assert!(path.contains(&helena_bonham_carter_id));
        assert!(path.len() <= 3); // Expecting a short path (Norton -> Pitt -> HBC or Norton -> HBC directly if they co-starred in another movie in your DB)

        Ok(())
    }
}