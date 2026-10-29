//! Deterministic K-means used by `ST_ClusterKMeans`.
//!
//! The objective is the PostgreSQL/PostGIS contract: deterministic farthest-point
//! initialization, weighted Lloyd iterations using the M coordinate of `POINT` inputs as
//! weight, and an optional post-convergence split pass that keeps every cluster radius within
//! `max_radius`. The algorithm is implemented independently from the documented behavior and
//! the observed reference results.

use datafusion::error::{DataFusionError, Result};

use super::algorithms::KMeansObject;

/// Reference implementation iteration limit.
const MAX_ITERATIONS: usize = 1000;

/// A converged K-means solution.
#[derive(Debug)]
pub(super) struct KMeansSolution {
    /// Cluster id per object.
    pub clusters: Vec<usize>,
    /// Cluster centers, one per output cluster.
    pub centers: Vec<KMeansObject>,
    /// Maximum squared distance from each center to its objects.
    pub radii: Vec<f64>,
}

/// Cluster a list of representative objects.
///
/// `k` must be positive and no larger than `objects.len()`.
pub(super) fn cluster(
    objects: &[KMeansObject],
    k: usize,
    max_radius: f64,
) -> Result<KMeansSolution> {
    if k == 0 {
        return Err(DataFusionError::Internal(
            "k-means needs at least one cluster".to_string(),
        ));
    }
    if objects.len() < k {
        return Err(DataFusionError::Internal(format!(
            "k-means received {} objects for {k} clusters",
            objects.len()
        )));
    }
    if objects.is_empty() {
        return Ok(KMeansSolution {
            clusters: Vec::new(),
            centers: Vec::new(),
            radii: Vec::new(),
        });
    }

    kmeans(objects, k, max_radius)
}

fn kmeans(objects: &[KMeansObject], min_k: usize, max_radius: f64) -> Result<KMeansSolution> {
    let mut clusters = vec![0usize; objects.len()];
    let mut centers = initialize(objects, min_k);
    let (_, mut radii) = assign(objects, &mut clusters, &centers, min_k);
    update_means(objects, &clusters, &mut centers, min_k);

    let mut current_k = min_k;
    let mut converged = false;
    'outer: for _ in 0..MAX_ITERATIONS {
        for _ in 0..MAX_ITERATIONS {
            let (is_converged, next_radii) = assign(objects, &mut clusters, &centers, current_k);
            radii = next_radii;
            converged = is_converged;
            if converged {
                break;
            }
            update_means(objects, &clusters, &mut centers, current_k);
        }

        if !converged || max_radius == 0.0 {
            break 'outer;
        }

        let new_k = improve_structure(
            objects,
            &mut clusters,
            &mut centers,
            &mut radii,
            current_k,
            max_radius,
        )?;
        if new_k == current_k {
            break;
        }
        current_k = new_k;
    }

    if !converged {
        return Err(DataFusionError::Execution(format!(
            "lwgeom_cluster_kmeans did not converge after {MAX_ITERATIONS} iterations"
        )));
    }

    centers.truncate(current_k);
    radii.truncate(current_k);
    Ok(KMeansSolution {
        clusters,
        centers,
        radii,
    })
}

/// Deterministic farthest-point initialization.
fn initialize(objects: &[KMeansObject], k: usize) -> Vec<KMeansObject> {
    let mut centers = Vec::with_capacity(k);
    if k < 2 {
        centers.push(objects[0]);
        return centers;
    }

    let mut first = 0usize;
    let mut second = 0usize;
    let mut max_distance = -1.0f64;
    for index in 1..objects.len() {
        let distance_first = objects[index].squared_distance(&objects[first]);
        let distance_second = objects[index].squared_distance(&objects[second]);
        if distance_first > max_distance || distance_second > max_distance {
            if distance_first > distance_second {
                max_distance = distance_first;
                second = index;
            } else {
                max_distance = distance_second;
                first = index;
            }
        }
    }

    centers.push(objects[first]);
    centers.push(objects[second]);

    if k > 2 {
        let mut distances = objects
            .iter()
            .map(|object| object.squared_distance(&centers[0]))
            .collect::<Vec<_>>();
        distances[first] = -1.0;
        distances[second] = -1.0;

        for center_index in 2..k {
            let mut candidate = 0usize;
            let mut candidate_distance = f64::NEG_INFINITY;
            for (index, distance) in distances.iter_mut().enumerate() {
                if *distance < 0.0 {
                    continue;
                }
                let current = objects[index].squared_distance(&centers[center_index - 1]);
                if current < *distance {
                    *distance = current;
                }
                if *distance > candidate_distance {
                    candidate = index;
                    candidate_distance = *distance;
                }
            }
            distances[candidate] = -1.0;
            centers.push(objects[candidate]);
        }
    }

    centers
}

/// Reassign every object to its nearest center and return the cluster radii.
fn assign(
    objects: &[KMeansObject],
    clusters: &mut [usize],
    centers: &[KMeansObject],
    k: usize,
) -> (bool, Vec<f64>) {
    let mut converged = true;
    let mut radii = vec![0.0f64; k];

    for (index, object) in objects.iter().enumerate() {
        let centers = &centers[..k];
        let mut distance = object.squared_distance(&centers[0]);
        let mut cluster = 0usize;
        for (candidate, center) in centers.iter().enumerate().skip(1) {
            let candidate_distance = object.squared_distance(center);
            if candidate_distance < distance {
                distance = candidate_distance;
                cluster = candidate;
            }
        }

        if clusters[index] != cluster {
            converged = false;
            clusters[index] = cluster;
        }
        if radii[cluster] < distance {
            radii[cluster] = distance;
        }
    }

    (converged, radii)
}

/// Refresh cluster centers as weighted means.
fn update_means(
    objects: &[KMeansObject],
    clusters: &[usize],
    centers: &mut [KMeansObject],
    k: usize,
) {
    centers[..k].fill(KMeansObject {
        x: 0.0,
        y: 0.0,
        z: 0.0,
        m: 0.0,
    });

    for (index, object) in objects.iter().enumerate() {
        let center = &mut centers[clusters[index]];
        center.x += object.x * object.m;
        center.y += object.y * object.m;
        center.z += object.z * object.m;
        center.m += object.m;
    }

    for center in centers[..k].iter_mut() {
        if center.m != 0.0 {
            center.x /= center.m;
            center.y /= center.m;
            center.z /= center.m;
        }
    }
}

/// Split clusters whose radius exceeds `max_radius`.
fn improve_structure(
    objects: &[KMeansObject],
    clusters: &mut [usize],
    centers: &mut Vec<KMeansObject>,
    radii: &mut Vec<f64>,
    k: usize,
    max_radius: f64,
) -> Result<usize> {
    if max_radius <= 0.0 {
        return Ok(k);
    }
    let max_radius_squared = max_radius * max_radius;
    let Some(first_cluster_to_split) = (0..k).find(|&cluster| radii[cluster] > max_radius_squared)
    else {
        return Ok(k);
    };

    centers.resize(
        objects.len(),
        KMeansObject {
            x: 0.0,
            y: 0.0,
            z: 0.0,
            m: 0.0,
        },
    );
    radii.resize(objects.len(), 0.0);

    let mut new_k = k;
    for cluster in first_cluster_to_split..k {
        if radii[cluster] <= max_radius_squared {
            continue;
        }

        let members = (0..objects.len())
            .filter(|&index| clusters[index] == cluster)
            .collect::<Vec<_>>();
        if members.len() <= 1 {
            continue;
        }

        let sub_objects = members
            .iter()
            .map(|&index| objects[index])
            .collect::<Vec<_>>();
        let sub_solution = kmeans(&sub_objects, 2, 0.0)?;

        let mut member_index = 0usize;
        for cluster_id in clusters.iter_mut() {
            if *cluster_id == cluster {
                if sub_solution.clusters[member_index] != 0 {
                    *cluster_id = new_k;
                }
                member_index += 1;
            }
        }

        centers[cluster] = sub_solution.centers[0];
        centers[new_k] = sub_solution.centers[1];
        radii[cluster] = sub_solution.radii[0];
        radii[new_k] = sub_solution.radii[1];
        new_k += 1;
    }

    Ok(new_k)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_max_radius_matches_reference() {
        let objects = [
            KMeansObject {
                x: 0.0,
                y: 0.0,
                z: 0.0,
                m: 1.0,
            },
            KMeansObject {
                x: 1.0,
                y: 0.0,
                z: 0.0,
                m: 1.0,
            },
            KMeansObject {
                x: 2.0,
                y: 0.0,
                z: 0.0,
                m: 10000.0,
            },
        ];
        let solution = cluster(&objects, 1, 1.0).unwrap();
        assert_eq!(solution.clusters, vec![1, 0, 0]);
    }
}
