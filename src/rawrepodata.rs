use futures::{StreamExt, TryStreamExt};
use rattler::default_cache_dir;
use rattler_conda_types::{ChannelInfo, PackageRecord, RepoData};
use rattler_repodata_gateway::fetch;
use rattler_repodata_gateway::fetch::CacheResult;
use reqwest::Client;
use reqwest_middleware::ClientWithMiddleware;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use url::Url;

pub struct RepodataFilenames {
    pub noarch: PathBuf,
    pub arches: Vec<PathBuf>,
}

pub async fn fetch_repodata(
    channel_alias: &str,
    architectures: &[String],
    is_offline: bool,
) -> Result<RepodataFilenames, Box<dyn std::error::Error>> {
    let cache = &default_cache_dir()?;
    let all_architectures = architectures.iter().map(String::as_str).chain(["noarch"]);
    let repodata_urls: Vec<Url> = all_architectures
        .map(|architecture| Url::parse(&(format!("{channel_alias}{architecture}/"))))
        .collect::<Result<Vec<Url>, _>>()?;
    let mut repodata_fns: Vec<PathBuf> = futures::stream::iter(repodata_urls)
        .map(|repodata_url| {
            let client = ClientWithMiddleware::from(Client::new());
            let mut opts = fetch::FetchRepoDataOptions {
                ..Default::default()
            };
            if is_offline {
                opts.cache_action = fetch::CacheAction::ForceCacheOnly;
            }
            async move {
                let result =
                    fetch::fetch_repo_data(repodata_url.clone(), client, cache.clone(), opts, None)
                        .await;
                result.map(|result| {
                    match &result.cache_result {
                        CacheResult::CacheHit | CacheResult::CacheHitAfterFetch => {}
                        CacheResult::CacheOutdated | CacheResult::CacheNotPresent => {
                            println!("fetched {repodata_url}");
                        }
                    }

                    result.repo_data_json_path
                })
            }
        })
        .buffered(20)
        .try_collect()
        .await?;

    let noarch = repodata_fns.pop().unwrap();

    Ok(RepodataFilenames {
        noarch,
        arches: repodata_fns,
    })
}

pub fn filtered_repodata_to_file<'a>(
    initial: &'a RepoData,
    output_dir: &std::path::Path,
    mut predicate: impl FnMut(&'a str) -> bool,
    subdir: &str,
    possible_replacement_base_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // This is like the RepoData from Rattler, except is built out of references.
    #[derive(Debug, Serialize)]
    struct RefRepoData<'a> {
        info: Option<ChannelInfo>,
        packages: HashMap<&'a str, &'a PackageRecord>,
        #[serde(rename = "packages.conda")]
        conda_packages: HashMap<&'a str, &'a PackageRecord>,
        removed: HashSet<&'a str>,
        #[serde(rename = "repodata_version")]
        version: Option<u64>,
    }

    let mut filepath = output_dir.to_path_buf();
    filepath.push(subdir);
    fs::create_dir_all(&filepath).expect("Failed to create directory for arch");
    filepath.push("repodata.json");
    let filename = filepath;

    let mut out = RefRepoData {
        info: initial.info.clone(),
        removed: initial.removed.iter().map(String::as_str).collect(),
        version: initial.version,
        packages: HashMap::with_capacity(initial.packages.len()),
        conda_packages: HashMap::with_capacity(initial.conda_packages.len()),
    };

    out.packages.extend(
        initial
            .packages
            .iter()
            .map(|(pkfn, pr)| (pkfn.as_str(), pr))
            .filter(|(package_filename, _)| predicate(package_filename)),
    );
    out.conda_packages.extend(
        initial
            .conda_packages
            .iter()
            .map(|(pkfn, pr)| (pkfn.as_str(), pr))
            .filter(|(package_filename, _)| predicate(package_filename)),
    );

    if initial.base_url().is_none() {
        // In conda's unit tests, they did not include a trailing slash on base_url.
        let url = Some(format!("{possible_replacement_base_url}{subdir}"));
        match out.info {
            None => {
                out.info = Some(ChannelInfo {
                    subdir: Some(subdir.to_string()),
                    base_url: url,
                });
            }
            Some(ref mut info) => info.base_url = url,
        }
    }
    out.version = Some(2);

    {
        let repodata = serde_json::to_string(&out)?;
        fs::write(filename, repodata)?;
    }

    Ok(())
}

#[must_use]
pub fn sorted_iter<'a>(repodatas: &[&'a RepoData]) -> Vec<(&'a String, &'a PackageRecord)> {
    let mut everything: Vec<(&'a String, &'a PackageRecord)> = repodatas
        .iter()
        .flat_map(|repodata| {
            repodata
                .packages
                .iter()
                .chain(repodata.conda_packages.iter())
        })
        .collect();
    everything.sort_unstable_by(|a, b| {
        a.1.name
            .cmp(&b.1.name)
            .then(a.1.version.cmp(&b.1.version))
            .then(a.0.cmp(b.0))
    });
    everything
}
