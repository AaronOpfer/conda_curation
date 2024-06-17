use rattler::default_cache_dir;
use rattler_conda_types::{ChannelInfo, PackageRecord, RepoData};
use rattler_repodata_gateway::fetch;
use reqwest::Client;
use reqwest_middleware::ClientWithMiddleware;
use std::fs;
use std::path::PathBuf;
use url::Url;

pub struct RepodataFilenames {
    pub linux64: PathBuf,
    pub noarch: PathBuf,
}

pub async fn fetch_repodata(
    channel_alias: &str,
) -> Result<RepodataFilenames, Box<dyn std::error::Error>> {
    let lin64_url = Url::parse(&(channel_alias.to_string() + "linux-64/"))?;
    let noarch_url = Url::parse(&(channel_alias.to_string() + "noarch/"))?;
    let client = ClientWithMiddleware::from(Client::new());
    let cache = default_cache_dir()?;
    let opts = fetch::FetchRepoDataOptions {
        ..Default::default()
    };
    let linresult = fetch::fetch_repo_data(lin64_url, client, cache, opts, None).await?;
    let client = ClientWithMiddleware::from(Client::new());
    let cache = default_cache_dir()?;
    let opts = fetch::FetchRepoDataOptions {
        ..Default::default()
    };
    let noarchresult = fetch::fetch_repo_data(noarch_url, client, cache, opts, None).await?;
    Ok(RepodataFilenames {
        linux64: linresult.repo_data_json_path,
        noarch: noarchresult.repo_data_json_path,
    })
}

pub fn filtered_repodata_to_file(
    initial: &RepoData,
    filename: &str,
    predicate: impl Fn(&str) -> bool,
    subdir: &str,
    possible_replacement_base_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut out = initial.clone();
    out.packages
        .retain(|package_filename, _| predicate(package_filename));
    out.conda_packages
        .retain(|package_filename, _| predicate(package_filename));
    if out.base_url().is_none() {
        // In conda's unit tests, they did not include a trailing slash on base_url.
        let url = Some(format!("{possible_replacement_base_url}{subdir}"));
        match out.info {
            None => {
                out.info = Some(ChannelInfo {
                    subdir: subdir.to_string(),
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

#[must_use] pub fn sorted_iter<'a>(repodatas: &[&'a RepoData]) -> Vec<(&'a String, &'a PackageRecord)> {
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
