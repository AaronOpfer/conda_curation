use rattler::default_cache_dir;
use rattler_conda_types::PackageRecord;
use rattler_repodata_gateway::fetch;
use reqwest::Client;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use url::Url;

#[derive(Deserialize)]
pub struct RawRepoData {
    pub packages: indexmap::IndexMap<String, PackageRecord>,
    #[serde(rename = "packages.conda")]
    pub packages_conda: indexmap::IndexMap<String, PackageRecord>,
}

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

impl RawRepoData {
    pub fn from_file(filename: &PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(serde_json::from_str(&fs::read_to_string(filename)?)?)
    }

    pub fn to_file(
        &self,
        filename: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Serialize)]
        struct SerializeTemp<'a> {
            pub packages: indexmap::IndexMap<&'a str, &'a PackageRecord>,
            #[serde(rename = "packages.conda")]
            pub packages_conda: indexmap::IndexMap<&'a str, &'a PackageRecord>,
        }

        let mut out: SerializeTemp = SerializeTemp {
            packages: indexmap::IndexMap::new(),
            packages_conda: indexmap::IndexMap::new(),
        };
        for (pkfn, record) in &self.packages {
            if predicate(pkfn) {
                out.packages.insert(pkfn, record);
            }
        }
        for (pkfn, record) in &self.packages_conda {
            if predicate(pkfn) {
                out.packages_conda.insert(pkfn, record);
            }
        }

        {
            let repodata = serde_json::to_string(&out)?;
            fs::write(filename, repodata)?;
        }

        Ok(())
    }
}

type IterItem<'a> = (&'a String, &'a PackageRecord);

pub fn sorted_iter<'a>(repodatas: &[&'a RawRepoData]) -> impl Iterator<Item = IterItem<'a>> {
    let mut arg = Vec::with_capacity(repodatas.len() * 2);
    for repodata in repodatas {
        arg.push(repodata.packages.iter());
        arg.push(repodata.packages_conda.iter());
    }
    itertools::kmerge(arg)
}
