use rattler_conda_types::PackageRecord;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Deserialize)]
pub struct RawRepoData {
    pub packages: indexmap::IndexMap<String, PackageRecord>,
    #[serde(rename = "packages.conda")]
    pub packages_conda: indexmap::IndexMap<String, PackageRecord>,
}

impl RawRepoData {
    pub fn from_file(filename: &str) -> Result<Self, Box<dyn std::error::Error>> {
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
