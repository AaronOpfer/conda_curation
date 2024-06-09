use rattler_conda_types::{NamelessMatchSpec, ParseStrictness};
use serde_yaml;
use std::collections::HashMap;

pub struct MatchspecYaml {
    /// Package Name -> ["package_name matchspec", ...]
    matchspecs: HashMap<String, Vec<String>>,
}

impl MatchspecYaml {
    pub fn from_file(filename: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let matchspecs: HashMap<String, Vec<String>> =
            serde_yaml::from_str(&std::fs::read_to_string(filename)?)?;

        Ok(MatchspecYaml { matchspecs })
    }

    pub fn matchspecs(
        &self,
    ) -> Result<HashMap<&str, Vec<NamelessMatchSpec>>, Box<dyn std::error::Error>> {
        let mut res = HashMap::with_capacity(self.matchspecs.len());
        for (package_name, values) in &self.matchspecs {
            res.insert(&package_name[..], {
                let mut res = Vec::with_capacity(values.len());
                for value in values {
                    res.push(NamelessMatchSpec::from_str(
                        value,
                        ParseStrictness::Lenient,
                    )?);
                }
                res
            });
        }
        Ok(res)
    }
}
