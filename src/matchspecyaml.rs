use rattler_conda_types::{MatchSpec, ParseStrictness};
use serde_yaml;
use std::collections::HashMap;

pub struct MatchspecYaml {
    /// Package Name -> ["package_name matchspec", ...]
    matchspecs: HashMap<String, Vec<String>>,
}

impl MatchspecYaml {
    pub fn from_file(filename: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut matchspecs: HashMap<String, Vec<String>> =
            serde_yaml::from_str(&std::fs::read_to_string(filename)?)?;
        for (pkgname, values) in &mut matchspecs {
            for value in values.iter_mut() {
                *value = format!("{pkgname} {value}");
            }
        }
        Ok(MatchspecYaml { matchspecs })
    }

    pub fn matchspecs(&self) -> Result<HashMap<&str, Vec<MatchSpec>>, Box<dyn std::error::Error>> {
        let mut res = HashMap::with_capacity(self.matchspecs.len());
        for (package_name, values) in &self.matchspecs {
            res.insert(&package_name[..], {
                let mut res = Vec::with_capacity(values.len());
                for value in values {
                    res.push(MatchSpec::from_str(value, ParseStrictness::Lenient)?);
                }
                res
            });
        }
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use crate::matchspecyaml::MatchspecYaml;
    use rattler_conda_types::{MatchSpec, PackageName};
    use std::collections::HashMap;
    #[test]
    fn matchspec_parses() {
        let mut matchspecs = HashMap::new();
        matchspecs.insert("python".to_string(), {
            let mut res = Vec::new();
            res.push("python >=3.6,<3.7".to_string());
            res.push("python >=3.7,<3.8".to_string());
            res
        });
        matchspecs.insert("pyyaml".to_string(), {
            let mut res = Vec::new();
            res.push("pyyaml =5.4.1".to_string());
            res
        });
        let yaml = MatchspecYaml { matchspecs };
        let matchspecs = yaml.matchspecs().unwrap();
        let matchspec: &MatchSpec = &matchspecs.get("python").unwrap()[0];
        assert_eq!(matchspec.name, Some(PackageName::new_unchecked("python")));
    }
}
