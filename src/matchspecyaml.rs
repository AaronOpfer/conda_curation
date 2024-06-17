use rattler_conda_types::{NamelessMatchSpec, ParseStrictness};
use serde_yaml;
use std::collections::HashMap;

pub fn get_user_matchspecs(
    filename: &std::path::PathBuf,
) -> Result<HashMap<String, Vec<NamelessMatchSpec>>, Box<dyn std::error::Error>> {
    let matchspecs: HashMap<String, Vec<String>> =
        serde_yaml::from_str(&std::fs::read_to_string(filename)?)?;

    Ok(matchspecs
        .into_iter()
        .map(|(package_name, values)| {
            (
                package_name,
                values
                    .into_iter()
                    .map(|matchspec_string| {
                        NamelessMatchSpec::from_str(
                            matchspec_string.as_str(),
                            ParseStrictness::Lenient,
                        )
                        .expect("parse failure in user matchspec")
                    })
                    .collect(),
            )
        })
        .collect())
}
