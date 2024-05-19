use rattler_conda_types::{BuildNumber, MatchSpec};

/// Log item for when a package is removed because of a dependency no longer being satsifiable.
/// Includes the filename of a package that was removed which would have satisfied the test if it
/// still existed, if there is such a package.
pub struct RemovedUnsatisfiableLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
    pub matchspec: &'a MatchSpec,
    pub cause_filename: Option<&'a str>,
}

impl<'a> std::fmt::Display for RemovedUnsatisfiableLog<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.cause_filename {
            Some(cause_filename) => write!(
                f,
                "{} removed: dependency {} unsatisfiable after removal of {}",
                self.filename, self.matchspec, cause_filename
            ),
            None => write!(
                f,
                "{} removed: dependency {} unsatisfiable",
                self.filename, self.matchspec
            ),
        }
    }
}

pub struct RemovedBecauseIncompatibleLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
    pub incompatible_with: &'a str,
}

impl<'a> std::fmt::Display for RemovedBecauseIncompatibleLog<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} removed: incompatible with {}",
            self.filename, self.incompatible_with
        )
    }
}

pub struct RemovedByUserLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
}

pub struct RemovedBySupercedingBuildLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
    pub build_number: BuildNumber,
}

pub struct RemovedByDevRcPolicyLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
}

pub struct RemovedWithFeatureLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
    pub feature: &'a str,
}
