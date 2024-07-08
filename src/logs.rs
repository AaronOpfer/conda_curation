use rattler_conda_types::{BuildNumber, NamelessMatchSpec};

pub trait Log<'a>: std::fmt::Display {
    fn filename(&self) -> &'a str;
    fn package_name(&self) -> &'a str;
}

/// Log item for when a package is removed because of a dependency no longer being satsifiable.
/// Includes the filename of a package that was removed which would have satisfied the test if it
/// still existed, if there is such a package.
pub struct RemovedUnsatisfiableLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
    pub dependency_package_name: &'a str,
    pub matchspec: &'a NamelessMatchSpec,
    pub cause_filename: Option<&'a str>,
}

impl<'a> std::fmt::Display for RemovedUnsatisfiableLog<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.cause_filename {
            Some(cause_filename) => write!(
                f,
                "{} removed: dependency {} {} unsatisfiable after removal of {}",
                self.filename, self.dependency_package_name, self.matchspec, cause_filename
            ),
            None => write!(
                f,
                "{} removed: dependency {} {} unsatisfiable, seemingly due to no fault of our own",
                self.filename, self.dependency_package_name, self.matchspec
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

impl<'a> std::fmt::Display for RemovedByUserLog<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} removed: failed user matchspec", self.filename)
    }
}

pub struct RemovedBySupercedingBuildLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
    pub build_number: BuildNumber,
}

impl<'a> std::fmt::Display for RemovedBySupercedingBuildLog<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} removed: superceded by build {}",
            self.filename, self.build_number
        )
    }
}

pub struct RemovedByDevRcPolicyLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
}
impl<'a> std::fmt::Display for RemovedByDevRcPolicyLog<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} removed: dev/rc policy", self.filename)
    }
}

pub struct RemovedWithFeatureLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
    pub feature: &'a str,
}
impl<'a> std::fmt::Display for RemovedWithFeatureLog<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} removed: has banned feature {}",
            self.filename, self.feature
        )
    }
}

pub struct RemovedIncompatibleArchitectureLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
    pub virtual_package: &'a str,
    pub actual_architecture: &'a str,
}

impl<'a> std::fmt::Display for RemovedIncompatibleArchitectureLog<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} removed: relies on {} which is impossible in {}",
            self.filename, self.virtual_package, self.actual_architecture
        )
    }
}

macro_rules! impl_Log {
    (for $($t:ty),+) => {
        $(impl<'a> Log<'a> for $t {
            fn filename(&self) -> &'a str {
                self.filename
            }
            fn package_name(&self) -> &'a str {
                self.package_name
            }
        })*
    }
}
impl_Log!(for RemovedWithFeatureLog<'a>, RemovedByDevRcPolicyLog<'a>, RemovedUnsatisfiableLog<'a>, RemovedBecauseIncompatibleLog<'a>, RemovedBySupercedingBuildLog<'a>, RemovedByUserLog<'a>, RemovedIncompatibleArchitectureLog<'a>);
