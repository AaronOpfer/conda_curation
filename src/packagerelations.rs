use crate::matchspeccache::MatchspecCache;
use itertools::Itertools;
use rattler_conda_types::{BuildNumber, MatchSpec, PackageRecord};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::ops::Range;

/// Log item for when a package is removed because of a dependency no longer being satsifiable.
/// Includes the filename of a package that was removed which would have satisfied the test if it
/// still existed, if there is such a package.
pub struct RemovedUnsatisfiableLog<'a> {
    pub filename: &'a str,
    pub package_name: &'a str,
    pub matchspec: &'a MatchSpec,
    pub cause_filename: Option<&'a str>,
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
enum Evaluation<'a> {
    RemoveAndLog((PkgIdx, RemovedUnsatisfiableLog<'a>)),
    UpdateSolution((PkgIdx, usize, PkgIdxOffset)),
}

// We will support 4 billion packages at the most. That should
// be more than enough to last Conda folks a long time
#[derive(Clone, Copy)]
struct PkgIdx {
    index: u32,
}
impl PkgIdx {
    fn index(self) -> usize {
        self.index as usize
    }

    fn range_to(self, offset: PkgIdxOffset) -> Range<usize> {
        {
            self.index()..self.index() + offset.offset()
        }
    }
}

// We will support at most 65K packages with the same name.
// My current analysis suggests the most names provided by
// any package currently is about 8K.
#[derive(Clone, Copy)]
struct PkgIdxOffset {
    offset: u16,
}
impl PkgIdxOffset {
    fn offset(self) -> usize {
        self.offset as usize
    }

    #[must_use]
    fn from_difference(start: PkgIdx, end: usize) -> Self {
        assert!(start.index() <= end, "start must be less than end");
        let diff = end - start.index();
        PkgIdxOffset {
            offset: u16::try_from(diff).expect("too many packages"),
        }
    }
}

struct PackageDependency<'a> {
    matchspec: &'a MatchSpec,
    last_successful_resolution: Option<PkgIdxOffset>,
}

struct PackageMetadata<'a> {
    removed: bool,
    filename: &'a str,
    package_record: &'a PackageRecord,
    // Vector of MatchSpec, and last successful package match.
    dependencies: Vec<PackageDependency<'a>>,
}

pub struct PackageRelations<'a> {
    matchspec_cache: &'a MatchspecCache<'a, 'a>,

    // Sorted by filename. Implies also sorted by packagename.
    // this allows us to use a range system to define packages.
    package_metadatas: Vec<PackageMetadata<'a>>,
    filename_to_metadata: HashMap<&'a str, PkgIdx>,
    // Package Name -> (Start Index, End Index)
    package_name_to_providers: HashMap<&'a str, (PkgIdx, PkgIdxOffset)>,
    // When eliminating a package, we'll want to go "upstream"
    // to eliminate packages whose dependencies may no longer
    // be satisified.
    package_name_to_consumers: HashMap<&'a str, HashSet<&'a str>>,
    // Lazy-populated when a matchspec that matches on build hash is found.
    //package_name_build_to_providers: HashMap<(&'a str, &'a str), Vec<bool>>,
}

impl<'a> PackageRelations<'a> {
    pub fn new(matchspec_cache: &'a MatchspecCache<'a, 'a>) -> Self {
        const CAPACITY: usize = 512 * 1024;
        PackageRelations {
            matchspec_cache,
            package_metadatas: Vec::with_capacity(CAPACITY),
            filename_to_metadata: HashMap::with_capacity(CAPACITY),
            package_name_to_providers: HashMap::with_capacity(CAPACITY),
            package_name_to_consumers: HashMap::with_capacity(CAPACITY),
        }
    }

    pub fn insert(&mut self, filename: &'a str, package_record: &'a PackageRecord) {
        let mut dependencies = Vec::with_capacity(package_record.depends.len());
        let package_name = package_record.name.as_source();
        for depend in &package_record.depends {
            let matchspec = self.matchspec_cache.get_or_insert(depend);
            dependencies.push(PackageDependency {
                matchspec,
                last_successful_resolution: None,
            });
            if let Some(depends_package_name) = &matchspec.name {
                self.package_name_to_consumers
                    .entry(depends_package_name.as_source())
                    .or_default()
                    .insert(package_name);
            }
        }

        let fastpath = match self.package_metadatas.last() {
            Some(last) => last.filename <= filename,
            None => true,
        };
        if !fastpath {
            todo!();
        }
        self.package_metadatas.push(PackageMetadata {
            removed: false,
            filename,
            package_record,
            dependencies,
        });
        let index = u32::try_from(self.package_metadatas.len() - 1).expect("too many packages");

        self.filename_to_metadata.insert(filename, PkgIdx { index });
        if index == 0 {
            self.package_name_to_providers
                .insert(package_name, (PkgIdx { index }, PkgIdxOffset { offset: 1 }));
        } else {
            let value = self
                .package_name_to_providers
                .entry(package_name)
                .or_insert((PkgIdx { index }, PkgIdxOffset { offset: 0 }));
            value.1.offset += 1;
        }
    }

    pub fn apply_build_prune(&mut self) -> Vec<RemovedBySupercedingBuildLog<'a>> {
        let mut result = Vec::new();
        for (_, group) in &self.package_metadatas[..].iter().group_by(|pkg| {
            let r = &pkg.package_record;
            let buildnumstr = r.build_number.to_string();
            let mut build = r.build.clone();
            if build.ends_with(&buildnumstr) {
                build.truncate(build.len() - buildnumstr.len());
            }
            (r.name.as_source(), &r.version, build)
        }) {
            let group: Vec<&PackageMetadata> = group.into_iter().collect();
            if group.len() < 2 {
                continue;
            }
            let big = group[group.len() - 1].package_record.build_number;
            for pkg in &group[..group.len() - 2] {
                if pkg.package_record.build_number < big {
                    result.push(RemovedBySupercedingBuildLog {
                        filename: group[0].filename,
                        package_name: group[0].package_record.name.as_source(),
                        build_number: big,
                    });
                }
            }
        }
        for res in &result {
            self.package_metadatas[self.filename_to_metadata[res.filename].index()].removed = true;
        }
        result
    }

    pub fn apply_matchspecs(
        &mut self,
        package_name: &str,
        specs: &[MatchSpec],
    ) -> Vec<RemovedByUserLog<'a>> {
        let mut result = Vec::new();
        if let Some((start, offset)) = self.package_name_to_providers.get(package_name) {
            for md in &mut self.package_metadatas[start.range_to(*offset)] {
                if md.removed {
                    continue;
                }
                let mut passes = false;

                // Determine if this package should no longer be here
                for spec in specs {
                    if spec.matches(md.package_record) {
                        passes = true;
                        break;
                    }
                }

                if !passes {
                    md.removed = true;
                    result.push(RemovedByUserLog {
                        package_name: md.package_record.name.as_source(),
                        filename: md.filename,
                    });
                }
            }
        }
        result
    }

    fn mkrange(&self, package_name: &str) -> Range<usize> {
        let (start, offset) = self.package_name_to_providers[package_name];
        start.range_to(offset)
    }

    pub fn find_unresolveables(
        &mut self,
        depending_ons: Vec<&'a str>,
    ) -> Vec<RemovedUnsatisfiableLog<'a>> {
        let updates: Vec<Evaluation> = depending_ons
            .into_par_iter()
            .filter_map(|depending_on| {
                self.package_name_to_consumers
                    .get(depending_on)
                    .map(|package_names| (package_names, depending_on))
            })
            .flat_map(|(package_names, depending_on)| {
                package_names.into_par_iter().flat_map(|package_name| {
                    self.mkrange(package_name)
                        .into_par_iter()
                        //.into_iter()
                        .filter_map(|package_index| self.evaluate(depending_on, package_index))
                })
            })
            .collect();
        let mut result = Vec::with_capacity(updates.len());
        for evaluation in updates {
            match evaluation {
                Evaluation::UpdateSolution((package_index, dependency_index, offset)) => {
                    self.package_metadatas[package_index.index()].dependencies[dependency_index]
                        .last_successful_resolution = Some(offset);
                }
                Evaluation::RemoveAndLog((package_index, log_entry)) => {
                    self.package_metadatas[package_index.index()].removed = true;
                    result.push(log_entry);
                }
            }
        }
        result
    }

    fn evaluate(&self, depending_on: &str, package_index: usize) -> Option<Evaluation<'a>> {
        let (candidates_start, candidates_offset) = self.package_name_to_providers[depending_on];
        // Is this package already removed?
        if self.package_metadatas[package_index].removed {
            return None; // Yes, it is
        }
        // Does this package depend on our search criteria?
        let depfind = &self.package_metadatas[package_index]
            .dependencies
            .iter()
            .enumerate()
            .find(|(_, d)| d.matchspec.name.as_ref().unwrap().as_source() == depending_on);
        if depfind.is_none() {
            return None; // No, it does not.
        }
        // Does this dependency have the same solution as before?
        let (dependency_index, dependency) = depfind.unwrap();
        let last = dependency.last_successful_resolution;
        if let Some(offset) = last {
            let candidate_index = candidates_start.index() + offset.offset();
            if !self.package_metadatas[candidate_index].removed {
                return None; // Yes, it does.
            }
        }
        let dependency_matchspec = dependency.matchspec;
        // Does the dependency have a solution?
        let candidate_index = candidates_start.range_to(candidates_offset).find(|index| {
            let md = &self.package_metadatas[*index];
            !md.removed && dependency_matchspec.matches(md.package_record)
        });
        if let Some(candidate_index) = candidate_index {
            // Yes, there is a solution. Save the solution in
            // case we need to return to this dependency later.
            return Some(Evaluation::UpdateSolution((
                PkgIdx {
                    index: u32::try_from(package_index).unwrap(),
                },
                dependency_index,
                PkgIdxOffset::from_difference(candidates_start, candidate_index),
            )));
        }

        // There is no solution.
        // Try to determine the reason for unresolveable.
        let candidate_index = match last {
            // We already know what package previously satisified
            Some(candidate_offset) => Some(candidates_start.index() + candidate_offset.offset()),
            // We need to find the previous package that satisfied
            None => candidates_start.range_to(candidates_offset).find(|index| {
                let md = &self.package_metadatas[*index];
                md.removed && dependency_matchspec.matches(md.package_record)
            }),
        };

        let md = &self.package_metadatas[package_index];
        return Some(Evaluation::RemoveAndLog((
            PkgIdx {
                index: u32::try_from(package_index).unwrap(),
            },
            RemovedUnsatisfiableLog {
                filename: md.filename,
                package_name: md.package_record.name.as_source(),
                matchspec: dependency_matchspec,
                cause_filename: candidate_index.map(|index| self.package_metadatas[index].filename),
            },
        )));
    }
}

#[cfg(test)]
mod tests {
    use crate::matchspeccache::MatchspecCache;
    use crate::packagerelations::PackageRelations;
    use crate::rawrepodata;
    use lazy_static::lazy_static;

    lazy_static! {
        static ref RAW_REPODATA_NOARCH: rawrepodata::RawRepoData =
            rawrepodata::RawRepoData::from_file("noarch_repodata.json")
                .expect("failed to load test data");
        static ref RAW_REPODATA_LINUX64: rawrepodata::RawRepoData =
            rawrepodata::RawRepoData::from_file("linux64_repodata.json")
                .expect("failed to load test data");
    }
    #[test]
    fn raw_repodata_was_populated_noarch() {
        let raw_repodata_noarch = &*RAW_REPODATA_NOARCH;
        assert!(!raw_repodata_noarch.packages.is_empty());
        assert!(!raw_repodata_noarch.packages_conda.is_empty());
    }

    #[test]
    fn raw_repodata_was_populated_linux64() {
        let raw_repodata_linux64 = &*RAW_REPODATA_LINUX64;
        assert!(!raw_repodata_linux64.packages.is_empty());
        assert!(!raw_repodata_linux64.packages_conda.is_empty());
    }

    #[test]
    fn load_packages_into_relations() {
        let matchspeccache = MatchspecCache::with_capacity(1024 * 128);
        let mut relations = PackageRelations::new(&matchspeccache);
        let rdl = &*RAW_REPODATA_LINUX64;
        let rdna = &*RAW_REPODATA_NOARCH;

        for (package_filename, package_record) in rawrepodata::sorted_iter(&[rdl, rdna]) {
            relations.insert(package_filename, package_record);
        }
    }
}
