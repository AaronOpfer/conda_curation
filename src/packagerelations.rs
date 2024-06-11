use crate::logs::{
    RemovedBecauseIncompatibleLog, RemovedByDevRcPolicyLog, RemovedBySupercedingBuildLog,
    RemovedByUserLog, RemovedUnsatisfiableLog, RemovedWithFeatureLog,
};
use crate::matchspeccache::MatchspecCache;
use itertools::Itertools;
use rattler_conda_types::{NamelessMatchSpec, PackageRecord};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::ops::Range;

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
        self.index()..self.index() + offset.offset()
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

#[inline]
fn wrap_range_from_middle(
    start: PkgIdx,
    end_offset: PkgIdxOffset,
    middle: Option<PkgIdxOffset>,
) -> Range<usize> {
    match middle {
        Some(middle) => start.index() + middle.offset()..start.index() + end_offset.offset(),
        None => start.range_to(end_offset),
    }
}

struct PackageDependency<'a> {
    name: &'a str,
    matchspec: &'a NamelessMatchSpec,
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
        const CAPACITY: usize = 768 * 1024;
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
            let dependency_name = depend.split_whitespace().next().unwrap();
            let matchspec;
            if dependency_name.len() == depend.len() {
                matchspec = self.matchspec_cache.get_or_insert("");
            } else {
                matchspec = self
                    .matchspec_cache
                    .get_or_insert(&depend[dependency_name.len() + 1..]);
            }
            dependencies.push(PackageDependency {
                name: dependency_name,
                matchspec,
                last_successful_resolution: None,
            });
            self.package_name_to_consumers
                .entry(dependency_name)
                .or_default()
                .insert(package_name);
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
        for (_, packages) in &self.package_metadatas[..].iter().chunk_by(|pkg| {
            let r = &pkg.package_record;
            let buildnumstr = r.build_number.to_string();
            let mut build: &str = &r.build;
            if build.ends_with(&buildnumstr) {
                build = &build[0..(build.len() - buildnumstr.len())];
            }
            (r.name.as_source(), &r.version, build)
        }) {
            let packages: Vec<&PackageMetadata> = packages.collect();
            if packages.len() < 2 {
                continue;
            }
            let big = packages[packages.len() - 1].package_record.build_number;
            for pkg in &packages[..packages.len() - 1] {
                if pkg.package_record.build_number < big {
                    result.push(RemovedBySupercedingBuildLog {
                        filename: pkg.filename,
                        package_name: packages[0].package_record.name.as_source(),
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

    pub fn apply_feature_removal(
        &mut self,
        features: HashSet<&str>,
    ) -> Vec<RemovedWithFeatureLog<'a>> {
        if features.len() == 0 {
            let res = Vec::with_capacity(0);
            return res;
        }
        let result: Vec<RemovedWithFeatureLog<'a>> = self
            .package_metadatas
            .par_iter()
            .filter_map(|package| {
                if let Some(feature) = package.package_record.features.as_ref() {
                    if features.contains(feature.as_str()) {
                        return Some(RemovedWithFeatureLog {
                            filename: package.filename,
                            package_name: package.package_record.name.as_source(),
                            feature,
                        });
                    }
                }
                for feature in &package.package_record.track_features {
                    if features.contains(feature.as_str()) {
                        return Some(RemovedWithFeatureLog {
                            filename: package.filename,
                            package_name: package.package_record.name.as_source(),
                            feature,
                        });
                    }
                }
                None
            })
            .collect();
        for res in &result {
            self.package_metadatas[self.filename_to_metadata[res.filename].index()].removed = true;
        }
        result
    }

    pub fn apply_dev_rc_ban(
        &mut self,
        ban_dev: bool,
        ban_rc: bool,
    ) -> Vec<RemovedByDevRcPolicyLog<'a>> {
        if !(ban_dev || ban_rc) {
            let result = Vec::with_capacity(0);
            return result;
        }
        let result: Vec<RemovedByDevRcPolicyLog<'a>> = self
            .package_metadatas
            .par_iter()
            .filter_map(|package| {
                if package
                    .package_record
                    .version
                    .segments()
                    .flat_map(|segment| segment.components())
                    .any(|component| {
                        (ban_dev && component.is_dev())
                            || (ban_rc
                                && component
                                    .as_string()
                                    .is_some_and(|the_str| the_str.starts_with("rc")))
                    })
                {
                    Some(RemovedByDevRcPolicyLog {
                        filename: package.filename,
                        package_name: package.package_record.name.as_source(),
                    })
                } else {
                    None
                }
            })
            .collect();
        for res in &result {
            self.package_metadatas[self.filename_to_metadata[res.filename].index()].removed = true;
        }
        result
    }

    pub fn apply_matchspecs(
        &mut self,
        package_name: &str,
        specs: &[&NamelessMatchSpec],
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

    pub fn apply_must_compatible(
        &mut self,
        package_name: &'a str,
    ) -> Vec<RemovedBecauseIncompatibleLog<'a>> {
        let mut result = Vec::new();
        let mut relevant_packages = HashSet::new();
        let mut relevant_matchspecs = HashMap::new();
        let mut found_first = false;

        for index in self.mkrange(package_name) {
            if self.package_metadatas[index].removed == true {
                continue;
            }
            if found_first == false {
                for dependency in &self.package_metadatas[index].dependencies {
                    let name = dependency.name;
                    relevant_packages.insert(name);
                    let mut matchspecs = HashSet::new();
                    matchspecs.insert(dependency.matchspec);
                    relevant_matchspecs.insert(name, matchspecs);
                }
                found_first = true;
            } else {
                let mut local_relevant_packages = HashSet::new();
                for dependency in &self.package_metadatas[index].dependencies {
                    let name = dependency.name;
                    if let Some(specs) = relevant_matchspecs.get_mut(name) {
                        specs.insert(dependency.matchspec);
                        local_relevant_packages.insert(name);
                    }
                }
                relevant_packages = &relevant_packages & &local_relevant_packages;
                if relevant_packages.len() == 0 {
                    break;
                }
            }
        }

        for package in &relevant_packages {
            let specs = relevant_matchspecs.remove(package).unwrap();
            for item in self.apply_matchspecs(
                package,
                &specs.into_iter().collect::<Vec<&NamelessMatchSpec>>(),
            ) {
                result.push(RemovedBecauseIncompatibleLog {
                    package_name: item.package_name,
                    filename: item.filename,
                    incompatible_with: package_name,
                });
            }
        }

        for package in relevant_packages {
            let mut sub_results = self.apply_must_compatible(package);
            result.append(&mut sub_results);
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
        // Is this package already removed?
        if self.package_metadatas[package_index].removed {
            return None; // Yes, it is
        }
        // Does this package depend on our search criteria?
        let depfind = &self.package_metadatas[package_index]
            .dependencies
            .iter()
            .enumerate()
            .find(|(_, d)| d.name == depending_on);
        if depfind.is_none() {
            return None; // No, it does not.
        }
        let (candidates_start, candidates_end_offset) = {
            if let Some(result) = self.package_name_to_providers.get(depending_on) {
                *result
            } else {
                (
                    PkgIdx {
                        index: self.package_metadatas.len() as u32,
                    },
                    PkgIdxOffset { offset: 0 },
                )
            }
        };
        // Does this dependency have the same solution as before?
        let (dependency_index, dependency) = depfind.unwrap();
        let last_successful_resolution = dependency.last_successful_resolution;
        if let Some(offset) = last_successful_resolution {
            let last_solution_index = candidates_start.index() + offset.offset();
            if !self.package_metadatas[last_solution_index].removed {
                return None; // Yes, it does.
            }
        }
        // Does the dependency have a solution?
        let new_solution_index = wrap_range_from_middle(
            candidates_start,
            candidates_end_offset,
            last_successful_resolution,
        )
        .find(|index| {
            let md = &self.package_metadatas[*index];
            !md.removed && dependency.matchspec.matches(md.package_record)
        });
        if let Some(new_solution_index) = new_solution_index {
            // Yes, there is a solution. Save the solution in
            // case we need to return to this dependency later.
            return Some(Evaluation::UpdateSolution((
                PkgIdx {
                    index: u32::try_from(package_index).unwrap(),
                },
                dependency_index,
                PkgIdxOffset::from_difference(candidates_start, new_solution_index),
            )));
        }

        // There is no solution.
        // Try to determine the reason for unresolveable.
        let cause_of_removal_index = match last_successful_resolution {
            // We already know what package previously satisified
            Some(offset) => Some(candidates_start.index() + offset.offset()),
            // We need to find the previous package that satisfied
            None => wrap_range_from_middle(candidates_start, candidates_end_offset, None).find(
                |index| {
                    let md = &self.package_metadatas[*index];
                    md.removed && dependency.matchspec.matches(md.package_record)
                },
            ),
        };

        let md = &self.package_metadatas[package_index];
        return Some(Evaluation::RemoveAndLog((
            PkgIdx {
                index: u32::try_from(package_index).unwrap(),
            },
            RemovedUnsatisfiableLog {
                filename: md.filename,
                package_name: md.package_record.name.as_source(),
                dependency_package_name: dependency.name,
                matchspec: dependency.matchspec,
                cause_filename: cause_of_removal_index
                    .map(|index| self.package_metadatas[index].filename),
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
