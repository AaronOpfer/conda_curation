use crate::logs::{
    RemovedBecauseIncompatibleLog, RemovedByDevRcPolicyLog, RemovedBySupercedingBuildLog,
    RemovedByUserLog, RemovedIncompatibleArchitectureLog, RemovedUnsatisfiableLog,
    RemovedWithFeatureLog,
};
use crate::matchspeccache::MatchspecCache;
use bitvec::vec::BitVec;
use itertools::Itertools;
use rattler_conda_types::Matches;
use rattler_conda_types::{NamelessMatchSpec, PackageRecord};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::ops::Range;

/// Provided some architecture subdir name, return the virtual packages that are unsatisfiable.
fn get_virtual_package_bans(architecture: &str) -> &'static [&'static str] {
    let mut iter = architecture.splitn(2, '-');
    let os = iter.next();
    if os.is_none() {
        return &[];
    }
    let os = os.unwrap();
    match os {
        "osx" | "freebsd" => &["__linux", "__win", "__glibc"],
        "linux" => &["__osx", "__win"],
        "win" => &["__linux", "__unix", "__glibc", "__osx"],
        _ => {
            eprintln!("subdir {architecture} virtual bans not understood");
            &[]
        }
    }
}

struct DependencyKey<'a> {
    name: &'a str,
    matchspec: &'a str,
}

enum Evaluation<'a> {
    RemoveAndLog(DependencyKey<'a>, Option<PkgIdx>),
    UpdateSolution(DependencyKey<'a>, PkgIdxOffset),
}

// We will support 4 billion packages at the most. That should
// be more than enough to last Conda folks a long time
#[derive(Clone, Copy)]
struct PkgIdx {
    index: u32,
}

impl PkgIdx {
    #[must_use]
    fn from_usize(value: usize) -> Self {
        PkgIdx {
            index: u32::try_from(value).unwrap(),
        }
    }

    #[must_use]
    fn index(self) -> usize {
        self.index as usize
    }

    #[must_use]
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

#[must_use]
fn dependsstr_to_name_and_spec(depend: &str) -> (&str, &str) {
    let dependency_name = depend.split_whitespace().next().unwrap();
    let dependency_spec = if dependency_name.len() == depend.len() {
        ""
    } else {
        &depend[dependency_name.len() + 1..]
    };
    (dependency_name, dependency_spec)
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
    /// If Set, this dependency is permanently unsatisfiable
    unsatisfiable: bool,
    /// What is the matchspec?
    matchspec: &'a NamelessMatchSpec,
    /// What package satisfied this dependency previously (if any)?
    last_successful_resolution: Option<PkgIdxOffset>,
    /// What packages contain this dependency?
    dependers: Vec<PkgIdx>,
}

struct PackageMetadata<'a> {
    filename: &'a str,
    package_record: &'a PackageRecord,
}

pub struct PackageRelations<'a> {
    removed: BitVec,
    package_dependencies: HashMap<&'a str, HashMap<&'a str, PackageDependency<'a>>>,
    // Sorted by filename. Implies also sorted by packagename.
    // this allows us to use a range system to define packages.
    package_metadatas: Vec<PackageMetadata<'a>>,
    filename_to_metadata: HashMap<&'a str, PkgIdx>,
    // Package Name -> (Start Index, End Index)
    package_name_to_providers: HashMap<&'a str, (PkgIdx, PkgIdxOffset)>,
    // TODO
    // Lazy-populated when a matchspec that matches on build hash is found.
    //package_name_build_to_providers: HashMap<(&'a str, &'a str), Vec<bool>>,
}

impl<'a> Default for PackageRelations<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> PackageRelations<'a> {
    #[must_use]
    pub fn new() -> Self {
        const VERSIONS_CAPACITY: usize = 768 * 1024;
        const PROVIDERS_CAPACITY: usize = 32 * 1024;
        PackageRelations {
            removed: bitvec::vec::BitVec::with_capacity(VERSIONS_CAPACITY),
            package_dependencies: HashMap::with_capacity(PROVIDERS_CAPACITY),
            package_metadatas: Vec::with_capacity(VERSIONS_CAPACITY),
            filename_to_metadata: HashMap::with_capacity(VERSIONS_CAPACITY),
            package_name_to_providers: HashMap::with_capacity(PROVIDERS_CAPACITY),
        }
    }

    #[must_use]
    pub fn stats(&self) -> (usize, usize, usize) {
        let edges = self.package_dependencies.values().map(HashMap::len).sum();
        (
            self.package_metadatas.len(),
            self.package_dependencies.len(),
            edges,
        )
    }

    pub fn insert(
        &mut self,
        matchspec_cache: &'a MatchspecCache<'a, 'a>,
        filename: &'a str,
        package_record: &'a PackageRecord,
    ) {
        let package_name = package_record.name.as_source();
        self.package_metadatas.push(PackageMetadata {
            filename,
            package_record,
        });
        self.removed.push(false);
        let index = PkgIdx {
            index: u32::try_from(self.package_metadatas.len() - 1).expect("too many packages"),
        };
        if index.index == 0 {
            self.package_name_to_providers
                .insert(package_name, (index, PkgIdxOffset { offset: 1 }));
        } else {
            let value = self
                .package_name_to_providers
                .entry(package_name)
                .or_insert((index, PkgIdxOffset { offset: 0 }));
            value.1.offset += 1;
        }
        self.filename_to_metadata.insert(filename, index);

        for depend in &package_record.depends {
            let (dependency_name, dependency_spec) = dependsstr_to_name_and_spec(depend);
            let matchspec = matchspec_cache
                .get_or_insert(dependency_spec)
                .expect(depend);

            let dependency = self
                .package_dependencies
                .entry(dependency_name)
                .or_default()
                .entry(dependency_spec)
                .or_insert_with(|| PackageDependency {
                    unsatisfiable: false,
                    matchspec,
                    last_successful_resolution: None,
                    dependers: Vec::new(),
                });
            dependency.dependers.push(index);
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.removed.shrink_to_fit();
        self.package_metadatas.shrink_to_fit();
        self.filename_to_metadata.shrink_to_fit();
        self.package_name_to_providers.shrink_to_fit();
        self.package_dependencies.shrink_to_fit();
        for matchspec_map in self.package_dependencies.values_mut() {
            matchspec_map.shrink_to_fit();
        }
    }

    pub fn apply_build_prune(&mut self) -> Vec<RemovedBySupercedingBuildLog<'a>> {
        let mut result = Vec::new();
        let pattern = regex::Regex::new(r".*h[\da-zA-Z]{7}.+\d").unwrap();
        for (_, packages) in &self.package_metadatas[..]
            .iter()
            .filter(|pkg| {
                let build = &pkg.package_record.build;
                pattern.is_match(build)
            })
            .chunk_by(|pkg| {
                let r = &pkg.package_record;
                let buildnumstr = r.build_number.to_string();
                let mut build: &str = &r.build;
                if build.ends_with(&buildnumstr) {
                    build = &build[0..(build.len() - buildnumstr.len())];
                }
                (r.name.as_source(), &r.version, build)
            })
        {
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
            self.removed
                .set(self.filename_to_metadata[res.filename].index(), true);
        }
        result
    }

    pub fn apply_feature_removal(
        &mut self,
        features: &HashSet<&str>,
    ) -> Vec<RemovedWithFeatureLog<'a>> {
        if features.is_empty() {
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
            self.removed
                .set(self.filename_to_metadata[res.filename].index(), true);
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
            self.removed
                .set(self.filename_to_metadata[res.filename].index(), true);
        }
        result
    }

    pub fn apply_incompatible_architecture(
        &mut self,
        architecture: &'a str,
    ) -> Vec<RemovedIncompatibleArchitectureLog<'a>> {
        let result: Vec<RemovedIncompatibleArchitectureLog<'a>> =
            (*get_virtual_package_bans(architecture))
                .into_par_iter()
                .copied()
                .filter_map(|depending_on| {
                    self.package_dependencies
                        .get(depending_on)
                        .map(|d| (depending_on, d))
                })
                .flat_map(|(dependency_name, dependencies)| {
                    dependencies
                        .par_iter()
                        .flat_map(|(_, dependency)| dependency.dependers.par_iter())
                        .map(|pkgindex| {
                            let package = &self.package_metadatas[pkgindex.index()];
                            RemovedIncompatibleArchitectureLog {
                                filename: package.filename,
                                package_name: package.package_record.name.as_source(),
                                virtual_package: dependency_name,
                                actual_architecture: architecture,
                            }
                        })
                })
                .collect();
        // Mark the packages as removed
        for res in &result {
            self.removed
                .set(self.filename_to_metadata[res.filename].index(), true);
        }
        // Mark the dependencies as unresolveable
        for virtual_package_name in get_virtual_package_bans(architecture) {
            if let Some(matchspec_map) = self.package_dependencies.get_mut(virtual_package_name) {
                for dependency in matchspec_map.values_mut() {
                    dependency.unsatisfiable = true;
                }
            }
        }
        result
    }

    pub fn apply_user_matchspecs(
        &mut self,
        user_matchspecs: &HashMap<String, Vec<NamelessMatchSpec>>,
    ) -> Vec<RemovedByUserLog<'a>> {
        let mut result = Vec::new();
        for (package_name, specs) in user_matchspecs {
            let spec_arg: Vec<&NamelessMatchSpec> = specs.iter().collect();
            result.append(&mut (self.apply_matchspecs(package_name, &spec_arg)));
        }
        result
    }

    fn apply_matchspecs(
        &mut self,
        package_name: &str,
        specs: &[&NamelessMatchSpec],
    ) -> Vec<RemovedByUserLog<'a>> {
        let mut result = Vec::new();
        if let Some((start, offset)) = self.package_name_to_providers.get(package_name) {
            for index in start.range_to(*offset) {
                if self.removed[index] {
                    continue;
                }
                let md = &mut self.package_metadatas[index];
                let mut passes = false;

                // Determine if this package should no longer be here
                for spec in specs {
                    if spec.matches(md.package_record) {
                        passes = true;
                        break;
                    }
                }

                if !passes {
                    self.removed.set(index, true);
                    result.push(RemovedByUserLog {
                        package_name: md.package_record.name.as_source(),
                        filename: md.filename,
                    });
                }
            }
        }
        result
    }

    fn get_dependencies(
        &self,
        index: usize,
    ) -> impl Iterator<Item = (&'a str, &PackageDependency<'a>)> {
        self.package_metadatas[index]
            .package_record
            .depends
            .iter()
            .map(|depend| {
                let (dependency_name, dependency_spec) = dependsstr_to_name_and_spec(depend);
                (
                    dependency_name,
                    &self.package_dependencies[dependency_name][dependency_spec],
                )
            })
    }

    pub fn apply_must_compatible(
        &mut self,
        package_name: &'a str,
    ) -> Vec<RemovedBecauseIncompatibleLog<'a>> {
        let mut result = Vec::new();

        let mut range = self
            .mkrange(package_name)
            .filter(|index| !self.removed[*index]);

        let mut relevant_packages = HashSet::new();
        let mut relevant_matchspecs = HashMap::new();
        let index = range.next();
        if index.is_none() {
            return result;
        }
        let index = index.unwrap();
        for (name, dependency) in self.get_dependencies(index) {
            relevant_packages.insert(name);
            relevant_matchspecs.insert(name, HashSet::from([dependency.matchspec]));
        }

        for index in range {
            let mut local_relevant_packages = HashSet::new();
            for (name, dependency) in self.get_dependencies(index) {
                if let Some(specs) = relevant_matchspecs.get_mut(name) {
                    specs.insert(dependency.matchspec);
                    local_relevant_packages.insert(name);
                }
            }
            relevant_packages = &relevant_packages & &local_relevant_packages;
            if relevant_packages.is_empty() {
                break;
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
        match self.package_name_to_providers.get(package_name) {
            Some((start, offset)) => start.range_to(*offset),
            None => 0..0,
        }
    }

    pub fn find_all_unresolveables(&mut self) -> Vec<RemovedUnsatisfiableLog<'a>> {
        return self.find_unresolveables(
            self.package_dependencies
                .keys()
                .map(|d| *d)
                .filter(|d| !d.starts_with("__"))
                .collect(),
        );
    }
    pub fn find_unresolveables(
        &mut self,
        depending_ons: Vec<&'a str>,
    ) -> Vec<RemovedUnsatisfiableLog<'a>> {
        let updates: Vec<Evaluation> = depending_ons
            .into_par_iter()
            .filter_map(|depending_on| {
                self.package_dependencies
                    .get(depending_on)
                    .map(|d| (depending_on, d))
            })
            .flat_map(|(dependency_name, dependencies)| {
                dependencies
                    .par_iter()
                    .filter_map(|(matchspec_str, dependency)| {
                        if dependency.unsatisfiable {
                            None
                        } else {
                            self.evaluate(
                                DependencyKey {
                                    name: dependency_name,
                                    matchspec: matchspec_str,
                                },
                                dependency,
                            )
                        }
                    })
            })
            .collect();
        let mut result = Vec::with_capacity(updates.len());
        for evaluation in updates {
            match evaluation {
                Evaluation::UpdateSolution(dep_key, offset) => {
                    self.package_dependencies
                        .get_mut(dep_key.name)
                        .unwrap()
                        .get_mut(dep_key.matchspec)
                        .unwrap()
                        .last_successful_resolution = Some(offset);
                }
                Evaluation::RemoveAndLog(dep_key, offset) => {
                    let dependency = self
                        .package_dependencies
                        .get_mut(dep_key.name)
                        .unwrap()
                        .get_mut(dep_key.matchspec)
                        .unwrap();
                    dependency.unsatisfiable = true;
                    for index in &dependency.dependers {
                        let package = self.package_metadatas.get_mut(index.index()).unwrap();
                        self.removed.set(index.index(), true);
                        result.push(RemovedUnsatisfiableLog {
                            dependency_package_name: dep_key.name,
                            filename: package.filename,
                            package_name: package.package_record.name.as_source(),
                            matchspec: dependency.matchspec,
                            cause_filename: offset
                                .map(|index| self.package_metadatas[index.index as usize].filename),
                        });
                    }
                }
            }
        }
        result
    }

    fn evaluate(
        &self,
        dependency_key: DependencyKey<'a>,
        dependency: &PackageDependency<'a>,
    ) -> Option<Evaluation<'a>> {
        let (candidates_start, candidates_end_offset) = {
            if let Some(result) = self.package_name_to_providers.get(dependency_key.name) {
                *result
            } else {
                (PkgIdx { index: u32::MAX }, PkgIdxOffset { offset: 0 })
            }
        };
        // Does this dependency have the same solution as before?
        let last_successful_resolution = dependency.last_successful_resolution;
        if let Some(offset) = last_successful_resolution {
            let last_solution_index = candidates_start.index() + offset.offset();
            if !self.removed[last_solution_index] {
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
            !self.removed[*index]
                && dependency
                    .matchspec
                    .matches(self.package_metadatas[*index].package_record)
        });
        if let Some(new_solution_index) = new_solution_index {
            // Yes, there is a solution. Save the solution in
            // case we need to return to this dependency later.
            return Some(Evaluation::UpdateSolution(
                dependency_key,
                PkgIdxOffset::from_difference(candidates_start, new_solution_index),
            ));
        }

        // There is no solution.
        // Try to determine the reason for unresolveable.
        let cause_of_removal_index = match last_successful_resolution {
            // We already know what package previously satisified
            Some(offset) => Some(candidates_start.index() + offset.offset()),
            // We need to find the previous package that satisfied
            None => wrap_range_from_middle(candidates_start, candidates_end_offset, None).find(
                |index| {
                    self.removed[*index]
                        && dependency
                            .matchspec
                            .matches(self.package_metadatas[*index].package_record)
                },
            ),
        };

        return Some(Evaluation::RemoveAndLog(
            dependency_key,
            cause_of_removal_index.map(PkgIdx::from_usize),
        ));
    }
}
