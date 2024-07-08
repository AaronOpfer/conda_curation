use conda_curation::matchspeccache::MatchspecCache;
use conda_curation::matchspecyaml::get_user_matchspecs;
use conda_curation::packagerelations::PackageRelations;
use conda_curation::rawrepodata;
use conda_curation::rawrepodata::filtered_repodata_to_file;

use rattler_conda_types::RepoData;
use std::collections::HashSet;
use std::time::Instant;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use clap::Parser;
use rayon::prelude::*;

const ARCHITECTURES: &[&str] = &[
    "freebsd-64",
    "linux-32",
    "linux-64",
    "linux-aarch64",
    "linux-armv6l",
    "linux-armv7l",
    "linux-ppc64",
    "linux-ppc64le",
    "linux-riscv64",
    "linux-s390x",
    "osx-64",
    "osx-arm64",
    "win-32",
    "win-64",
    "win-arm64",
    "zos-z",
];

fn architectures_parser(value: &str) -> Result<String, &'static str> {
    if !value
        .chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        Err("Invalid architecture. Must be alphanumeric with hyphen or underscores")
    } else if value.is_empty() {
        Err("cannot be an empty string")
    } else if value == "noarch" {
        Err("noarch may not be specified (it is included implicitly)")
    } else {
        Ok(value.to_string())
    }
}

#[derive(Parser)]
#[command(
    author = "Aaron Opfer",
    about = "Apply various filtering rules to remove packages from a Conda Channel in order to speed up downloads and solutions and/or enforce policy."
)]
#[allow(clippy::struct_excessive_bools, clippy::doc_markdown)]
struct Cli {
    /// remove packages with this feature
    #[arg(short = 'F', long = "ban-feature", value_name = "FEATURE")]
    ban_features: Vec<String>,
    /// remove packages that aren't compatible with any variant of PACKAGE_NAME
    #[arg(
        short = 'C',
        long = "must-compatible-with",
        value_name = "PACKAGE_NAME"
    )]
    must_compatible: Vec<String>,
    /// don't remove development (dev) packages
    #[arg(long = "keep-dev", action=clap::ArgAction::SetFalse)]
    ban_dev: bool,
    /// don't remove release candidate (rc) packages
    #[arg(long = "keep-rc", action=clap::ArgAction::SetFalse)]
    ban_rc: bool,
    /// Base URL for downloading repodata
    #[arg(
        long = "channel-alias",
        default_value = "https://conda.anaconda.org/conda-forge/",
        value_name = "CHANNEL_URL"
    )]
    channel_alias: String,
    /// Use cached repodata and do not make network calls
    #[arg(long = "offline", action=clap::ArgAction::SetTrue)]
    is_offline: bool,
    /// Emit the reasons why packages are being removed.
    #[arg(short = 'e', long = "explain")]
    explain: bool,
    /// Write repodata.json files to the specified directory
    #[arg(short = 'o', long = "output-dir", default_value = "out")]
    output_directory: std::path::PathBuf,
    /// Which architectures to render index information for. If none are specified, will default to
    /// all architectures.
    #[arg(short = 'a', long = "architecture", value_parser = architectures_parser)]
    architectures: Vec<String>,
    matchspecs_yaml: std::path::PathBuf,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = Cli::parse();
    if !args.channel_alias.ends_with('/') {
        args.channel_alias += "/";
    }
    if args.architectures.is_empty() {
        args.architectures
            .extend(ARCHITECTURES.iter().map(|arch| (*arch).to_string()));
    }
    let args = args; // read-only for now on.

    std::fs::create_dir_all(&args.output_directory).expect("Failed to create output directory");

    let banned_features: HashSet<&str> = args.ban_features.iter().map(String::as_str).collect();
    let user_matchspecs = get_user_matchspecs(&args.matchspecs_yaml)
        .expect("Failed to load user-provided matchspecs file");
    let matchspec_cache = MatchspecCache::with_capacity(1024 * 192);

    let rawrepodata::RepodataFilenames {
        noarch: noarch_repodata_fn,
        arches: repodata_fns,
    } = rawrepodata::fetch_repodata(&args.channel_alias, &args.architectures, args.is_offline)
        .await
        .expect("Failed to download repodata");

    let repodata_noarch =
        RepoData::from_path(noarch_repodata_fn).expect("Failed to load noarch repodata");

    let repodatas: Vec<RepoData> = repodata_fns
        .into_par_iter()
        .map(|repodata_fn| RepoData::from_path(repodata_fn).expect("Failed to load repodata"))
        .collect();

    let pairs: Vec<(&RepoData, &String)> =
        repodatas.iter().zip(args.architectures.iter()).collect();

    let common_filtered_fns: HashSet<&str> = pairs
        .into_iter()
        .map(|(repodata_arch, architecture)| {
            println!("{architecture}-----");
            let removed_filenames = filter_repodata(
                architecture,
                &args,
                &matchspec_cache,
                &user_matchspecs,
                &banned_features,
                &repodata_noarch,
                repodata_arch,
            );
            filtered_repodata_to_file(
                repodata_arch,
                &args.output_directory,
                |pkfn| !removed_filenames.contains(pkfn),
                architecture,
                &args.channel_alias,
            )
            .expect("Error writing repodata to file");
            removed_filenames
        })
        .reduce(|left, right| {
            left.intersection(&right)
                .copied()
                .collect::<HashSet<&str>>()
        })
        .unwrap();
    // Rayon Version
    //.reduce(HashSet::<&str>::new, |mut acc, fns| {
    //    acc.extend(fns);
    //    acc
    //})
    //.into_iter()
    //.collect();
    let mut removed = std::collections::HashSet::new();
    filtered_repodata_to_file(
        &repodata_noarch,
        &args.output_directory,
        |pkfn| {
            if common_filtered_fns.contains(pkfn) {
                removed.insert(pkfn);
                false
            } else {
                true
            }
        },
        "noarch",
        &args.channel_alias,
    )
    .expect("Failed writing noarch repodata to file");
    println!(
        "Noarch packages removed: {} of {}",
        removed.len(),
        repodata_noarch.packages.len() + repodata_noarch.conda_packages.len()
    );
}

#[inline]
fn perform_round<'a, F, S, L>(
    label: S,
    action: F,
    removed_filenames: &mut HashSet<&'a str>,
    removed_package_names: &mut HashSet<&'a str>,
    explain: bool,
) where
    S: std::fmt::Display,
    L: conda_curation::logs::Log<'a>,
    F: FnOnce() -> Vec<L>,
{
    let start = Instant::now();
    let mut removal_count = 0;
    for log_entry in action() {
        if removed_filenames.insert(log_entry.filename()) {
            removal_count += 1;
            if explain {
                println!("{log_entry}");
            }
            removed_package_names.insert(log_entry.package_name());
        }
    }
    let duration = start.elapsed().as_secs_f64();
    println!("{label:>15}: - {removal_count:>7} ({duration:>2.7}s)");
}

fn filter_repodata<'a>(
    architecture: &'a str,
    args: &'a Cli,
    matchspec_cache: &'a MatchspecCache<'a, 'a>,
    user_matchspecs: &'a std::collections::HashMap<
        String,
        Vec<rattler_conda_types::NamelessMatchSpec>,
    >,
    banned_features: &HashSet<&str>,
    repodata_noarch: &'a RepoData,
    repodata_arch: &'a RepoData,
) -> HashSet<&'a str> {
    let mut relations = PackageRelations::new();

    for (package_filename, package_record) in
        rawrepodata::sorted_iter(&[repodata_arch, repodata_noarch])
    {
        relations.insert(matchspec_cache, package_filename, package_record);
    }
    relations.shrink_to_fit();
    let (package_count, package_name_count, edges) = relations.stats();
    println!(
        "  package count:   {package_count:>7} ({package_name_count} unique names, {edges} edges)"
    );

    let mut removed_filenames = HashSet::new();
    let mut next_round = HashSet::new();
    perform_round(
        "user matchspecs",
        || relations.apply_user_matchspecs(user_matchspecs),
        &mut removed_filenames,
        &mut next_round,
        args.explain,
    );
    perform_round(
        "old builds",
        || relations.apply_build_prune(),
        &mut removed_filenames,
        &mut next_round,
        args.explain,
    );
    perform_round(
        "features",
        || relations.apply_feature_removal(banned_features),
        &mut removed_filenames,
        &mut next_round,
        args.explain,
    );
    perform_round(
        "dev & rc",
        || relations.apply_dev_rc_ban(args.ban_dev, args.ban_rc),
        &mut removed_filenames,
        &mut next_round,
        args.explain,
    );
    perform_round(
        "incompat arch",
        || relations.apply_incompatible_architecture(architecture),
        &mut removed_filenames,
        &mut next_round,
        args.explain,
    );
    unresolveable(
        &mut relations,
        &mut removed_filenames,
        &next_round,
        args.explain,
    );

    for package_name in &args.must_compatible {
        perform_round(
            format!("compat {package_name}"),
            || relations.apply_must_compatible(package_name),
            &mut removed_filenames,
            &mut next_round,
            args.explain,
        );
        unresolveable(
            &mut relations,
            &mut removed_filenames,
            &next_round,
            args.explain,
        );
    }

    // We want to round up the floating point value that we calculate.
    // Integer division rounds down. So, we'll calculate the percentage
    // of packages we removed, and then subtract 1 from it instead.
    let total_removed_count = removed_filenames.len();
    let remaining_count = package_count - total_removed_count;
    let percent = 100 - (total_removed_count * 100 / package_count);
    println!("=============================================");
    println!("      Remaining:   {remaining_count:>7} ({percent}% of original)");
    removed_filenames
}

fn unresolveable<'a>(
    relations: &mut PackageRelations<'a>,
    removed_filenames: &mut HashSet<&'a str>,
    test_set: &HashSet<&'a str>,
    explain: bool,
) {
    let mut round = 0;
    let mut next_round: HashSet<&'a str> = test_set.clone();
    while !next_round.is_empty() {
        round += 1;
        let this_round = next_round.clone();
        next_round.clear();
        perform_round(
            format!("No Sln Round {round}"),
            || relations.find_unresolveables(this_round.into_iter().collect()),
            removed_filenames,
            &mut next_round,
            explain,
        );
        if next_round.is_empty() {
            break;
        }
    }
}
