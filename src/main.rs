use conda_curation::matchspeccache::MatchspecCache;
use conda_curation::matchspecyaml::MatchspecYaml;
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

#[derive(Parser)]
#[command(
    author = "Aaron Opfer",
    about = "Apply various filtering rules to remove packages from a Conda Channel in order to speed up downloads and solutions and/or enforce policy."
)]
struct Cli {
    /// remove packages with this feature
    #[arg(
        short = 'F',
        long = "ban-feature",
        value_name = "FEATURE",
        default_value = "pypy"
    )]
    ban_features: Vec<String>,
    /// remove packages that aren't compatible with any providers of provided package
    #[arg(
        short = 'C',
        long = "compatible-with",
        value_name = "PACKAGE_NAME",
        default_value = "python"
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
        default_value = "https://conda.anaconda.org/conda-forge/"
    )]
    channel_alias: String,
    /// Emit the reasons why packages are being removed.
    #[arg(short = 'e', long = "explain")]
    explain: bool,
    matchspecs_yaml: std::path::PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let banned_features: HashSet<&str> = args.ban_features.iter().map(String::as_str).collect();
    let yaml_data = MatchspecYaml::from_file(&args.matchspecs_yaml.to_str().unwrap()).unwrap();
    let user_matchspecs = yaml_data.matchspecs().unwrap();
    let matchspeccache = MatchspecCache::with_capacity(1024 * 192);

    let rawrepodata::RepodataFilenames {
        noarch: noarch_repodata_fn,
        linux64: linux64_repodata_fn,
    } = rawrepodata::fetch_repodata(&args.channel_alias)
        .await
        .expect("Failed to download repodata");

    let (repodata_noarch, repodata_linux) = rayon::join(
        || RepoData::from_path(&noarch_repodata_fn).expect("failed to load test data"),
        || RepoData::from_path(&linux64_repodata_fn).expect("failed to load test data"),
    );
    let mut relations = PackageRelations::new(&matchspeccache);

    let package_count = {
        let mut i = 0;
        for (package_filename, package_record) in
            rawrepodata::sorted_iter(&[&repodata_linux, &repodata_noarch])
        {
            relations.insert(package_filename, package_record);
            i += 1;
        }
        i
    };
    println!("  package count:   {package_count:>7}");

    let mut removed_filenames = HashSet::new();
    let mut next_round = HashSet::new();
    {
        let start = Instant::now();
        let mut removal_count = 0;
        for (package_name, user_matchspecs) in &user_matchspecs {
            next_round.insert(*package_name);
            let spec_arg: Vec<&rattler_conda_types::NamelessMatchSpec> =
                user_matchspecs.iter().collect();
            for log_entry in relations.apply_matchspecs(package_name, &spec_arg) {
                if removed_filenames.insert(log_entry.filename) {
                    removal_count += 1;
                    if args.explain {
                        println!("{}", log_entry);
                    }
                }
            }
        }
        let duration = start.elapsed().as_secs_f64();
        println!("user matchspecs: - {removal_count:>7} ({duration:>2.7}s)");
    }
    {
        let start = Instant::now();
        let mut removal_count = 0;
        for log_entry in relations.apply_build_prune() {
            if removed_filenames.insert(log_entry.filename) {
                removal_count += 1;
                if args.explain {
                    println!("{}", log_entry);
                }
            }
            next_round.insert(log_entry.package_name);
        }
        let duration = start.elapsed().as_secs_f64();
        println!("     old builds: - {removal_count:>7} ({duration:>2.7}s)");
    }
    {
        let start = Instant::now();
        let mut removal_count = 0;
        for log_entry in relations.apply_feature_removal(banned_features) {
            if removed_filenames.insert(log_entry.filename) {
                removal_count += 1;
                if args.explain {
                    println!("{}", log_entry);
                }
            }
            next_round.insert(log_entry.package_name);
        }
        let duration = start.elapsed().as_secs_f64();
        println!("       features: - {removal_count:>7} ({duration:>2.7}s)");
    }

    {
        let start = Instant::now();
        let mut removal_count = 0;
        for log_entry in relations.apply_dev_rc_ban(args.ban_dev, args.ban_rc) {
            if removed_filenames.insert(log_entry.filename) {
                removal_count += 1;
                if args.explain {
                    println!("{}", log_entry);
                }
            }
            next_round.insert(log_entry.package_name);
        }
        let duration = start.elapsed().as_secs_f64();
        println!("       dev & rc: - {removal_count:>7} ({duration:>2.7}s)");
    }

    unresolveable(
        &mut relations,
        &mut removed_filenames,
        &next_round,
        args.explain,
    );

    for package_name in &args.must_compatible {
        let start = Instant::now();
        let mut removal_count = 0;
        for log_entry in relations.apply_must_compatible(package_name) {
            if removed_filenames.insert(log_entry.filename) {
                removal_count += 1;
                if args.explain {
                    println!("{}", log_entry);
                }
            }
            next_round.insert(log_entry.package_name);
        }
        let duration = start.elapsed().as_secs_f64();
        println!("  compat {package_name}: - {removal_count:>7} ({duration:>2.7}s)");
        unresolveable(
            &mut relations,
            &mut removed_filenames,
            &next_round,
            args.explain,
        );
    }

    let total_removed_count = removed_filenames.len();
    let remaining_count = package_count - total_removed_count;
    let percent = remaining_count as f32 / package_count as f32;
    let percent = (percent * 100.0).ceil();
    println!("=============================================");
    println!("      Remaining:   {remaining_count:>7} ({percent}% of original)");

    rayon::join(
        || {
            filtered_repodata_to_file(
                &repodata_linux,
                "linux-64/repodata.json",
                |pkfn| !removed_filenames.contains(pkfn),
                "linux-64",
                &args.channel_alias,
            )
            .expect("failed to write linux repodata");
        },
        || {
            filtered_repodata_to_file(
                &repodata_noarch,
                "noarch/repodata.json",
                |pkfn| !removed_filenames.contains(pkfn),
                "noarch",
                &args.channel_alias,
            )
            .expect("failed to write noarch repodata");
        },
    );
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
        let start = Instant::now();
        round += 1;
        let mut removal_count = 0;
        let this_round = next_round.clone();
        next_round.clear();

        let mut round_logs = relations.find_unresolveables(this_round.into_iter().collect());
        removal_count += round_logs.len();
        for log_entry in &round_logs {
            if removed_filenames.insert(log_entry.filename) {
                removal_count += 1;
                if explain {
                    println!("{}", log_entry);
                }
            }
            next_round.insert(log_entry.package_name);
        }
        round_logs.sort_unstable_by_key(|l| l.filename);
        if next_round.is_empty() {
            break;
        }
        let duration = start.elapsed().as_secs_f64();
        println!(" No Sln Round {round}: - {removal_count:>7} ({duration:>2.7}s)");
    }
}
