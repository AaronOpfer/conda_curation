use conda_curation::matchspeccache::MatchspecCache;
use conda_curation::matchspecyaml::MatchspecYaml;
use conda_curation::packagerelations::PackageRelations;
use conda_curation::rawrepodata;
use std::collections::HashSet;

fn main() {
    let yaml_data = MatchspecYaml::from_file("matchspecs.yaml").unwrap();
    let user_matchspecs = yaml_data.matchspecs().unwrap();
    let matchspeccache = MatchspecCache::with_capacity(1024 * 128);

    let (rdna, rdl) = rayon::join(
        || {
            rawrepodata::RawRepoData::from_file("noarch_repodata.json")
                .expect("failed to load test data")
        },
        || {
            rawrepodata::RawRepoData::from_file("linux64_repodata.json")
                .expect("failed to load test data")
        },
    );
    let mut relations = PackageRelations::new(&matchspeccache);

    let package_count = {
        let mut i = 0;
        for (package_filename, package_record) in rawrepodata::sorted_iter(&[&rdl, &rdna]) {
            relations.insert(package_filename, package_record);
            i += 1;
        }
        i
    };
    println!("  package count:   {package_count:>7}");

    let mut removed_filenames = HashSet::new();
    let mut next_round = HashSet::new();
    {
        let mut removal_count = 0;
        for (package_name, user_matchspecs) in &user_matchspecs {
            for log_entry in relations.apply_matchspecs(package_name, &user_matchspecs[..]) {
                if removed_filenames.insert(log_entry.filename) {
                    removal_count += 1;
                }
                next_round.insert(log_entry.package_name);
            }
        }
        println!("user matchspecs: - {removal_count:>7}");
    }
    {
        let mut removal_count = 0;
        for log_entry in relations.apply_build_prune() {
            if removed_filenames.insert(log_entry.filename) {
                removal_count += 1;
            }
            next_round.insert(log_entry.package_name);
        }
        println!("     old builds: - {removal_count:>7}");
    }

    {
        let mut round = 0;
        while !next_round.is_empty() {
            round += 1;
            let mut removal_count = 0;
            let this_round = next_round.clone();
            next_round.clear();

            let mut round_logs = relations.find_unresolveables(this_round.into_iter().collect());
            removal_count += round_logs.len();
            for log_entry in &round_logs {
                if let Some(_cause_filename) = log_entry.cause_filename {
                    if removed_filenames.insert(log_entry.filename) {
                        removal_count += 1;
                    }
                }
                next_round.insert(log_entry.package_name);
            }
            round_logs.sort_unstable_by_key(|l| l.filename);
            if next_round.is_empty() {
                break;
            }
            println!(" No Sln Round {round}: - {removal_count:>7}");
        }
    }
    let total_removed_count = removed_filenames.len();
    let remaining_count = package_count - total_removed_count;
    let percent: f64 = f64::from(remaining_count as u32) / f64::from(package_count as u32);
    let percent = (percent * 100.0).ceil();
    println!("=============================================");
    println!("      Remaining:   {remaining_count:>7} ({percent}% of original)");

    rayon::join(
        || {
            rdl.to_file("linux-64/repodata.json", |pkfn| {
                !removed_filenames.contains(pkfn)
            })
            .expect("failed to write linux repodata");
        },
        || {
            rdna.to_file("noarch/repodata.json", |pkfn| {
                !removed_filenames.contains(pkfn)
            })
            .expect("failed to write noarch repodata");
        },
    );
}
