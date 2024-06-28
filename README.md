# `conda_curation`

`conda_curation` is a tool which is designed to filter conda repositories, especially [Conda Forge](https://conda-forge.org/), in order to remove packages based on a variety of kinds of criteria.

## Features

### Filtering

* Remove packages that do not match any of the user-provided matchspecs for that package (for an example, see `matchspecs/secure_python.yaml`)
* Remove packages that have been superceded by new builds (i.e. `python-3.9.18-h12345678_0` is superceded by `python-3.9.18-h12345678_1`, and so the former package is removed)
* Remove `dev` and `rc` packages (i.e. `2.0.0.dev0` or `2.0.0.rc0`).
* Remove packages that track undesired features (i.e. `pypy`, etc)
* Remove packages that are incompatible with any available candidates of another package chosen by the user. For example, if user only selects `python >=3.12`, and specifies `-C python`, then older `openssl` such as `openssl 1.1.1n` will be removed, since `mamba create -n ... openssl==1.1.1n python>=3.12` cannot be solved.
* After applying any/all of the above filters, perform follow-up analysis to find packages which depended on now-removed dependencies, and remove those as well, and apply this recursively. For example, filtering out Python 2.7 will also filter out all builds of numpy that were compiled against Python 2.7.

### Standards

* Supports [CEP-15](https://github.com/conda/ceps/blob/main/cep-15.md) `base_url`: if the source respository (as specified by the `--channel-alias` flag) does not already have a `info.base_url` set, then the output `repodata.json` will have its `info.base_url` set to the `--channel-alias`. If it was set in the original `repodata.json`, then it will be preserved.

  If all clients support CEP-15, then this obviates the need for a proxy server configured to 30x redirect all package requests to the `--channel-alias` destination.

## Applications

`conda_curation` serves a small-to-medium sized enterprise that want to begin using Conda internally and wants to leverage the rich Conda Forge package ecosystem rather than create their own packages or hand-curate.

### Solve Performance

The main reason why `conda_curation` was created was for performance: by reducing the Conda Forge repodata to a smaller size, substantial Conda client performance improvements may be observed. At Chicago Trading Company, a prototype of this repodata-filtering system applied to Conda-Forge reduced `mamba mambabuild` runtimes by about two minutes across a wide variety of pipelines. `mamba create --dry-run` commands were seen to take 10 seconds instead of 20 seconds. Solve failures were also rendered much faster (and cleaner).

### Policy Enforcement

A security team may demand that insecure packages, such as older Python interpreters, CA certificate bundles, OpenSSL versions, etc. are completely unavailable from within the enterprise. `conda_curation` is capable of creating these kinds of policies.


## Alpha Software

There are significant feature limitations of this software, as it was initially only targeting a Minimum Viable Product (MVP) of fitting into a specific point in Chicago Trading Company's artifact delivery. As such, it will be necessary for the user to bring their own HTTP proxy / cache proxy system for serving packages, but also contains a diversion for `.*repodata.*\.json.*` URLs that redirects to the rendered output of `conda_curation`. We have successfully done this using `nginx` with 301 redirects for asset downloads to the artifact server during thest testing phase, and by putting nginx directly in front of the artifact server in the deployment phase.

## History

The original prototype of this tool was developed by myself (@AaronOpfer) at [Chicago Trading Company](https://www.chicagotrading.com/), based on observations from my colleague Bozhao Jiang that hand-crafted "curated" channels caused conda builds to finish several minutes faster than they were previously. The original version was written in Python and, due to its performance issues, reached a hard limit on feature development as the development cycle time lengthened. I rewrote the project in Rust in my free time to create this version, and have received permission to release it to the community under the MIT License.

## Special Thanks

- Bozhao Jiang
- Derek Shoemaker
- Jason Bryan
- Mel Williams

