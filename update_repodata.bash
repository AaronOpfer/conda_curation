#!/bin/bash
curl \
	--compressed \
	$(test -e noarch_repodata.json && echo '-z noarch_repodata.json') \
	https://conda.anaconda.org/conda-forge/noarch/repodata.json \
	-o noarch_repodata.json \
	$(test -e linux64_repodata.json && echo '-z linux64_repodata.json') \
	https://conda.anaconda.org/conda-forge/linux-64/repodata.json \
	-o linux64_repodata.json

