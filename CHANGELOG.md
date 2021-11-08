# Changelog

## 0.2.1

- Initial working version for testing packaging and distribution through pip
and conda

## 0.2.2

- New subcommand collect, to create sequence sets for proteins and 16S SSU
from installed data
- New subcommand best, to create tables of best genomes per taxonomic rank
based on the available metadata from `genome_summary`
- New subcommand pack to create a gzipped archive from a provided list of
genome ids in a file.
- Fix mirror for better error handling and retrying

