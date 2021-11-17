# cirtap

A command-line utility to handle PATRIC data from their FTP

Check out the [wiki page](https://github.com/MGXlab/cirtap/wiki) for more info.

## Installation

```
$ pip install cirtap
```

## Usage

```
Usage: cirtap [OPTIONS] COMMAND [ARGS]...

  Run `cirtap COMMAND -h` for subcommand help

Options:
  --version   Show the version and exit.
  --help  Show this message and exit.

Commands:
  best     Select best genomes based on stats retrieved from genome_summary
  collect  Create sequence sets based on the installed files
  index    Create an index of contents for all directories
  mirror   Mirror all data from ftp.patricbrc.org in the specified DB_DIR
  pack     Create a gzipped tar archive from a list of genome ids in a file
```

## Quickstart


### mirror

* Start a new mirror of all data in a local path, wiht 8 parallel downloads

```
$ cirtap mirror -j 8 some/path

```

* Resume a failing job

```
$ cirtap mirror -j 8 -r some/path
```

* Archive previous release notes and send notification emails when a mirror job launches or fails
to some users

```
$ cirtap mirror -j 8 --notify user1@example.com,user2@gmail.com --archive-notes some/path
```
---

**The rest of the commands assume a mirror is set up**


### index
* Create a presence/absence index of files installed

```
$ cirtap index -j 16 path/to/genomes index.tsv
```
This is useful for selecting genomes based on file presence


### collect

* Collect all proteins for e.g. building a `blastp` database

```
$ cirtap collect -t proteins -j 4 -i path/to/index.tsv path/to/genomes all_proteins.fa.gz
```

* Collect all 16S SSU sequences

```
$ cirtap collect -t SSU -j 4 -i path/to/index.tsv path/to/genomes SSU.fa.gz
```

### best

* Make a selection of best genomes based on completeness, consistency,
fine/coarse consistency.

```
$ cirtap best -i path/to/index.tsv -d path/to/local/patric best_genomes
```
