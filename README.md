# cirtap

A command-line utility to handle PATRIC data from their FTP

## Description

This pa

## Installation

```
$ pip install cirtap
```

## Usage

```
$ cirtap -h

Usage: cirtap [OPTIONS] COMMAND [ARGS]...

Options:
  -h, --help  Show this message and exit.

Commands:
  mirror  Mirror all data from ftp.patricbrc.org in the specified DB_DIR
```


### <a name="mirror"></a>Subcommand: `mirror`

Mirroring is based on the information available in the `RELEASE_NOTES` directory
on the FTP site.

* Fresh

  - Retrieve and set a version based on the modification date of the file
`genome_summary`.
  - Fetch all available genomes based on the `genome_id` column of the `genome_summary`.
  - Multiple concurrent downloads are supported with the `-j/--jobs` option.
  - Send notifications when a mirror job starts, crashes or finishes.
>Note
>
>This is a public service used by many people so be careful with the number
>of jobs you are setting. Each job starts a new connection.
  - The unit of work is a genome directory. All files found on that directory
are retrieved. This is not configurable in `mirror`. See [the download subcommand](#mirror)
to define which files to download for a genome.
  - An `ftp_info.tsv` is written in each local directory. This file stores
modification times based on polling each genome directory. This is used internally
to handle future invocations.
 - Modification times were chosen over md5sums for gaining some speed. PATRIC
constantly grows in size with more than 400k genome directories avaialable.
Each directory can contain some 10 files so calculating md5sums over all of
these can become prohibitively slow.
- Of course, all of these are subject to change if PATRIC makes this info
available on the FTP site.

>Note
>A `date_modified` column is present in the `genome_summary` file but it is
>unclear what that represents. In general, the `genome_summary` is malformatted
>with missing values for a number of columns, hence cirtap doesn't use it for now.
>A question on that to the PATRIC support is still pending.

Simply run the following command to mirror all data in the directory `MY_PATRIC`.
```
$ cirtap mirror -j 8 MY_PATRIC
```

  - To resume failed jobs add the `-r/--resume` flag to the previous invocation.
```
$ cirtap mirror --resume -j 8 MY_PATRIC
```

  - To send a notification when a cirtap run starts
```
$ cirtap mirror -j 8 --notify jon@example.com,joanne@gmail.com MY_PATRIC
```

where all valid email addresses are comma separated.

* Existing

If you already have used `cirtap mirror` before not much changes. It automatically
tries to figure out whether to download new genomes, update existing ones or
do nothing.

This is all based, again on the `genome_summary` file and the `ftp_info.tsv`
found in each genome directory.

If you already have a mirrored installation that will be used.
