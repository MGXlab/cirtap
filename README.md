# cirtap

A command-line utility to handle PATRIC data from their FTP

Check out the [wiki page](https://github.com/MGXlab/cirtap/wiki) for more info.

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

## Quickstart 

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

