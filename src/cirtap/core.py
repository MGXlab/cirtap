"""
References:
    - https://setuptools.readthedocs.io/en/latest/userguide/entry_point.html
    - https://pip.pypa.io/en/stable/reference/pip_install
"""

import click
import logging
import pathlib
import sys

from cirtap import __version__
from cirtap.mirror import (
    check_release_dir,
    check_cache_dir,
    create_genome_jobs,
)
from cirtap.mirror import mirror_genomes_dir
from cirtap.index import contents, all_data, write_index
from cirtap.mailer import send_start_mail, send_exit_mail

__author__ = "papanikos"
__copyright__ = "papanikos"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


def setup_logging(loglevel, logfile):
    """Setup basic logging

    Args:
      loglevel: str: minimum loglevel for emitting messages
    """
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: {}".format(loglevel))

    hs = [logging.StreamHandler(stream=sys.stderr)]
    if logfile:
        filelogger=logging.FileHandler(logfile, mode="w")
        hs.append(filelogger)

    logformat = "[%(asctime)s - %(levelname)s:%(name)s] %(message)s"

    logging.basicConfig(
        level=numeric_level,
        handlers=hs,
        format=logformat,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@click.group(context_settings=dict(help_option_names=["-h", "--help"]))
@click.version_option(__version__)
def cli():
    """Run `cirtap COMMAND -h` for subcommand help"""
    pass


@click.command()
@click.argument("db_dir", required=True, type=pathlib.Path)
@click.option(
    "--cache-dir",
    type=pathlib.Path,
    help="Directory where cirtap will store some info for its execution. "
    "Subsequent executions rely on it so be careful when you delete",
    required=False,
)
@click.option(
    "-j",
    "--jobs",
    default=1,
    show_default=True,
    help="Number of parallel processes to start for downloading",
)
@click.option(
    "--skip-release-check",
    default=False,
    is_flag=True,
    show_default=True,
    help="Skip checking for RELEASE_NOTES based updates",
)
@click.option(
    "--skip-processed-genomes",
    default=False,
    show_default=True,
    is_flag=True,
    help="Skip checks for already processed genomes as found in the cache.",
)
@click.option(
    "--force-check",
    is_flag=True,
    default=False,
    show_default=True,
    help="Force update the genomes directory regardless of RELEASE_NOTES "
    "outcome",
)
@click.option(
    "-r",
    "--resume",
    is_flag=True,
    default=False,
    show_default=True,
    help="Use this to set both --skip-release-check and "
    "--skip-processed-genomes on. "
    "Useful for resuming a failed run",
)
@click.option(
    "--loglevel",
    default="INFO",
    help="Define loglevel",
    show_default=True,
    required=False,
)
@click.option(
    "--archive-notes",
    is_flag=True,
    default=False,
    show_default=True,
    help="Create an tar.gz archive of the RELEASE_NOTES files in the DB_DIR",
)
@click.option(
    "--notify",
    type=click.STRING,
    required=False,
    help="Comma (,) separated list of emails provided as a string. E.g. "
    "'user1@mail.com,user2@anothermail.com'",
)
@click.option(
    "--progress",
    is_flag=True,
    required=False,
    show_default=True,
    help="(Experimental) Print a progress bar when downloading genomes. This "
    "option cannot be set with `--loglevel debug`. If they are both supplied, "
    "progress will not be shown and the more descriptive debugging messages "
    "will be printed to stderr instead",
)
@click.option(
    "--logfile",
    help="Write logging information in this file",
    show_default=True,
    required=False,
)
def mirror(
    db_dir,
    loglevel,
    cache_dir,
    skip_release_check,
    jobs,
    skip_processed_genomes,
    notify,
    archive_notes,
    resume,
    force_check,
    progress,
    logfile
):
    """Mirror all data from ftp.patricbrc.org in the specified DB_DIR"""

    setup_logging(loglevel, logfile)
    _logger.info("Full command: {}".format(' '.join(sys.argv[:])))
    _logger.info("Version: {}".format(__version__))

    if progress and (loglevel == "debug"):
        _logger.info(
            "Unsetting `progress` option because it conflicts with "
            "`loglevel debug`"
        )
        progress = False

    release_notes_dir = db_dir / pathlib.Path("RELEASE_NOTES")
    genomes_dir = db_dir / pathlib.Path("genomes")

    if not db_dir.exists():
        _logger.info("Fresh mirror in: {}".format(db_dir.resolve()))
        db_dir.mkdir(parents=True)
        release_notes_dir.mkdir()
        genomes_dir.mkdir()

    # Load already processed genomes from cache if found
    # Otherwise processed_genomes is an empty set
    if not cache_dir:
        cache_dir = db_dir / pathlib.Path(".cache")
    processed_genomes = check_cache_dir(cache_dir)

    check_genomes = True
    if not skip_release_check and not resume:
        _logger.info("Checking RELEASE_NOTES status".format(release_notes_dir))
        check_genomes = check_release_dir(
            release_notes_dir, archive=archive_notes
        )

    genome_summary = release_notes_dir / pathlib.Path("genome_summary")

    # Create a list of jobs that can be multiprocessed
    genome_jobs = create_genome_jobs(
        genome_summary, genomes_dir, processed_genomes
    )

    # Do not check already existing genome_ids retrieved from the cache
    # These will not be submitted in the main mirror step
    # Speeds up re-executions if things failed the first time
    # Assumes that an update has not occured in between
    if skip_processed_genomes or resume:
        genome_jobs = [
            job for job in genome_jobs if job not in processed_genomes
        ]

    # Try to notify but don't try too hard
    if notify:
        recipients = [str(i).strip() for i in notify.split(",")]
        try:
            send_start_mail(
                recipients,
                db_dir,
                len(genome_jobs),
            )
        except:
            _logger.debug("Failed to send email")

    # Testing
    #ten_targets = [
    #    "100053.5",
    #    "100.11",
    #    "100053.4",
    #    "100.9",
    #    "1123738.3",
    #    "1000562.3",
    #    "100053.8",
    #    "469009.4",
    #    "1309411.5",
    #    "100053.6",
    #]
    #genome_jobs = [job for job in genome_jobs if job in ten_targets]

    try:
        if (
            len(genome_jobs) != 0 and check_genomes is True
        ) or force_check is True:
            finished_jobs = mirror_genomes_dir(
                genome_jobs, genomes_dir, jobs, progress_bar=progress
            )
            new_genomes_processed = processed_genomes.union(set(finished_jobs))
            processed_genomes_txt = cache_dir / pathlib.Path(
                "processed_genomes.txt"
            )
            # Re-write the file with all new and old ids
            with open(processed_genomes_txt, "w") as fout:
                for genome_id in new_genomes_processed:
                    fout.write(f"{genome_id}\n")
        else:
            _logger.info(
                "All genomes for this version of RELEASE_NOTES seem "
                "to have been properly processed"
            )
    except Exception as e:
        if notify:
            recipients = [str(i).strip() for i in notify.split(",")]
            send_exit_mail(recipients, str(e))

        _logger.critical("Mirror job failed with \n{}".format(e))
        raise

    if notify:
        recipients = [str(i).strip() for i in notify.split(",")]
        send_exit_mail(recipients)


@click.command()
@click.argument(
    "genomes-dir",
    required=True,
    type=pathlib.Path,
)
@click.argument(
    "output-index",
    required=True,
    type=pathlib.Path,
)
@click.option(
    "--loglevel",
    default="INFO",
    help="Define loglevel",
    show_default=True,
    required=False,
)
@click.option(
    "--logfile",
    help="Write logging information in this file",
    show_default=True,
    required=False,
)
@click.option(
    "-j",
    "--jobs",
    default=1,
    show_default=True,
    help="Number of parallel reads to execute. Speeds things up when "
    "iterating over all the data dirs",
)
def index(genomes_dir, output_index, loglevel, logfile, jobs):
    """Create an index of contents for all directories

    This can be useful for generating valid paths before gathering inputs.
    The output_index is a tab-separated file with each column representing
    the files that are expected to be found for a genome that has full
    information available from both PATRIC and RefSeq. If any of the files
    is missing the value is 0.

    genomes_dir: The location where all data is stored

    output_index:  The files to write all the info in

    """
    setup_logging(loglevel, logfile)
    all_records = all_data(genomes_dir, contents, jobs)
    write_index(all_records, output_index)
    return


cli.add_command(mirror)
cli.add_command(index)


if __name__ == "__main__":
    cli()
