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
from cirtap.mailer import send_start_mail

__author__ = "papanikos"
__copyright__ = "papanikos"
__license__ = "MIT"

_logger = logging.getLogger(__name__)


# ---- Python API ----
# The functions defined in this section can be imported by users in their
# Python scripts/interactive interpreter, e.g. via
# `from cirtap.skeleton import fib`,
# when using this Python module as a library.


# ---- CLI ----
# The functions defined in this section are wrappers around the main Python
# API allowing them to be called directly from the terminal as a CLI
# executable/script.


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel: str: minimum loglevel for emitting messages
    """
    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: {}".format(loglevel))
    else:
        logformat = "[%(asctime)s - %(levelname)s:%(name)s] %(message)s"
        logging.basicConfig(
            level=numeric_level,
            stream=sys.stderr,
            format=logformat,
            datefmt="%Y-%m-%d %H:%M:%S",
        )


@click.group(context_settings=dict(help_option_names=["-h", "--help"]))
@click.version_option(__version__)
def cli():
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
    help="Force update the genomes directory regardless of RELEASE_NTOES outcome",
)
@click.option(
    "-r",
    "--resume",
    is_flag=True,
    default=False,
    show_default=True,
    help="Use this to set both --skip-release-check and --skip-processed-genomes on. "
    "Useful for resuming an unsuccessful run",
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
):
    """Mirror all data from ftp.patricbrc.org in the specified DB_DIR"""

    if progress and (loglevel == "debug"):
        _logger.info(
            "Unsetting `progress` option because it conflicts with "
            "`loglevel debug`"
        )
        progress = False

    setup_logging(loglevel)

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
    ten_targets = [
        "100053.5",
        "100.11",
        "100053.4",
        "100.9",
        "1123738.3",
        "1000562.3",
        "100053.8",
        "469009.4",
        "1309411.5",
        "100053.6",
    ]
    genome_jobs = [job for job in genome_jobs if job in ten_targets]

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


cli.add_command(mirror)


if __name__ == "__main__":
    cli()