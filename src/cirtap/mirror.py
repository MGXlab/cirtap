from functools import partial
import ftplib
import logging
import multiprocessing as mp
import pandas as pd
import pathlib
import shutil
import time
from tqdm import tqdm

from .common import PATRIC_FTP, RELEASE_NOTES_FILES
from .common import get_missing_files, get_dir_md5, get_remote_dir_timestamps
from .common import load_cached_genomes, genomes_from_summary
from .common import get_local_info, get_remote_file_md5
from .common import filter_files_on_mdtm, download_genome_targets

_logger = logging.getLogger(__name__)


def set_remote_version_on_summary():
    """Use the year and month in YYYYMM format as a version"""
    with ftplib.FTP(PATRIC_FTP) as ftp:
        ftp.login()
        release_notes_times = get_remote_dir_timestamps(ftp, "RELEASE_NOTES")
    remote_version = release_notes_times["genome_summary"]["ftp_mdtm"][:6]

    _logger.debug("Remote version is set to {}".format(remote_version))

    return remote_version


def set_local_version(release_dir):
    """Try to infer version in YYYYMM format from existing data"""
    local_version = None
    local_timestamps = get_local_info(release_dir)
    if local_timestamps:
        try:
            local_version = local_timestamps["genome_summary"]["ftp_mdtm"][:6]
        except KeyError:
            pass

    local_version_file = release_dir / pathlib.Path("VERSION")
    if local_version_file.exists():
        local_version = local_version_file.read_text()[:6]

    _logger.debug("Local version is set to {}".format(local_version))
    return local_version


def archive_notes(release_dir, version_suffix=None):
    """Create an archive of all RELEASE_NOTES contents in the parent dir"""

    db_dir = release_dir.parent.resolve()
    if version_suffix:
        basename = "{}.RELEASE_NOTES.bkp".format(version_suffix)
    else:
        basename = "REELASE_NOTES.bkp"
    archive_name = db_dir / pathlib.Path(basename)
    root_dir = db_dir.resolve()
    base_dir = "RELEASE_NOTES"

    shutil.make_archive(
        archive_name, "gztar", root_dir=root_dir, base_dir=base_dir
    )
    return


def check_release_dir(release_dir, archive=True):
    """Check for updates and download if necessary"""

    remote_version = set_remote_version_on_summary()
    local_version = set_local_version(release_dir)

    if remote_version == local_version:
        _logger.info("Versions based on genome_summary are the same")
    if not local_version:
        local_version = "unknown_version"

    missing_files = get_missing_files(release_dir, RELEASE_NOTES_FILES)

    present_files = RELEASE_NOTES_FILES.difference(missing_files)

    if (len(present_files) != 0) and (archive is True):
        _logger.info("Archiving existing RELEASE_NOTES")
        archive_notes(release_dir, version_suffix=local_version)

    # Set the new version
    version_txt = release_dir / pathlib.Path("VERSION")
    version_txt.write_text(remote_version)

    local_md5s = get_dir_md5(release_dir, RELEASE_NOTES_FILES)

    check_genomes = False
    with ftplib.FTP(PATRIC_FTP) as ftp:
        ftp.login()

        for f in RELEASE_NOTES_FILES:
            remote_name = f"RELEASE_NOTES/{f}"
            remote_md5, contents = get_remote_file_md5(ftp, remote_name)
            local_fp = release_dir / pathlib.Path(f)
            if f in present_files:
                if remote_md5 != local_md5s[f]["md5"]:
                    _logger.info("Updating file: {}".format(f))
                    local_fp.write_bytes(contents)
                    check_genomes = True
                else:
                    _logger.info("File: {} is up to date".format(f))

            elif f in missing_files:
                _logger.info("Fetching file: {}".format(f))
                local_fp.write_bytes(contents)
                check_genomes = True

            else:
                _logger.critical("WHAT HAPPENED with {}?".format(f))
                raise

    return check_genomes


def check_cache_dir(cache_dir):
    processed_genomes = []
    if not cache_dir.exists():
        _logger.debug("No cache found. Creating it at {}".format(cache_dir))
        cache_dir.mkdir()
        processed_txt = cache_dir / pathlib.Path("processed_genomes.txt")
    else:
        try:
            processed_txt = cache_dir / pathlib.Path("processed_genomes.txt")
            processed_genomes = load_cached_genomes(processed_txt)
        except FileNotFoundError:
            _logger.debug(
                "No processed_genomes.txt found in {}".format(
                    cache_dir.resolve()
                )
            )
    return set(processed_genomes)


# @tenacity.retry(
#    retry=tenacity.retry.retry_if_exception_type(ftplib.all_errors),
#    before=tenacity.before.before_log(_logger, log_level=logging.DEBUG),
#    wait=tenacity.wait.wait_exponential(multiplier=1, min=10, max=60),
#    stop=tenacity.stop.stop_after_attempt(3),
# )
def sync_single_dir(genomes_dir, genome_id, retries=3, write_info=True):
    """Download/update all info for the genome id"""

    remote_dirname = f"genomes/{genome_id}"

    local_dirpath = genomes_dir / pathlib.Path(genome_id)
    if not local_dirpath.exists():
        local_dirpath.mkdir()

    local_info = get_local_info(local_dirpath)

    # This loop should raise after attempt 3 and break the thing
    # no matter what
    # So no genome id will be there
    for attempt in range(1, retries + 1):
        try:
            with ftplib.FTP(PATRIC_FTP) as ftp:
                ftp.login()
                remote_info = get_remote_dir_timestamps(ftp, remote_dirname)
                targets = filter_files_on_mdtm(remote_info, local_info)
                missing_files = get_missing_files(local_dirpath, list(remote_info.keys()))

                if len(missing_files) != 0:
                       targets.extend(missing_files)

                if len(targets) != 0:
                    download_genome_targets(ftp, local_dirpath, targets)
                else:
                    _logger.debug("{} is up to date".format(genome_id))

            if write_info is True:
                ftp_info_tsv = local_dirpath / pathlib.Path("ftp_info.tsv")
                modtimes_df = pd.DataFrame.from_dict(
                    remote_info, orient="index"
                )
                modtimes_df.index.name = "fname"
                modtimes_df.to_csv(ftp_info_tsv, sep="\t")

            break

        # Catch ftplib errors
        except ftplib.all_errors as ftp_err:
            if attempt == retries + 1:
                _logger.debug("Failed syncing {}".format(genome_id))
                _logger.debug(
                    "Removing directory that might contain corrupted files "
                    "at {}".format(local_dirpath)
                )
                shutil.rmtree(local_dirpath.resolve())
                raise
            else:
                _logger.debug("Failed syncing  {}".format(genome_id))
                _logger.debug("Error was : {}".format(ftp_err))
                attempt += 1
                _logger.debug(
                    "Sleeping for {} s before retrying".format(attempt * 60)
                )
                time.sleep(attempt * 60)

        # Catch CTRL-C if user doesn't want to proceed
        except KeyboardInterrupt:
            _logger.debug("Ctrl+C signal detected")
            _logger.debug(
                "Removing directory that might contain corrupted files "
                "at {}".format(local_dirpath)
            )

            shutil.rmtree(local_dirpath.resolve())
            break
        # For any other exception reraise
        except:
            raise

    return genome_id


def create_genome_jobs(genome_summary, genomes_dir, processed_genomes=None):

    all_genomes = genomes_from_summary(genome_summary)
    all_genome_jobs = []

    if processed_genomes:
        _logger.debug(
            "{} genomes will be skipped".format(len(processed_genomes))
        )
        all_genomes = sorted(
            [i for i in all_genomes if i not in processed_genomes]
        )
        _logger.debug("Continuing with {} genomes".format(len(all_genomes)))

    all_genome_jobs = all_genomes
    #    all_genome_jobs = [
    #        (genome_id, pathlib.Path(genomes_dir) / pathlib.Path(genome_id))
    #        for genome_id in all_genomes
    #    ]
    #
    return all_genome_jobs


def mirror_genomes_dir(
    all_genome_jobs, local_genomes_dir, procs=1, progress_bar=True
):

    parallel_sync = partial(
        sync_single_dir, local_genomes_dir, write_info=True, retries=3
    )

    results = []
    with mp.Pool(processes=procs) as pool:
        if progress_bar:
            pbar = tqdm(total=len(all_genome_jobs))
            for res in pool.imap_unordered(parallel_sync, all_genome_jobs):
                pbar.update()
                results.append(res)
        else:
            for res in pool.imap_unordered(parallel_sync, all_genome_jobs):
                results.append(res)
    return results
