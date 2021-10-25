import os
import logging
import pandas as pd
import hashlib
import pathlib
from io import BytesIO


# CONSTANTS

PATRIC_FTP = "ftp.patricbrc.org"

RELEASE_NOTES_FILES = {
    "genome_summary",
    "genome_metadata",
    "genome_lineage",
    "PATRIC_genomes_AMR.txt",
}

_logger = logging.getLogger(__name__)

# HELPERS

def get_remote_dir_timestamps(ftp_handle, remote_dir_name, skip_dirs=True):
    """
    Parse the remote timestamps.

    Create a dict that holds file names as keys and timestamps as values
    Timestamp is formatted as 'YYYYMMDDHHMMSS'

    Positional arguments:
      - ftp_handle: ftplib.FTP: A working ftp connection
      - remote_dir_name: string: A relative path to the ftp root.
        e.g. 'RELEASE_NOTES/genome_summary'

    Keyword arguments:
      - skip_dirs: bool: If this is True directories, are skipped.

    Return:
      - timestamps_dict: dict: {fname: timestamp, ... }. `fname` as read from
      the ftp. `timestamp` is formatted as 'YYYYMMDDHHMMSS'
        e.g. { '1234.5.fna' : '20200112235902', ... }
    """
    timestamps_dict = {}
    for entry in ftp_handle.mlsd(remote_dir_name, facts=["modify", "type"]):
        if (entry[1]["type"] != "file") and (skip_dirs is True):
            pass
        else:
            timestamps_dict[entry[0]] = {"ftp_mdtm": entry[1]["modify"]}

    return timestamps_dict


def get_remote_file_md5(ftp_handle, remote_file_name):
    """Store the md5sum of a file and its contents in memory"""
    _logger.debug(
        "Calculating md5sum for file: {} from remote".format(remote_file_name)
    )
    content = BytesIO()
    ftp_handle.retrbinary(f"RETR {remote_file_name}", content.write)
    content_string = content.getvalue()
    md5 = hashlib.md5(content_string).hexdigest()
    return md5, content_string


def get_local_info(a_dir):
    """Get records from the ftp_info.tsv if it exists"""
    dir_info = None
    info_tsv = a_dir / pathlib.Path("ftp_info.tsv")
    if info_tsv.exists():
        dir_info = pd.read_csv(
            info_tsv, sep="\t", dtype={"ftp_mdtm": "string"}, index_col="fname"
        )
        dir_info = dir_info.to_dict(orient="index")
    else:
        _logger.debug(
            "No mod time information found for {}".format(a_dir.resolve())
        )
    return dir_info


def get_missing_files(a_dir, files_list):
    """Given a list of filenames, scan the dir for their existence"""
    missing_files = []
    for f in files_list:
        fp = a_dir / pathlib.Path(f)
        if not fp.exists():
            missing_files.append(f)

    if len(missing_files) != 0:
        _logger.debug(
            "Directory: {} is missing file(s): {}".format(
                a_dir.resolve(), ",".join(missing_files)
            )
        )
    else:
        logging.debug(
            "Directory: {} contains all files".format(a_dir.resolve())
        )

    return set(missing_files)


def md5(fp):
    """Calculate md5sum for a file"""
    content = fp.read_text().encode()
    return hashlib.md5(content).hexdigest()


def get_dir_md5(a_dir, files_list):
    md5_data = {}
    for f in a_dir.iterdir():
        if f.name in files_list:
            md5_data[f.name] = {"md5": md5(f)}
    return md5_data


def dir_is_empty(dirpath):
    with os.scandir(dirpath) as it:
        return not any(it)


def download_single_file(ftp_handle, remote_fn, local_fp):

    """
    Download one file from the ftp.

    Given a valid ftplib.FTP connection, download the remote_fn
    and store it in the local_fp.

    Positional arguments:
      - ftp_handle: ftplib.FTP:
      - remote_fn: str: Absolute paths or relative to the root dir.
          e.g. "'/genomes/1234.5' or 'genomes/1234.5'
            but not '1234.5'
      -  local_fp: pathlib.Path: Path where the remote is stored

    Return:
      - None

    """
    _logger.debug("Downloading {} in {}".format(remote_fn, local_fp))
    with open(local_fp, "wb") as fout:
        ftp_handle.retrbinary("RETR {}".format(remote_fn), fout.write)


def load_cached_genomes(processed_txt):
    with open(processed_txt, "r") as fin:
        processed_genomes = [line.strip() for line in fin]
    return processed_genomes


def genomes_from_summary(genome_summary):
    genome_ids = pd.read_csv(
        genome_summary,
        sep="\t",
        usecols=["genome_id"],
        dtype={"genome_id": "string"},
    )
    genome_ids = genome_ids["genome_id"].unique()

    _logger.info(
        "Loaded {} genome ids from {}".format(len(genome_ids), genome_summary)
    )

    return genome_ids


def filter_files_on_mdtm(remote_tstamps, local_tstamps=None):
    """
    Compare two timestamps dictionaries for files for files changed

    Filenames are basenames

    Positional arguments:
      - remote_tstamps: dict: A dictionary
          {filename: {'ftp_mdtm' : timestamp, ... }, ...}
      - local_tstamps: dict or None: A dictionary
          {filename: {'ftp_mdtm': timestamp} ... } , ...}
      If it is `None`, all files from `remote_tstamps` are returned

    Return:
      - targets_list: list: A list of filenames. Can be empty if nothing has changed.
    """
    targets_list = []
    if local_tstamps is not None:
        for f in local_tstamps:
            if local_tstamps[f]["ftp_mdtm"] != remote_tstamps[f]["ftp_mdtm"]:
                targets_list.append(f)
    else:
        targets_list = list(remote_tstamps.keys())

    return targets_list


def download_genome_targets(ftp_handle, local_dir, targets_list):
    """Download the list of target filenames to the local dir"""
    genome_id = local_dir.name
    for f in targets_list:
        remote_fname = f"genomes/{genome_id}/{f}"
        local_fpath =  local_dir / pathlib.Path(f)
        download_single_file(ftp_handle, remote_fname, local_fpath)

