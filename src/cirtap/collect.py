from concurrent.futures import ProcessPoolExecutor as tPool
import multiprocessing as mp
from Bio import SeqIO
import gzip
import pandas as pd
import pathlib
import logging
import shutil
import sys

_logger = logging.getLogger(__name__)

supported_suffixes = {
    "patric_proteins": "PATRIC.faa",
    "patric_rnas": "PATRIC.frn",
}

SSU_DESCRIPTIONS = ["16S ribosomal RNA", "SSU rRNA"]


def is_gz(fp):
    return fp.name.endswith("gz")


def optionally_compressed_handle(fp, mode):
    if mode == "r" or mode == "rb":
        mode = "rt"
    if mode == "a" or mode == "ab":
        mode = "at"
    if mode == "w" or mode == "wb":
        mode = "wt"

    if is_gz(fp):
        return gzip.open(fp, mode=mode)
    else:
        return open(fp, mode)


def load_index(index_path):
    df = pd.read_csv(index_path, sep="\t", dtype={"genome_id": "string"})
    return df


def select_genome_ids(index_path, on_col):
    """Get a list of genome ids that have a file"""
    df = pd.read_csv(index_path, sep="\t", dtype={"genome_id": "string"})
    genome_ids = df.loc[df[on_col] == 1]["genome_id"].tolist()
    assert len(genome_ids) != 0, _logger.critical("No genome ids found")
    return genome_ids


def generate_file_list(genomes_dir, genome_ids, target, suffixes_dict):
    """Get a list of files that are going to be parsed"""
    if target in suffixes_dict:
        suffix = suffixes_dict[target]
        files_list = [
            genomes_dir / pathlib.Path(f"{genome_id}/{genome_id}.{suffix}")
            for genome_id in genome_ids
        ]
    else:
        _logger.critical("Unsupported filetype for set {}".format(target))
        sys.exit(1)

    return files_list


def is_SSU(record):
    """Return true if the record is a 16S sequence based on description"""
    return any(desc in record.description for desc in SSU_DESCRIPTIONS)


def has_valid_seq(record):
    """Basic check for empties and `1`s"""
    return len(record.seq) > 1


def fasta_reader(fa, q, *filters):
    # TO DO
    # Check packing/unpacking
    # Should this be just a list, even if it's empty?
    """
    Reader worker for the fa file in the specified q(ueue)

    Applies a filter on the sequence length > 1. This
    is there to parse out
    (a) empty sequence strings
    (b) sequences that are only represented as 1

    Parsing is done with Bio.SeqIO.parse()

    The optional `filters` can be any number of callables that can
    be applied to a SeqRecord object. Each should return a single
    boolean True or False, if the record is to be kept. True to
    keep, False to discard. If a record must be kept, all filters
    should return True. If one fails, the record is skipped.


    Arguments:
      fa: Path obj: The Path representation of the fasta to read
      q: Queue obj: A multiprocessing.Manager.Queue() instance
      *filters: callables: Filtering rules that apply a test to
      the record object. They should return a single True or
      False value.


    Return:
        seq_dict: dict: Dictionary that holds seq descriptions as seq ids
        and sequences.
        A 'skipped' key is there to also gather sequence ids that were
        skipped due to the filtering. Of the form
        {
            seq.description : sequence,
                ...,
            'skipped': [seq.description_1, ...]
                ...
        }
    """
    seq_dict = {"skipped": []}
    for record in SeqIO.parse(fa, "fasta"):
        keep = has_valid_seq(record)
        if filters and keep is True:
            keep = all(f(record) for f in filters[0])
        if keep is True:
            seq_dict[record.description] = record.seq
        else:
            seq_dict["skipped"].append(record.id)
            pass

    q.put(seq_dict)

    return seq_dict


def writer(q, out_fp):
    """
    Writer worker that handles writing to files fed from the reader
    in the q(ueue)

    out_fp is gzipped to save space.

    Arguments:
      q: mp.Queue() obj: A queue
      out_fp: Path obj: Path to output.fa.gz

    Return:
      -
    """
    with gzip.open(out_fp, "wt") as fout:
        while 1:
            m = q.get()
            if m == "kill":
                break
            for k, v in m.items():
                if k == "skipped":
                    # Used to handle that
                    pass
                else:
                    fout.write(">{}\n{}\n".format(k, v))
                    fout.flush()


def chunkify(files_list, chunksize=1000):
    """
    Create a list of chunks.

    Each chunk is a list itself, with size `chunksize`.

    Arguments:
      files_list: list: A list of Path objects
      chunksize: int: Size of each chunk

    Return:
      chunks: list: A list of lists. Each nested list has size
        `chunksize`.
    """
    chunks = []
    for i in range(0, len(files_list), chunksize):
        chunk = files_list[i : i + chunksize]
        chunks.append(chunk)
    return chunks


def create_jobs_list(chunks, outdir, *filters):
    # TO DO
    # Figure out the packing/unpacking
    """
    Create a list of dictionaries that hold information for the given
    chunks

    Arguments:
      chunks: list: A list of lists. Each nested list contains the
      filepaths to be processed
      outdir: Path object: The directory where results will be written
      filters: Callables

    Return:
    jobs_list: list: A list of dictionaries that holds information for
    the execution of each chunk. Of the form
      [
        {'chunk_id'  : int,         (0,1,2,...)
         'out_fp'    : Path object, (outdir/chunk_<chunk_id>.fa.gz)
         'fastas'    : list of Path objects,
                       ([PosixPath('path/to/PATRIC.faa'),...])
         'filters'   : list of functions
        }
      ]

    """
    jobs_list = []
    for i, chunk in enumerate(chunks):
        chunk_id = f"chunk_{i}"
        chunk_out = f"{chunk_id}.fa.gz"
        out_fp = outdir / pathlib.Path(chunk_out)
        # chunk_skipped = f"{chunk_id}.skipped.txt"
        chunk_fastas = chunk
        chunk_dict = {
            "chunk_id": chunk_id,
            "fastas": chunk_fastas,
            "out_fp": out_fp,
            # Should there be an if filters or if len(filters) != 0 ?
            "filters": [f for f in filters],
        }

        jobs_list.append(chunk_dict)
    return jobs_list


# https://stackoverflow.com/a/13530258
def process_chunk(chunk_dict):
    """
    Multiprocessing of a single chunk.

    This spawns 4 processes: 1 is dedicated to writing and 3 are reading.
    This is to ensure that the file being written is locked.
    """
    manager = mp.Manager()
    q = manager.Queue()

    # TO DO
    # Avoid hardcoding this value
    with mp.Pool(4) as mpool:
        # 1 dedicated process to write
        watcher = mpool.apply_async(writer, (q, chunk_dict["out_fp"]))

        # The rest 3 are reading
        jobs = []
        for fa in chunk_dict["fastas"]:
            job = mpool.apply_async(
                fasta_reader, (fa, q, chunk_dict["filters"])
            )
            jobs.append(job)

        # Collect results
        for job in jobs:
            job.get()

        q.put("kill")
        mpool.close()
        mpool.join()


def collect_sequences(files_list, outdir, *filters, nthreads=2):
    """Start nthreads that each spawns a multiprocessing pool of 4

    Yeah, this is weird. Probably a threads-only based thing would
    be better
    """
    chunks = chunkify(files_list)

    job_dicts = create_jobs_list(chunks, outdir, *filters)

    # Restrict cpu usage
    max_cpus = mp.cpu_count()
    if nthreads * 4 > max_cpus:
        _logger.info("Too many threads provided ( {} )".format(nthreads))
        _logger.info("This is an implementation detail that should be fixed")
        use_threads = nthreads // 4
        _logger.info("Resorting to {}".format(use_threads))
    else:
        use_threads = nthreads
        _logger.info("Using {} threads".format(use_threads))

    # tPool is concurrent.futures.PoolExecutor
    # It allows for children processes
    # to spawn their own pools, even multiprocessing.Pool
    # see https://stackoverflow.com/a/61470465
    with tPool(use_threads) as tpool:
        counter = 0
        for i in range(0, len(job_dicts), nthreads):
            current_jobs = job_dicts[i : i + nthreads]
            a = tpool.map(process_chunk, current_jobs)
            for _ in a:
                counter += 1
            _logger.info(
                "Finished {} / {} chunks".format(counter, len(chunks))
            )
    return


def concatenate_chunk_files(chunks_dir, output_path, cleanup=True):
    """Gather all files in one"""
    _logger.info("Concatenatating all sequence files")
    files_counter = 0
    seq_counter = 0
    with optionally_compressed_handle(output_path, "w") as fout:
        for f in chunks_dir.iterdir():
            files_counter += 1
            with optionally_compressed_handle(f, "r") as fin:
                for record in SeqIO.parse(fin, "fasta"):
                    SeqIO.write(record, fout, "fasta")
                    seq_counter += 1
    if cleanup is True:
        _logger.info("Removing {}".format(chunks_dir.resolve()))
        shutil.rmtree(chunks_dir)

    return files_counter, seq_counter
