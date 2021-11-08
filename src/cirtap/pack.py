import tarfile
import pathlib
import logging

_logger = logging.getLogger(__name__)


def genome_ids_from_text(txt_file):
    """Parse single column text file to a set"""
    genome_ids = []
    with open(txt_file, "r") as fin:
        for line in fin:
            genome_id = line.strip()
            genome_ids.append(genome_id)
    genome_ids_set = set(genome_ids)
    _logger.info(
        "Read {} unique ids from {} lines".format(
            len(genome_ids_set), len(genome_ids)
        )
    )

    return genome_ids_set


def pack_genome_data(genomes_dir, ids_text, tar_out):
    """Create a gzipped tarfile from the ids in the text file"""

    genome_ids = genome_ids_from_text(ids_text)

    with tarfile.open(tar_out, "w:gz") as fout:
        for genome_id in genome_ids:
            full_path = genomes_dir / pathlib.Path(genome_id)
            _logger.debug(full_path)
            if full_path.exists():
                for f in full_path.iterdir():
                    leaf = f.name
                    tar_name = f"genomes/{genome_id}/{leaf}"
                    _logger.debug("Compressing {}".format(f.resolve()))
                    fout.add(f, arcname=tar_name)
            else:
                _logger.warning(f"No data found for genome id {genome_id}")

    return
