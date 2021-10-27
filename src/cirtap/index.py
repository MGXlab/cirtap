#!/usr/bin/env python

import logging
import pathlib
import pandas as pd
import multiprocessing as mp

_logger = logging.getLogger(__name__)

## A "complete" dir listing
# 511145.12.fna
# 511145.12.PATRIC.faa
# 511145.12.PATRIC.features.tab
# 511145.12.PATRIC.ffn
# 511145.12.PATRIC.frn
# 511145.12.PATRIC.gff
# 511145.12.PATRIC.pathway.tab
# 511145.12.PATRIC.spgene.tab
# 511145.12.PATRIC.subsystem.tab
# 511145.12.RefSeq.cds.tab
# 511145.12.RefSeq.faa
# 511145.12.RefSeq.ffn
# 511145.12.RefSeq.frn
# 511145.12.RefSeq.gbf
# 511145.12.RefSeq.gff
# 511145.12.RefSeq.pathway.tab
# 511145.12.RefSeq.rna.tab

# TO DO
# This is a bit awkward to use
contents = {
    "genome": {"fna": "patric_genome"},
    "PATRIC": {
        "faa": "patric_proteins",
        "features.tab": "patric_features",
        "ffn": "patric_functions",
        "frn": "patric_rnas",
        "gff": "patric_annotations",
        "pathway.tab": "patric_patwhays",
        "spgene.tab": "patric_spgenes",
        "subsystem.tab": "patric_subsystems",
    },
    "RefSeq": {
        "cds.tab": "refseq_cds",
        "faa": "refseq_proteins",
        "features.tab": "refseq_features",
        "ffn": "refseq_functions",
        "frn": "refseq_rnas",
        "gbf": "refseq_genbank",
        "gff": "refseq_annotations",
        "pathway.tab": "refseq_patwhays",
        "rna.tab": "refseq_rna_tab",
    },
}


# def construct_filename(genome_id, db, f):
#    db_suffix = contents["db"][db]["name"]
#    file_suffix = contents["db"][db]["files"][f]
#    return f"{genome_id}.{db_suffix}.{file_suffix}"


def genome_data(genome_dir, contents):
    """Create a dict of presence (1) | absence (0) of files in a dir"""
    genome_id = genome_dir.name
    genome_data = {"genome_id": genome_id}

    # Required for cases like 7227.4 where an ht2.tar was there
    valid_suffixes = []
    for d in contents.keys():
        for dk in contents[d].keys():
            if dk not in valid_suffixes:
                valid_suffixes.append(dk)

    _logger.debug("Processing dir: {}".format(genome_dir))
    for f in genome_dir.iterdir():
        fname = f.name
        fname_fields = fname.split(".")

        if fname == "ftp_info.tsv":
            continue

        if fname_fields[-1] == "fna":
            col_name = contents["genome"]["fna"]
            genome_data["patric_genome"] = 1
        else:
            db = fname_fields[2]
            suffix = ".".join(fname_fields[3:])
            if suffix in valid_suffixes:
                if db == "RefSeq":
                    col_name = contents["RefSeq"][suffix]
                else:
                    col_name = contents["PATRIC"][suffix]

                genome_data[col_name] = 1

    # TO DO
    # Change the contents dic to something more sane
    ## For the files that were not found
    for d in contents.keys():
        for v in contents[d].values():
            if v not in genome_data:
                genome_data[v] = 0

    return genome_data


def all_data(genomes_dir, contents_dic, jobs=1):
    """Parallelizable read of all directories in a list of dicts"""
    all_records = []
    _logger.info("Loading directories")
    dir_jobs = [(g, contents_dic) for g in genomes_dir.iterdir()]
    _logger.info("Reading information")
    with mp.Pool(processes=jobs) as pool:
        all_records = pool.starmap(genome_data, dir_jobs)

    return all_records


def write_index(records_list, output_index):
    """Create and write a pandas df to the specified file"""
    # Grab the first record dict
    some_record = records_list[0]
    # Map all values to integers
    dtypes = {k: "int64" for k in some_record.keys()}
    # Except for genome id that needs to be a string
    dtypes["genome_id"] = "string"
    index_df = pd.DataFrame.from_records(records_list)
    index_df = index_df.astype(dtypes)

    # Show missing genomes first
    index_df = index_df.sort_values(by="patric_genome")

    # Reorder the columns so that genome id and genome show first
    cols_reordered = ["genome_id", "patric_genome"]
    for col in index_df.columns.tolist():
        if col not in cols_reordered:
            cols_reordered.append(col)
    index_df = index_df[cols_reordered]

    # Write it
    index_df.to_csv(output_index, sep="\t", index=False)
