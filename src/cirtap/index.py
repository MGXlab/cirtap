#!/usr/bin/env python

import logging
import pathlib
import pandas as pd
import multiprocessing as mp

_logger = logging.getLogger(__name__)

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

contents = {
    "genome": {"fna": "patric_genome"},
    "PATRIC": {
        "cds.tab": "patric_cds",
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

                genome_data[col_name] = int(1)

    for d in contents.keys():
        for v in contents[d].values():
            if v not in genome_data:
                genome_data[v] = 0

    return genome_data


def all_data(genomes_dir, contents_dic, jobs=1):
    all_records = []
    _logger.info("Loading directories")
    dir_jobs = [(g, contents_dic) for g in genomes_dir.iterdir()]
    _logger.info("Reading information")
    with mp.Pool(processes=jobs) as pool:
        all_records = pool.starmap(genome_data, dir_jobs)

    return all_records


def write_index(records_list, output_index):

    some_record = records_list[0]
    dtypes = {k: "int64" for k in some_record.keys()}
    dtypes["genome_id"] = "string"
    index_df = pd.DataFrame.from_records(records_list)
    index_df = index_df.astype(dtypes)

    index_df.to_csv(output_index, sep="\t", index=False)
