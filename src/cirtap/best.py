import logging
import pathlib
import numpy as np
import pandas as pd

from ete3 import NCBITaxa

# CONSTANTS
OFFICIAL_RANKS = [
    "superkingdom",
    "phylum",
    "class",
    "order",
    "family",
    "genus",
    "species",
]

column_dtypes = {
    "taxid": "string",
    "taxon_id": "string",
    "genome_id": "string",
}

_logger = logging.getLogger(__name__)


def field_to_float(field_string):
    """Convert a str representation of a float to a float"""
    if field_string != "":
        return float(field_string)
    else:
        return np.nan


def parse_genome_summary(genome_summary_fp, thresh, available_genomes):
    """Parse the genome summary file in a data frame

    Only genomes passing the completeness - 5*contamination > thresh
    rule are retained.

    Positional arguments:
      genome_summary_fp: pathlib.Path: Path to the genome_summary file
      thresh: int: Threshold used for filtering out based on the rule
      available_genomes: list: List of strings of genome ids that have
        a genome file

    Return
      df: pd.DataFrame: A data frame with the stats per genomes that
        pass this first filtering
    """
    data = []
    with open(genome_summary_fp, "r") as fin:
        next(fin)
        for line in fin:
            fields = line.split("\t")
            genome_id = fields[0]
            genome_name = fields[1]
            taxid = fields[2]
            genome_status = fields[3].lower()
            coarse_consistency = field_to_float(fields[14])
            fine_consistency = field_to_float(fields[15])
            completeness = field_to_float(fields[16])
            contamination = field_to_float(fields[17])

            if (
                (completeness and contamination)
                and (completeness - (5 * contamination) > thresh)
                and (genome_status in ["complete", "wgs"])
            ):
                genome_stats = {
                    "genome_id": genome_id,
                    "genome_name": genome_name,
                    "taxid": taxid,
                    "genome_status": genome_status,
                    "completeness": completeness,
                    "contamination": contamination,
                    "coarse_consistency": coarse_consistency,
                    "fine_consistency": fine_consistency,
                }

                data.append(genome_stats)
            else:
                pass

    df = pd.DataFrame.from_records(data)

    _logger.info("Passing genomes : {}".format(genome_summary_fp, df.shape[0]))
    # Convert taxid and genome_id types to string
    # Plays better with the NCBI tree
    df[["taxid", "genome_id"]] = df[["taxid", "genome_id"]].astype(str)

    valid_genomes = set(available_genomes)
    df = df.loc[df.genome_id.isin(valid_genomes)]

    if df.shape[0] == 0:
        raise ValueError("Ooops. Something went wrong with filtering")

    return df


### FILTERING RULES ###
def filter_df_on_completeness(in_df):
    """Get a df or string of the genome id with the highest completeness"""
    max_completeness = in_df.completeness.max()
    on_completeness = in_df.loc[
        in_df.completeness == max_completeness,
    ]
    if on_completeness.shape[0] == 1:
        return on_completeness.genome_id.values[0]
    else:
        return on_completeness


def filter_df_on_contamination(in_df):
    """Get a df or string of the genome id with the lowest contamination"""
    min_contamination = in_df.contamination.min()
    on_contamination = in_df.loc[
        in_df.contamination == min_contamination,
    ]
    if on_contamination.shape[0] == 1:
        return on_contamination.genome_id.values[0]
    else:
        return on_contamination


# TO DO
# Make a function for coarse consistency
# Once the genome_summary gets fixed
def filter_df_on_fine_consistency(in_df):
    """Get a df or string of the genome id with the highest fine consistency"""

    max_fine_consistency = in_df.fine_consistency.min()
    on_fine_consistency = in_df.loc[
        in_df.fine_consistency == max_fine_consistency,
    ]
    if on_fine_consistency.shape[0] == 1:
        return on_fine_consistency.genome_id.values[0]
    else:
        return on_fine_consistency


def filter_df_on_status(in_df):
    """Get a df or string of the genome id with status complete or wgs"""
    statuses = in_df.genome_status.unique()

    if "complete" in statuses:
        on_status = in_df.loc[in_df.genome_status == "complete"]
    else:
        on_status = in_df.loc[in_df.genome_status == "wgs"]

    if on_status.shape[0] == 1:
        return on_status.genome_id.values[0]
    else:
        return on_status


def final_selection(status_df, random_state=1234):
    """Get the genome id as string no matter what.

    If we reach this step there must be a string instance returned.
    This is done with sampling one genome_id at random.
    The random_state ensures that we always choose the same genome from the
    same dataframe.

    Positional arguments:
      status_df: pd.DataFrame: A dataframe that has at least genome_id as
      column

    Optional arguments:
      random_state: int: An integer that controls the random state of
        pd.DataFrame.sample()

    Return:
      genome_id : string: A string of the `genome_id`
    """
    status_df = status_df.sort_values(by="genome_id")
    genome_id = status_df.sample(
        1, random_state=random_state
    ).genome_id.values[0]
    return genome_id


def get_best_genome_id(df):
    """Filter the df until a genome id is returned as string"""
    genome_id = np.nan

    while True:
        genome_id = filter_df_on_completeness(df)
        if type(genome_id) == str:
            break

        genome_id = filter_df_on_contamination(genome_id)
        if type(genome_id) == str:
            break

        genome_id = filter_df_on_fine_consistency(genome_id)
        if type(genome_id) == str:
            break

        genome_id = filter_df_on_status(genome_id)
        if type(genome_id) == str:
            break

        genome_id = final_selection(genome_id)
        if type(genome_id) == str:
            break

        if type(genome_id) != str:
            _logger.error(
                "No valid genome_id found for taxid: {}".format(genome_id)
            )
            _logger.error("Last data seen for it: \n{}".format(genome_id))
            break

    return genome_id


def select_best_from_tree(tree, best_per_taxid_df):
    """Select the best genome from a TreeNode instance"""
    genome_id = np.nan

    # Edge case for species-level tree
    if tree.is_leaf():
        taxids = [tree.name]
    else:
        taxids = [d.name for d in tree.iter_descendants()]

    subset_df = best_per_taxid_df.loc[best_per_taxid_df.taxid.isin(taxids)]

    if not subset_df.empty:
        genome_id = get_best_genome_id(subset_df)
    else:
        _logger.debug(
            "No genome available for tree:{} (rank: {}, taxid: {})".format(
                tree.sci_name, tree.rank, tree.name
            )
        )

    return genome_id


def select_best_for_rank(rank_name, ncbi_tree, best_per_taxid_df, lineages_df):
    """Create a dataframe that holds best genomes for a specified rank"""

    rank_trees = ncbi_tree.search_nodes(rank=rank_name)

    genome_ids = []
    for tree in rank_trees:
        genome_id = select_best_from_tree(tree, best_per_taxid_df)
        if genome_id:
            genome_ids.append(genome_id)

    rank_df = best_per_taxid_df.loc[
        best_per_taxid_df.genome_id.isin(genome_ids)
    ]

    rank_lineages = lineages_df.loc[lineages_df.genome_id.isin(genome_ids)]

    rank_complete_df = pd.merge(rank_df, rank_lineages, on="genome_id")

    _logger.info("{} : {}".format(rank_name, rank_complete_df.shape[0]))

    return rank_complete_df


def write_rank_df_to_file(rank_df, output_tsv):
    """Write a df to the specified file"""
    rank_df.to_csv(output_tsv, sep="\t", index=False)

    return


## WRAPPERS
def select_best_per_taxid(main_df):
    """Build a dictionary of taxids with their best genome representative"""

    grouped = main_df.groupby("taxid")
    best_per_taxid = {}

    for taxid in grouped.groups.keys():
        group = grouped.get_group(taxid)
        best_per_taxid[taxid] = get_best_genome_id(group)

    return best_per_taxid


def select_best_for_ranks(
    ncbi_tree, best_per_taxid_df, lineages_df, output_dir
):

    for rank in OFFICIAL_RANKS:
        rank_df = select_best_for_rank(
            rank, ncbi_tree, best_per_taxid_df, lineages_df
        )

        output_tsv = output_dir / pathlib.Path("best_per_{}.tsv".format(rank))
        write_rank_df_to_file(rank_df, output_tsv)
        _logger.info("{} info written in {}".format(rank, output_tsv))

    return


def select_best(
    genome_summary,
    genome_lineage,
    genome_ids,
    ncbi_db,
    thresh,
    output_path,
):

    lineages_df = pd.read_csv(genome_lineage, sep="\t", dtype=column_dtypes)

    # genome_name: is there and gets suffixed with _x, _y
    # taxon_id: is present as taxid in the original df
    # Drop the columns from the lineages_df before merging
    lineages_df = lineages_df.drop(columns=["taxon_id", "genome_name"])

    filtered_data = parse_genome_summary(genome_summary, thresh, genome_ids)

    best_per_taxid = select_best_per_taxid(filtered_data)

    best_df = filtered_data.loc[
        filtered_data.genome_id.isin(best_per_taxid.values())
    ]

    # Create a Series of filepaths
    # local_fp = best_df.apply(lambda x: genome_ids.get(x.genome_id), axis=1)

    # Make a copy to avoid the copy on slice warning from pandas
    final_df = best_df.copy()

    # Append lineages info
    best_per_taxid_df = pd.merge(final_df, lineages_df, on="genome_id")

    # Append the local_fp series to the df
    # final_df["local_fp"] = local_fp

    if not output_path.exists():
        output_path.mkdir()

    # Write the df to the specified output.
    best_per_taxid_tsv = output_path / pathlib.Path("best_per_taxid.tsv")
    best_per_taxid_df.to_csv(best_per_taxid_tsv, sep="\t", index=False)
    _logger.info(
        "Best genomes per taxid written in {}".format(best_per_taxid_tsv)
    )

    _logger.info("Loading NCBI Taxonomy information")
    ncbi = NCBITaxa(dbfile=ncbi_db)

    # Get an array of the taxids covered here
    taxids_used = final_df.taxid.values

    # Create a taxonomy tree for the valid taxids
    ncbi_tree = ncbi.get_topology(taxids_used, intermediate_nodes=True)

    select_best_for_ranks(ncbi_tree, final_df, lineages_df, output_path)
    _logger.info("Done. Results are found in {}".format(output_path))
