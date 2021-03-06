
# Stacks + R

This is a template project that shows how to interact with the corpus data generated by the [Stacks](https://github.com/davidmcclure/stacks) application on the Sherlock cluster.

The code does this:

- Queries the metadata database and loads a subset of text identifiers from ECCO and Gail Amfic.
- Combines these identifiers into a single virtual corpus.
- Splits the combined text set into a set of partitions, one for each rank in the MPI universe.
- Scatters the path segments to the workers, each of work extracts word counts from the texts in the segment.
- These sub-results are then returned to the master rank, and merged into a single, complete set of word counts.

To deploy and run the job:

- SSH onto Sherlock with `ssh <username>@sherlock.stanford.edu`.
- Change into `/share/PI/malgeehe/code`.
- Clone this repository with `git clone https://github.com/davidmcclure/stacks-r.git`.
- Change down into the project directory, and queue the job with `sbatch count_words.run`.
- Check the status of the job with `squeue -u <username>`. When `TIME` is something other than `0:00`, the job is running. If no rows are listed, then the job is finished.
- During the job, stdout will get written to a `count-words.out` file in the current directory (or, whatever directory you run `sbatch` from), and stderr gets flushed to `count-words.err`. (This is configurable in the `count_words.run` file.)
- When the job finishes, it will dump the output to `~/word-counts.csv`.
