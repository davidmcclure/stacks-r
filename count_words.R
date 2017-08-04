

library(Rmpi)
library(parallel)
library(plyr)
library(DBI)
library(data.table)
library(rjson)


CountWords = function(db.file, texts.dir, out.file, chunk.size=10000) {

  # Load word list.
  words = read.table('words.txt', header=F, stringsAsFactors=F)[,1]

  # Build text list.

  conn = dbConnect(RSQLite::SQLite(), 'metadata.db')

  ecco.query = dbSendQuery(conn, "SELECT corpus, text_hash from ecco_text where pub_date > '1790-01-01' and pub_date < '1795-01-01' limit 10")
  ecco.texts = dbFetch(ecco.query)

  amfic.query = dbSendQuery(conn, "SELECT corpus, text_hash from amfic_text where pub_date_start > 1890 and pub_date_start < 1900 limit 10")
  amfic.texts = dbFetch(amfic.query)

  texts = rbind(ecco.texts, amfic.texts)

  text.list = split(texts, seq(nrow(texts)))

  # Create MPI cluster.
  size = mpi.universe.size()
  np = if (size > 1) size - 1 else 1
  cluster = makeCluster(np, type='MPI', outfile='')

  # Copy variables / functions to workers.
  clusterExport(cluster, c('words', 'countTokens'), envir=environment())

  # Import libraries on workers.
  clusterEvalQ(cluster, {
    library(rjson)
  })

  segments = split(text.list, ceiling(seq_along(text.list)/np))

  res = clusterApplyLB(cl=cluster, x=segments, fun=function(segment) {

    tryCatch({

      counts = NULL

      for (row in segment) {

        # Form the tokens JSON path.
        prefix = substr(row$text_hash, 1, 3)
        suffix = substr(row$text_hash, 4, nchar(row$text_hash))
        path.tokens = file.path(texts.dir, row$corpus, prefix, suffix, 'tokens.json.bz2')

        # Build up per-text token counts.
        new.counts = countTokens(path.tokens, words)

        # Merge in the new counts with the accumulator.
        counts = tapply(c(counts, new.counts), names(c(counts, new.counts)), sum)

        print(Sys.time(), path)

      }

      return(counts)

    }, error=function(e) {
      print(e)
      return(NULL)
    })

  })

  # Merge together partial results for segments.
  all.counts = do.call('c', res)
  all.counts = tapply(all.counts, names(all.counts), sum)

  write.csv(all.counts, file=out.file)

}


countTokens = function(path, words) {

  json = fromJSON(file=path)

  tokens = data.table::rbindlist(json)

  counts = table(tokens$token)
  counts = counts[which(names(counts) %in% words)]

  return(counts)

}


if (!interactive()) {
  CountWords(
    db.file='/scratch/PI/malgeehe/data/stacks/metadata.db',
    texts.dir='/scratch/PI/malgeehe/data/stacks/ext/texts',
    out.file='~/word-counts.csv',
  )
}
