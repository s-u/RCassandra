\name{RC.read.table}
\alias{RC.read.table}
\alias{RC.write.table}
\title{
  Read and write tables into column families in Cassandra
}
\description{
  \code{RC.read.table} reads the contents of a column family into a data
  frame

  \code{RC.write.table} writes the contents of a data frame into a
  column familly
}
\usage{
RC.read.table(conn, c.family, convert = TRUE, na.strings = "NA",
              as.is = FALSE, dec = ".")
RC.write.table(conn, c.family, df)
}
\arguments{
  \item{conn}{connection handle as obtained form \code{\link{RC.connect}}}
  \item{c.family}{column family name (string)}
  \item{convert}{logical, if \code{TRUE} the resulting data frame is
    processed using \code{\link{type.convert}}, otherwise all columns will
    be character vectors}
  \item{na.strings}{passed to \code{\link{type.convert}}}
  \item{as.is}{passed to \code{\link{type.convert}}}
  \item{dec}{passed to \code{\link{type.convert}}}
  \item{df}{data frame - it must have both row and column names}
}
\details{
  Cassandra is a key/value store with dynamic columns, so tables are not
  the native format. Row names are used as keys and columns are treated
  as fixed. \code{RC.read.table} is really jsut a wrapper for
  \code{\link{RC.get.range.slices}(conn, c.family, fixed=TRUE)}.
  \code{RC.write.table} uses the same facility as
  \code{\link{RC.mutate}} but without actually creating the mutation
  object on the R side.

  Note that all updates in Cassandra are "upserts", i.e.,
  \code{RC.write.table} updates any existing row key/coumn name
  combinations or creates new ones where not present (insert). Additonal
  columns (or even keys) may still exist in the column family and they
  will not be touched.

  \code{RC.read.table} creates a data frame from all columns that are
  ever encountered in at least one key. All other values are filled with
  \code{NA}s.
}
\value{
  \code{RC.read.table} returns the resulting data frame

  \code{RC.write.table} returns \code{conn}
}
\author{
  Simon Urbanek
}
\note{
  IMPORTANT: Cassandra does \emph{NOT} preserve order of keys and
  columns. Internally, keys are ordered by their hash value and columns
  are ordered lexicographically (treated as bytes). However, due to the
  fact that columns are dynamic the order of columns will vary if keys
  have different columns, because columns are added to the data frame in
  the sequence they are encountered as the keys are loaded. You may want
  to use \code{df <- df[order(as.integer(row.names(df))),]} on the
  result of \code{RC.read.table} for tables with automatic row names to
  obtain the original order of rows.

  \code{RC.read.table} is more effcient than
  \code{\link{RC.get.range.slices}} because it can store columns into
  vectors and can pre-allocate the whole structure in advance.
  
  Note that the current implementation of tables (\code{RC.read.table}
  and \code{RC.write.table}) supports only string-based representation
  of columns and values ("UTF8Type",  "AsciiType" or similar).
}
\seealso{
  \code{\link{RC.connect}}, \code{\link{RC.use}}, \code{\link{RC.get}}
}
\keyword{interface}
