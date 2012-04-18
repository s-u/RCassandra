\name{RC.get}
\alias{RC.use}
\alias{RC.get}
\alias{RC.get.range}
\alias{RC.mget.range}
\alias{RC.get.range.slices}
\title{
  Functions for querying Cassandra database
}
\description{
  \code{RC.use} selects the keyspace (aka database) to use for all
  subsequent operations. All functions described below require keyspace
  to be set using this function.
  
  \code{RC.get} queries one key and one column

  \code{RC.get.range} queries one key and multiple columns

  \code{RC.mget.range} queries multiple keys and multiple columns

  \code{RC.get.range.slices} queries a range of keys (or tokens) and a
  range of columns
}
\usage{
RC.use(conn, key.space)
RC.get(conn, c.family, key, c.name)
RC.get.range(conn, c.family, key, first = "", last = "",
             reverse = FALSE, limit = 1e+07)
RC.mget.range(conn, c.family, keys, first = "", last = "",
             reverse = FALSE, limit = 1e+07)
RC.get.range.slices(conn, c.family, k.start = "", k.end = "",
                    first = "", last = "", reverse = FALSE,
                    limit = 1e+07, k.limit = 1e+07,
                    tokens = FALSE)
}
\arguments{
  \item{conn}{connection handle as returned by \code{\link{RC.connect}}}
  \item{key.space}{name of the keyspace to use}
  \item{c.family}{column family (aka table) name}
  \item{key}{row key}
  \item{c.name}{column name}
  \item{first}{starting column name}
  \item{last}{ending column name}
  \item{reverse}{if \code{TRUE} the resutl is returned in reverse order}
  \item{limit}{return at most as many columns per key}
  \item{keys}{row keys (character vector)}
  \item{k.start}{start key (or token)}
  \item{k.end}{end key (or token)}
  \item{k.limit}{return at most as many keys (rows)}
  \item{tokens}{if \code{TRUE} then keys are interpreted as tokens
    (i.e. values after hashing)}
}
\value{
  \code{RC.use} returns \code{conn}
  
  \code{RC.get} returns nothing useful :P

  \code{RC.get.range} returns a data frame with columns \code{key}
  (column name), \code{value} (value in that column) and \code{ts}
  (timestamp).

  \code{RC.mget.range} and  \code{RC.get.range.slices} return a named
  list of data frames as described in \code{RC.get.range} with names
  being the row keys.
}
\details{The nomenclature can be a bit confusing and it comes from the
  literature and the Cassandra API. Put in simple terms, \emph{keyspace}
  is comparable to a database, and \emph{column family} is somewhat
  comparable to a table. However, a table may have different number of
  columns for each row, so it can be used to create a flexible
  two-dimensional query structure. A row is defined by a (row)
  \emph{key}. A query is performed by first finding out which row(s)
  will be fetched according to the key (\code{RC.get},
  \code{RC.get.range}), keys (\code{RC.mget.range}) or key range
  (\code{RC.get.rnage.slices}), then selecting the columns of
  interest. Empty string (\code{""}) can be used to denote an
  unspecified range (so the default is to fetch all columns).

  Cassandra collects results in memory so key (\code{k.limit}) and
  column (\code{limit}) limits are mandatory. Future versions of
  RCassandra may abstract this limitation out (by using a limit and
  repeating queries with new start key/column based on the last result
  row), but not at this point.

  Note that in Cassandra keys are typically hashed, so key range may be
  counter-intuitive as it is based on the hash and not on the actual
  value. Columns are always sorted by their name (=key).

  The result of queries may be also counter-intuitive, especially when
  querying fixed column tables as it is not returned in the form that
  would be expected from a relational database. But you have to keep in
  mind that Cassandra is essentailly key/key/value storage (row key,
  column key, value) with partitioning on row keys and sorting of column
  keys, so designing the correct schema for a task needs some
  thought. Dynamic columns are what makes it so powerful.
}
%\references{
%}
\author{
  Simon Urbanek
}
%\note{
%}
\seealso{
  \code{\link{RC.connect}}
}
%\examples{
%}
\keyword{interface}
