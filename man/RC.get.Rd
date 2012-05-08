\name{RC.get}
\alias{RC.use}
\alias{RC.get}
\alias{RC.get.range}
\alias{RC.mget.range}
\alias{RC.get.range.slices}
\alias{RC.consistency}
\title{
  Functions for querying Cassandra database
}
\description{
  \code{RC.use} selects the keyspace (aka database) to use for all
  subsequent operations. All functions described below require keyspace
  to be set using this function.
  
  \code{RC.get} queries one key and a fixed list of columns

  \code{RC.get.range} queries one key and multiple columns

  \code{RC.mget.range} queries multiple keys and multiple columns

  \code{RC.get.range.slices} queries a range of keys (or tokens) and a
  range of columns

  \code{RC.consistency} sets the desired consistency level for all query
  operations
}
\usage{
RC.use(conn, keyspace, cache.def = TRUE)
RC.get(conn, c.family, key, c.names,
       comparator = NULL, validator = NULL)
RC.get.range(conn, c.family, key, first = "", last = "",
             reverse = FALSE, limit = 1e+07,
             comparator = NULL, validator = NULL)
RC.mget.range(conn, c.family, keys, first = "", last = "",
             reverse = FALSE, limit = 1e+07,
             comparator = NULL, validator = NULL)
RC.get.range.slices(conn, c.family, k.start = "", k.end = "",
                    first = "", last = "", reverse = FALSE,
                    limit = 1e+07, k.limit = 1e+07,
                    tokens = FALSE, fixed = FALSE,
                    comparator = NULL, validator = NULL)
RC.consistency(conn, level = c("one", "quorum", "local.quorum",
               "each.quorum", "all", "any", "two", "three")) 
}
\arguments{
  \item{conn}{connection handle as returned by \code{\link{RC.connect}}}
  \item{keyspace}{name of the keyspace to use}
  \item{cache.def}{if \code{TRUE} then in addition to setting the
    keyspace a query on the keyspace definition is sent and the result
    cached. This allows automatic detection of comparators and
    validators, see details section for more information.}
  \item{c.family}{column family (aka table) name}
  \item{key}{row key}
  \item{c.name}{column name}
  \item{comparator}{string, type of the column keys (comparator in
    Cassandra speak) or NULL to rely on cached schema definitions}
  \item{validator}{string, type of the values (validator in Cassandra
    speak) or NULL to rely on cached schema definitions}
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
  \item{fixed}{if \code{TRUE} then the result if be a single data frame
    consisting of rows and keys and all columns ever encountered -
    essentially assuming fixed column structure}
  \item{level}{the desired consistency level for query operations on
  this connection. \code{"one"} is the default if not explicitly set.}
}
\value{
  \code{RC.use} and \code{RC.consistency} returns \code{conn}
  
  \code{RC.get} and  \code{RC.get.range} return a data frame with
  columns \code{key} (column name), \code{value} (value in that column)
  and \code{ts} (timestamp).

  \code{RC.mget.range} and  \code{RC.get.range.slices} return a named
  list of data frames as described in \code{RC.get.range} with names
  being the row keys, except if \code{fixed=TRUE} in which case the
  result is a data frame with row names as keys and values as elements
  (timestamps are not retrieved in that case).
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
  (\code{RC.get.range.slices}), then selecting the columns of
  interest. Empty string (\code{""}) can be used to denote an
  unspecified range (so the default is to fetch all columns).

  \code{comparator} and \code{validator} specify the types of column
  keys and values respectively. Every key or value in Cassandra is
  simply a byte string, so it can deal with arbitrary values, but
  sometimes it is convenient to impose some structure on that content
  by declaring what is represented by that byte string. Unfortunately
  Cassandra does not include that information in the results, so the
  user has to define how column names and values are to be
  interpreted. The default interpretation is simply as a UTF-8 encoded
  string, but RCassandra also supports following conversions:
  "UTF8Type", "AsciiType" (stored as character vectors), "BytesType"
  (opaque stream of bytes, stored as raw vector),
  "LongType" (8-bytes integer, stored as real vector in R), "DateType"
  (8-bytes integer, stored as \code{POSIXct} in R), "BooleanType" (one
  byte, logical vector in R), "FloatType" (4-bytes float, real vector
  in R), "DoubleType" (8-bytes float, real vector in R) and "UUIDType"
  (16-bytes, stored as UUID-formatted string). No other conversions
  are supported at this point. If the value is \code{NULL} then
  \code{RCassandra} attempts to guess the proper value by taking into
  account the schema definition obtained by
  \code{RC.use(..., cache.def=TRUE)}, otherwise it falls back to
  "UTF8Type". You can always get the raw form using "BytesType" and
  decode the values in R.

  The \code{comparator} also determines how the values of \code{first}
  and \code{last} will be interpreted. Regardless of the comparator, it
  is always possible to pass either \code{NULL}, \code{""} (both
  denoting 0-length value) or a raw vector. Other supported types must
  match the comparator.

  Most users will be happy with the default settings, but if you want to
  save every nanosecond you can, call
  \code{RC.use(..., cache.def = FALSE)} (which saves one extra
  \code{\link{RC.describe.keyspace}} request to the Cassandra instance)
  and always specify both \code{comparator} and \code{validator} (even
  if it is just "UTF8String").
  
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
  would be expected from a relational database. See
  \code{\link{RC.read.table}} and \code{\link{RC.write.table}} for
  retrieving and storing relational structures in rectangular tables
  (column families with fixed columns). But you have to keep in
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
  \code{\link{RC.connect}}, \code{\link{RC.read.table}}, \code{\link{RC.write.table}}
}
%\examples{
%}
\keyword{interface}
