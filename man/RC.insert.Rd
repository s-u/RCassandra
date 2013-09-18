\name{RC.insert}
\alias{RC.insert}
\alias{RC.mutate}
\title{
  Update functions to insert data into Cassandra
}
\description{
  \code{RC.insert} updates or inserts new column/value pairs

  \code{RC.mutate} batchwise updates or inserts a list of keys, column
  families and columns/values.
}
\usage{
RC.insert(conn, c.family, key, column, value = NULL,
          comparator = NULL, validator = NULL)
RC.mutate(conn, mutation)
}
\arguments{
  \item{conn}{connection handle obtained from \code{\link{RC.connect}}}
  \item{c.family}{name of the column family (string)}
  \item{key}{row key name (string) or a vector of (preferably
    contiguous) keys to use with the column names vector}
  \item{column}{column name - any vector supported by the comparator}
  \item{value}{optinally values to add into the columns - if specified,
    must be the same length as \code{column}. If \code{NULL}
    only the column is created}
  \item{comparator}{comparator (column name type) to be used - see
    \code{\link{RC.get}} for details}
  \item{validator}{validator (value type) to be used - see
  \code{\link{RC.get}} for details}
  \item{mutation}{a structure describing the desired mutation (see
    Cassandra documentation). In its simplest form it is a nested list:
    \code{list(row.key1=list(c.family1=list(col1=val1, ...), ...), ...)}
    so to add column "foo" with value "bar" to column family "table" and
    row "key" the mutation would be
    \code{list(key=list(table=list(foo="bar")))}. The innermost list can
    optionally be a character vector (if unnamed it specifies the column
    names, otherwise names are column names and elements are values).
    Only string keys and column names are supproted.
  }
}
\value{
  \code{conn}
}
\author{
  Simon Urbanek
}
\note{
  \code{RC.insert} supports multi-column insertions where \code{column}
  and \code{value} are vectors. For the scalar case \code{insert}
  message is used, for vector case \code{batch_mutate}. If \code{key} is
  a scalar, all \code{column}/\code{value} pairs are added to that row
  key. Alternatively, \code{key} can be a vector of the same length as
  \code{column} in which case the mutation will consist of
  \code{key}/\code{column}/\code{value} triplets. Note that \code{key}
  should be contiguous as the mutation will only group contiguous
  sequences (see \code{coalesce} from the \code{fastmatch} package for a
  fast way of obtaining contiguous sequences).
  
  \code{RC.insert} honors both the validator and comparator (the latter
  is taken from the cache if not specified).

  \code{RC.mutate} currently only uses "UTF8Type" validator and
  comparator as there is no way to specify either in the mutation
  object.

  Cassandra requires timestamps on all objects that specify
  columns/values for conflict resolution. All functions above generate
  such timestamps from the system time as POSIX time in milliseconds.
}
\seealso{
  \code{\link{RC.connect}}, \code{\link{RC.use}}, \code{\link{RC.get}}
}
\keyword{interface}
