\name{RC.insert}
\alias{RC.insert}
\alias{RC.mutate}
\title{
  Update function to insert data into Cassandra
}
\description{
  \code{RC.insert} updates or inserts a new column/value

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
  \item{key}{row key name (string) }
  \item{column}{column name (string) }
  \item{value}{optinally a value to add into the column. If \code{NULL}
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
  }
}
\value{
  \code{conn}
}
\author{
  Simon Urbanek
}
\note{
  Cassandra requires timestamps on all objects that specify
  columns/values for conflict resolution. All functions above generate
  such timestamps from the system time as POSIX time in milliseconds.

  \code{RC.mutate} currently only uses "UTF8Type" validator and
  comparator as there is no way to specify either in the mutation
  object.
}
\seealso{
  \code{\link{RC.connect}}, \code{\link{RC.use}}, \code{\link{RC.get}}
}
\keyword{interface}
