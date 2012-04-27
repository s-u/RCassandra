\name{RC.cluster.name}
\alias{RC.cluster.name}
\alias{RC.describe.keyspace}
\alias{RC.describe.keyspaces}
\alias{RC.version}
\title{
  Functions retrieving meta-information from a Cassandra connection
}
\description{
  \code{RC.cluster.name} returns the name of the cluster.

  \code{RC.version} returns the protocol version.

  \code{RC.describe.keyspace} returns a keyspace definition.

  \code{RC.describe.keyspaces} returns a list of definitions for all
  keyspaces.
}
\usage{
RC.cluster.name(conn)
RC.version(conn)
RC.describe.keyspaces(conn)
RC.describe.keyspace(conn, keyspace)
}
\arguments{
  \item{conn}{connection handle are returned by
    \code{\link{RC.connect}}}
  \item{keyspace}{string, name of the keyspace to describe}
}
%\details{
%}
\value{
  For \code{RC.cluster.name} and \code{RC.version} a string.

  For \code{RC.describe.keyspace} a structure describing the keyspace -
  see \code{KsDef} structure in Cassandra.

  For \code{RC.describe.keyspaces} a list of the \code{KsDef} strctures.
}
\author{
  Simon Urbanek
}
\seealso{
  \code{\link{RC.connect}}, \code{\link{RC.get}}
}
\keyword{interface}
