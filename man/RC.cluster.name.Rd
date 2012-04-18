\name{RC.cluster.name}
\alias{RC.cluster.name}
\alias{RC.version}
\title{
  Functions retrieving meta-information from a Cassandra connection
}
\description{
  \code{RC.cluster.name} returns the name of the cluster.

  \code{RC.version} returns the protocol version.
}
\usage{
RC.cluster.name(conn)
RC.version(conn)
}
\arguments{
  \item{conn}{connection handle are returned by \code{\link{RC.connect}}}
}
%\details{
%}
\value{
  A string
}
\author{
  Simon Urbanek
}
\seealso{
  \code{\link{RC.connect}}, \code{\link{RC.get}}
}
\keyword{interface}
