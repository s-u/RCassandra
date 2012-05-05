\name{RCassandra-package}
\alias{RCassandra-package}
\alias{RCassandra}
\docType{package}
\title{
  R/Cassandra interface
}
\description{
  This packages provides a direct interface (without the use of Java) to
  the most basic functionality of Apache Cassanda ditributed NoSQL
  database such as login, updates and queries. The focus is on effciency
  and speed.
}
\details{
  \code{\link{RC.connect}} is used to connect to a Cassandra
  instance. The obtained handle is then used for all operations until
  \code{\link{RC.close}} is used to close the connection.

  A set of \code{\link{RC.get}} functions can be used to query the
  database. Specialized high-level interface for fixed-column tables
  (not the most common in Cassandra, thgouh) is also available with
  \code{\link{RC.read.table}}.

  Updates and inserts can be performed either individually using the
  \code{\link{RC.insert}} function or batch-mutations using
  \code{\link{RC.mutate}}.

  Auxiliary functions retrieving meta-information from the database
  are described on the \code{\link{RC.version}} help page.
  
  Currently, communication to Cassanra is performed directly on a
  blocking TCP/IP socket. This implies that transactions currently
  cannot be interrupted on the R side and there is no timeout. This may
  change in future versions. The code does \emph{not} use R connections
  to avoid extra overhead.
  
  \tabular{ll}{
    Package: \tab RCassandra\cr
    License: \tab GPL-2\cr
    URL: \tab http://www.rforge.net/RCassandra\cr
  }

  Index:
  \preformatted{
RC.cluster.name         Functions retrieving meta-information from a
                        Cassandra connection
RC.connect              Connect, login, close connection to Cassandra
RC.get                  Functions for querying Cassandra database
RC.insert               Update function to insert data into Cassandra
RC.read.table           Read and write tables into column families in
                        Cassandra
}
}
\author{
Simon Urbanek <simon.urbanek@r-project.org>
}
\keyword{package}
