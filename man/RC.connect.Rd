\name{RC.connect}
\alias{RC.connect}
\alias{RC.close}
\alias{RC.login}
\title{
  Connect, login, close connection to Cassandra
}
\description{
  \code{RC.connect} connects a to a host running Cassandra. All
  subsequent operations are performed on the handle returned by this
  function.

  \code{RC.close} closes a Cassandra connection.

  \code{RC.login} perform an authentication request.
}
\usage{
RC.connect(host = NULL, port = 9160L)
RC.close(conn)
RC.login(conn, username = "default", password = "")
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{host}{host name or IP address to connect to using TCP/IP}
  \item{port}{port to connect to on the above host}
  \item{conn}{connection handle as returned by \code{RC.connect}}
  \item{username}{username for the authentication dictionary}
  \item{password}{password for the authentication dictionary}
}
\details{
  \code{RC.connect} return an opaque connection handle that has to be
  used for all subsequent calls on the same connection.
  \code{RCassandra} uses low-level system calls to communicate with
  Cassandra, this handle is not an R connection.

  \code{RC.close} closes an existing Cassandra connection.

  \code{RC.login} sends an authenticataion request with the given
  credentials. How this is processed depend on the authentication module
  use in the Cassandra instance this connection connects to.
}
\value{
  \code{RC.connect} returns a Cassandra connection handle.

  \code{RC.close} return \code{NULL}

  \code{RC.login} returns \code{conn}.
}
\author{
  Simon Urbanek
}
%\seealso{
%}
\examples{
\dontrun{
c <- RC.connect("cassandra-host")
RC.login(c, "foo", "bar")
RC.cluster.name(c)
RC.describe.keyspaces(c)
RC.close(c)
}
}
\keyword{interface}
