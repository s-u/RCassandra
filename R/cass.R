RC.connect <- function(host = NULL, port = 9160L) .Call("RC_connect", host, port, PACKAGE="RCassandra")

RC.close <- function(conn) .Call("RC_close", conn, PACKAGE="RCassandra")

RC.use <- function(conn, key.space) .Call("RC_use", conn, key.space, PACKAGE="RCassandra")

RC.get <- function(conn, c.family, key, c.name, comparator=NULL, validator=NULL) .Call("RC_get", conn, key, c.family, c.name, comparator, validator, PACKAGE="RCassandra")

RC.get.range <- function(conn, c.family, key, first="", last="", reverse=FALSE, limit=1e7, comparator=NULL, validator=NULL) .Call("RC_get_range", conn, key, c.family, first, last, limit, reverse, comparator, validator, PACKAGE="RCassandra")

RC.mget.range <- function(conn, c.family, keys, first="", last="", reverse=FALSE, limit=1e7, comparator=NULL, validator=NULL) .Call("RC_mget_range", conn, keys, c.family, first, last, limit, reverse, comparator, validator, PACKAGE="RCassandra")

RC.get.range.slices <- function(conn, c.family, k.start="", k.end="", first="", last="", reverse=FALSE, limit=1e7, k.limit=1e7, tokens=FALSE, fixed=FALSE, comparator=NULL, validator=NULL) .Call("RC_get_range_slices", conn, k.start, k.end, c.family, first, last, limit, reverse, k.limit, tokens, fixed, comparator, validator, PACKAGE="RCassandra")

RC.insert <- function(conn, c.family, key, column, value=NULL) .Call("RC_insert", conn, key, c.family, column, value, PACKAGE="RCassandra")

RC.mutate <- function(conn, mutation) .Call("RC_mutate", conn, mutation)

RC.cluster.name <- function(conn) .Call("RC_cluster_name", conn, PACKAGE="RCassandra")

RC.version <- function(conn) .Call("RC_version", conn, PACKAGE="RCassandra")

RC.login <- function(conn, username="default", password="") .Call("RC_login", conn, username, password, PACKAGE="RCassandra")

RC.write.table <- function(conn, c.family, df) .Call("RC_write_table", conn, df, row.names(df), names(df))

RC.read.table <- function(conn, c.family, convert = TRUE, na.strings = "NA", as.is = FALSE, dec = ".") {
  df <- RC.get.range.slices(conn, c.family, fixed=TRUE)
  if (convert) for (i in seq.int(df)) df[[i]] <- type.convert(df[[i]], na.strings, as.is, dec)
  df
}

RC.consistency <- function(conn, level = c("one", "quorum", "local.quorum", "each.quorum", "all", "any", "two", "three")) {
  level <- match.arg(level)
  level <- match(level, c("one", "quorum", "local.quorum", "each.quorum", "all", "any", "two", "three"))
  invisible(.Call("RC_set_cl", conn, level))
}
