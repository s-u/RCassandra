/* R/Cassandra interface
   
   Thrift is one of those things that sounded like a good idea in theory but
   is useless in practice, so we end up coding the protocol by hand (surprisibly
   Thrift doesn't even document the wire protocol, lovely..). At least this is
   more effcient than using rJava and some existing classes and we care mainly
   about speed here so it should pay off in the end (it took essentially a day
   to write so that's not too much wasted time...).

   (C)Copyright 2012 Simon Urbanek.

   Released under GPL v2, no warranties.

*/

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>

#define USE_RINTERNALS
#include <Rinternals.h>

typedef struct tconn {
    int s, seq;
    unsigned int send_len, send_alloc;
    unsigned int recv_len, recv_alloc, recv_next;
    int recv_frame;
    char *send_buf, *recv_buf, *recv_buf0, next_char;
} tconn_t;

#define tc_ok(X) (((X)->s) != -1)

static tconn_t *tc_connect(const char *host, int port) {
    tconn_t *c = (tconn_t*) malloc(sizeof(tconn_t));
    c->s = -1;
    c->seq = 0;
    c->recv_frame = -1;
    c->send_alloc = 65536;
    c->send_len = 0;
    c->next_char = 0;
    c->send_buf = (char*) malloc(c->send_alloc);
    if (!c->send_buf) { free(c); return 0; }
    c->recv_alloc = 65536;
    c->recv_next = c->recv_len = 0;
    c->recv_buf0 = c->recv_buf = (char*) malloc(c->recv_alloc);
    if (!c->recv_buf) { free(c->send_buf); free(c); return 0; }
    c->s = socket(AF_INET, SOCK_STREAM, 0);
    if (c->s != -1) {
	struct sockaddr_in sin;
	struct hostent *haddr;
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);
	if (host) {
	    if (inet_pton(sin.sin_family, host, &sin.sin_addr) != 1) { /* invalid, try DNS */
		if (!(haddr = gethostbyname(host))) { /* DNS failed, */
		    close(c->s);
		    c->s = -1;
		}
		sin.sin_addr.s_addr = *((uint32_t*) haddr->h_addr); /* pick first address */
	    }
	} else
	    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if (c->s != -1 && connect(c->s, (struct sockaddr*)&sin, sizeof(sin))) {
	    close(c->s);
	    c->s = -1;
	}
    }
    if (c->s == -1) {
	free(c->send_buf);
	free(c->recv_buf);
	free(c);
	return 0;
    }
    return c;
}

static int tc_abort(tconn_t *c, const char *reason) {
    if (c->s != -1)
	close(c->s);
    c->s = -1;
    fprintf(stderr, "* ERROR * tc_abort: %s\n", reason);
    return -1;
}

static void tc_flush(tconn_t *c) {
    if (c->s != -1 && c->send_len) {
	int n, sp = 0;
	uint8_t fl[4];
#if RC_DEBUG
	int i;
	fprintf(stderr, "INFO.send:");
	for (i = 0; i < c->send_len; i++) fprintf(stderr, " %02x", (int) ((uint8_t*)c->send_buf)[i]);
	fprintf(stderr, "  ");
	for (i = 0; i < c->send_len; i++) fprintf(stderr, "%c", (((uint8_t*)c->send_buf)[i] > 31 && ((uint8_t*)c->send_buf)[i] < 128) ? ((uint8_t*)c->send_buf)[i] : '.');
	fprintf(stderr, "\n");
#endif
	fl[0] = c->send_len >> 24;
	fl[1] = (c->send_len >> 16) & 255;
	fl[2] = (c->send_len >> 8) & 255;
	fl[3] = c->send_len & 255;
	if (send(c->s, fl, 4, 0) != 4)
	    tc_abort(c, "send error (frame head)");
	else {
	    while (sp < c->send_len &&
		   (n = send(c->s, c->send_buf + sp, c->send_len - sp, 0)) > 0)
		sp += n;
	    if (sp < c->send_len)
		tc_abort(c, "send error");
	}
    }
    c->send_len = 0;
}

static void tc_close(tconn_t *c) {
    if (!c) return;
    if (c->s != -1) {
	tc_flush(c);
	close(c->s);
    }
    free(c->send_buf);
    free(c->recv_buf0);
    free(c);
}

static void tc_write(tconn_t *c, const void *buf, int len) {
    const char *cb = (const char*) buf;
    while (c->send_len + len > c->send_alloc) {
	int ts = c->send_alloc - c->send_len;
	if (ts) {
	    memcpy(c->send_buf + c->send_len, cb, ts);
	    c->send_len += ts;
	    cb += ts;
	    len -= ts;
	}
	tc_flush(c);
    }
    memcpy(c->send_buf + c->send_len, cb, len);
    c->send_len += len;
}

static int tc_read(tconn_t *c, int needed) {
    int n;
    if (needed < 0) return tc_abort(c, "attempt to read negative number of bytes (integer overflow?)");
    /* printf("[%d.read(%d)[%d/%d/%d|%d]\n", c->s, needed, (int)(c->recv_buf - c->recv_buf0), c->recv_next, c->recv_len, c->recv_frame); */
    if (c->s == -1) return -1;
    if (c->next_char) { /* this is an ugly hack that comes from the fact that the original code assumed unframed reads ... */
	c->recv_buf0[c->recv_next] = c->next_char;
	c->next_char = 0;
    }
    /* all in memory ? */
    if (needed <= c->recv_len - c->recv_next) {
	c->recv_buf = c->recv_buf0 + c->recv_next;
	c->recv_next += needed;
	return needed;
    }
    if (c->recv_frame < 1) { /* read a new frame */
	uint8_t fl[4];
	int n = recv(c->s, fl, 4, 0), rn;
	if (n == 0) return tc_abort(c, "connection closed by peer (frame head)");
	if (n < 0) return tc_abort(c, "read error (frame head)");
	rn = c->recv_frame = fl[3] | (fl[2] << 8) | (fl[1] << 16) | (fl[0] << 24);
#if RC_DEBUG
	fprintf(stderr, "INFO: read frame %d bytes\n", rn);
#endif
	if (rn >= c->recv_alloc) rn = c->recv_alloc - 1;
	c->recv_buf = c->recv_buf0;
	n = recv(c->s, c->recv_buf, rn, 0);
	if (n < 0) return tc_abort(c, "read error");
	c->recv_frame -= (c->recv_len = n);
	c->recv_buf = c->recv_buf0;
#if RC_DEBUG
	int i;
	fprintf(stderr, "INFO.recv:");
	for (i = 0; i < c->recv_len; i++) fprintf(stderr, " %02x", (int) ((uint8_t*)c->recv_buf)[i]);
	fprintf(stderr, "  ");
	for (i = 0; i < c->recv_len; i++) fprintf(stderr, "%c", (((uint8_t*)c->recv_buf)[i] > 31 && ((uint8_t*)c->recv_buf)[i] < 128) ? ((uint8_t*)c->recv_buf)[i] : '.');
	fprintf(stderr, "\n");
#endif
	
	if (needed <= n)
	    return c->recv_next = needed;
    }
    if (c->recv_next) { /* move content to align with the buffer */
	if (c->recv_next < c->recv_len)
	    memmove(c->recv_buf0, c->recv_buf0 + c->recv_next, c->recv_len - c->recv_next);
	c->recv_buf = c->recv_buf0;
	c->recv_len -= c->recv_next;
	c->recv_next = 0;
    }
    if (needed >= c->recv_alloc) {
	unsigned int nall = ((unsigned int) needed) + (((unsigned int)needed) >> 2);
	void *v = realloc(c->recv_buf0, nall);
	if (!v)
	    return tc_abort(c, "out of memory");
	c->recv_buf0 = c->recv_buf = v;
	c->recv_alloc = nall;
    }
    if (c->recv_frame > needed) { /* try to read more of the frame first */
	int rn = c->recv_frame;
	if (rn >= c->recv_alloc - c->recv_len) rn = c->recv_alloc - c->recv_len - 1;
	n = recv(c->s, c->recv_buf0 + c->recv_len, rn, 0);
	if (n < 0) return tc_abort(c, "read error");
	c->recv_len += n;
	c->recv_frame -= n;
    }
    while (c->recv_len < needed) {
	if (needed - c->recv_len > c->recv_frame) return tc_abort(c, "attempt to read across frames");
	n = recv(c->s, c->recv_buf + c->recv_len, needed - c->recv_len, 0);
#if RC_DEBUG
	fprintf(stderr, "INFO.recv: read(%d/%d) = %d\n", c->recv_len, needed, n);
#endif
	if (n == 0) return tc_abort(c, "connection closed by peer");
	if (n < 0) return tc_abort(c, "read error");
	c->recv_len += n;
	c->recv_frame -= n;
    }
    return c->recv_next = needed;
}

static uint8_t tc_read_u8(tconn_t *c) {
    tc_read(c, 1);
    return *((uint8_t*) c->recv_buf);
}

static int tc_read_i16(tconn_t *c) {
    uint8_t *ub;
    tc_read(c, 2);
    ub = (uint8_t*) c->recv_buf;
    return ((ub[0] & 255) <<  8) | (ub[1] & 255);
}

static int tc_read_i32(tconn_t *c) {
    uint8_t *ub;
    tc_read(c, 4);
    ub = (uint8_t*) c->recv_buf;
    return ((ub[0] & 255) << 24) | ((ub[1] & 255) << 16) | ((ub[2] & 255) <<  8) | (ub[3] & 255);
}

static int64_t tc_read_i64(tconn_t *c) {
    int64_t i;
    int j;
    char *u = (char*) &i;
    tc_read(c, 8);
    /* FIXME: this assumes litte endianness */
    for (j = 0; j < 8; j++) u[j] = c->recv_buf[7 - j];
    return i;
}

static const char *tc_read_str(tconn_t *c) {
    int len = tc_read_i32(c);
    if (tc_ok(c) && tc_read(c, len) == len) {
	if (tc_ok(c)) {
	    c->next_char = c->recv_buf[len];
	    c->recv_buf[len] = 0; /* read guarantees that an extra byte is available */
	    return (const char*) c->recv_buf;
	}
    }
    return 0;
}

/* -- Thrift protocol building blocks -- */

enum {
  TMessageType_CALL      = 1,
  TMessageType_REPLY     = 2,
  TMessageType_EXCEPTION = 3,
  TMessageType_ONEWAY    = 4
};

enum {
  TType_STOP   = 0,
  TType_VOID   = 1,
  TType_BOOL   = 2,
  TType_BYTE   = 3,
  TType_DOUBLE = 4,
  TType_I16    = 6,
  TType_I32    = 8,
  TType_I64    = 10, /* 0x0a */
  TType_STRING = 11, /* 0x0b */
  TType_STRUCT = 12, /* 0x0c */
  TType_MAP    = 13, /* 0x0d */
  TType_SET    = 14, /* 0x0e */
  TType_LIST   = 15  /* 0x0f */
};

#define VERSION_1    0x80010000
#define VERSION_MASK 0xffff0000

static void tc_write_u8(tconn_t *c, uint8_t b) {
    if (c->send_len + 1 > c->send_alloc)
	tc_flush(c);
    c->send_buf[c->send_len++] = (char) b;
}

static void tc_write_i16(tconn_t *c, short i) {
    if (c->send_len + 2 > c->send_alloc)
	tc_flush(c);
    c->send_buf[c->send_len++] = (i >> 8) & 255;
    c->send_buf[c->send_len++] = i & 255;    
}

static void tc_write_i32(tconn_t *c, int i) {
    if (c->send_len + 4 > c->send_alloc)
	tc_flush(c);
    c->send_buf[c->send_len++] = (i >> 24) & 255;
    c->send_buf[c->send_len++] = (i >> 16) & 255;
    c->send_buf[c->send_len++] = (i >> 8) & 255;
    c->send_buf[c->send_len++] = i & 255;    
}

static void tc_write_str(tconn_t *c, const char *s) {
    int len = s ? strlen(s) : 0;
    tc_write_i32(c, len);
    if (len) tc_write(c, s, len);
}

static void tc_write_field(tconn_t *c, int type, int id) {
    tc_write_u8(c, type);
    tc_write_i16(c, id);
}

static void tc_write_stop(tconn_t *c) {
    tc_write_u8(c, TType_STOP);
}

static void tc_write_msg(tconn_t *c, const char *name, int type, int seq) {
#if 1 /* strict */
    tc_write_i32(c, VERSION_1 | type);
    tc_write_str(c, name);
    tc_write_i32(c, seq);
#else
    tc_write_str(c, name);
    tc_write_i32(c, type);
    tc_write_i32(c, seq);
#endif
}

static void tc_write_fstr(tconn_t *c, int seq, const char *str) {
    tc_write_field(c, TType_STRING, seq);
    tc_write_str(c, str);
}

static void tc_write_map(tconn_t *c, int keyType, int valType, int size) {
    tc_write_u8(c, keyType);
    tc_write_u8(c, valType);
    tc_write_i32(c, size);
}

typedef struct msg {
    char *name;    /* message name */
    int type, seq; /* message type and id */
    int rest, rid; /* result type and result id */
} msg_t;

/* reads a message including the type/id of the first element.
   rid is only valid if rest != TType_STOP.
   returns NULL on protocol error
*/
static msg_t *tc_read_msg(tconn_t *c, msg_t *msg) {
    int size = tc_read_i32(c);

    if (!tc_ok(c)) return 0;
    if (size < 0) { /* strict */
	const char *ms;
	if ((size & VERSION_MASK) != VERSION_1) {
	    tc_abort(c, "bad version");
	    return 0;
	}
	msg->type = size & 255;
	ms = tc_read_str(c);
	msg->name = ms ? strdup(ms) : 0;
	msg->seq = tc_read_i32(c);
    } else {
	if (tc_read(c, size) != size) return 0;
	c->recv_buf[size] = 0;
	msg->name = strdup(c->recv_buf);
	msg->type = tc_read_i32(c);
	msg->seq = tc_read_i32(c);
    }
    msg->rid = -1;
    if (tc_ok(c) && (msg->rest = tc_read_u8(c)))
	msg->rid = tc_read_i16(c);
    return tc_ok(c) ? msg : 0;
}

/* handling of unknown/unexpected payload -- skip value and/or all fields */

static void tc_skip_fields(tconn_t *c);

static void tc_skip_value(tconn_t *c, int type) {
    switch (type) {
    case TType_STOP:
    case TType_VOID:
	break;
    case TType_BYTE:
    case TType_BOOL:
	tc_read_u8(c);
	break;
    case TType_DOUBLE:
    case TType_I64:
	tc_read(c, 8);
	break;
    case TType_I16:
	tc_read(c, 2);
	break;
    case TType_I32:
	tc_read(c, 4);
	break;
    case TType_STRING:
	tc_read_str(c);
	break;
    case TType_MAP: {
	int tk = tc_read_u8(c);
	int tv = tc_read_u8(c);
	int ct = tc_read_i32(c);
	int i;
	for (i = 0; i < ct; i++) {
	    tc_skip_value(c, tk);
	    tc_skip_value(c, tv);
	}
	break;
    }
    case TType_SET:
    case TType_LIST:
	{
	    int ty = tc_read_u8(c);
	    int ct = tc_read_i32(c);
	    int i;
	    for (i = 0; i < ct; i++)
		tc_skip_value(c, ty);
	    break;
	}	
    case TType_STRUCT: {
	tc_skip_fields(c);
	break;
    }
    }
}

static void tc_skip_fields(tconn_t *c) {
    int type;
    while (tc_ok(c) && (type = tc_read_u8(c)) != 0) {
	/* id */ tc_read_i16(c);
	tc_skip_value(c, type);
    }
}

/* --- Cassandra --- */

static char *string_msg(tconn_t *c, const char *msg) {
    msg_t m;
    tc_write_msg(c, msg, TMessageType_CALL, c->seq++);
    tc_write_stop(c);
    tc_flush(c);
    if (tc_read_msg(c, &m)) {
	if (m.rest == TType_STRING) {
	    const char *cn;
	    char *s;
	    cn = tc_read_str(c);
	    fprintf(stderr, "INFO: msg '%s', type=%d, seq=%d: '%s'\n", m.name ? m.name : "<NULL>", m.type, m.seq, cn ? cn : "<NULL>");
	    s = strdup(cn);
	    tc_skip_fields(c);
	    return s;
	} else {
	    if (m.rest) {
		tc_skip_value(c, m.rest);
		tc_skip_fields(c);
	    }
	}
    }
    return 0;
}

#define describe_cluster_name(C) string_msg(C, "describe_cluster_name")
#define describe_version(C) string_msg(C, "describe_version")

typedef enum ConsistencyLevel {
    ONE = 1,
    QUORUM = 2,
    LOCAL_QUORUM = 3,
    EACH_QUORUM = 4,
    ALL = 5,
    ANY = 6,
    TWO = 7,
    THREE = 8,
} ConsistencyLevel;


/* --- R API -- */

#define R2UTF8(X) translateCharUTF8(STRING_ELT(X, 0))

static void tconn_fin(SEXP what) {
    tconn_t *c = (tconn_t*) EXTPTR_PTR(what);
    if (c) tc_close(c);
}

SEXP RC_connect(SEXP s_host, SEXP s_port) {
    int port = asInteger(s_port);
    const char *host;
    tconn_t *c;
    SEXP res;

    if (port < 1 || port > 65534)
	Rf_error("Invalid port number");
    if (s_host == R_NilValue)
	host = "127.0.0.1";
    else {
	if (TYPEOF(s_host) != STRSXP || LENGTH(s_host) != 1)
	    Rf_error("host must be a character vector of length one");
	host = translateCharUTF8(STRING_ELT(s_host, 0));
    }
    c = tc_connect(host, port);
    if (!c)
	Rf_error("cannot connect to %s:%d", host, port);
    res = PROTECT(R_MakeExternalPtr(c, R_NilValue, R_NilValue));
    setAttrib(res, R_ClassSymbol, mkString("CassandraConnection"));
    R_RegisterCFinalizer(res, tconn_fin);
    UNPROTECT(1);
    return res;
}

SEXP RC_close(SEXP sc) {
    tconn_t *c;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    c = (tconn_t*) EXTPTR_PTR(sc);
    /* we can't use tc_close because it frees the connection object */
    close(c->s);
    c->s = -1;
    return R_NilValue;
}

SEXP RC_cluster_name(SEXP sc) {
    tconn_t *c;
    char *s;
    SEXP res;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    c = (tconn_t*) EXTPTR_PTR(sc);
    s = describe_cluster_name(c);
    if (!s) Rf_error("cannot obtain cluster name");
    res = mkCharCE(s, CE_UTF8);
    free(s);
    return ScalarString(res);
}

SEXP RC_version(SEXP sc) {
    tconn_t *c;
    char *s;
    SEXP res;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    c = (tconn_t*) EXTPTR_PTR(sc);
    s = describe_version(c);
    if (!s) Rf_error("cannot obtain version");
    res = mkCharCE(s, CE_UTF8);
    free(s);
    return ScalarString(res);
}

/* read call response struct payload, parse out exceptions and raise them using Rf_error, discard everything else */
static void RC_void_ex(tconn_t *c, int rest) {
    if (rest == TType_STOP)
	return;
    if (rest == TType_STRUCT) { /* exception */
	int pt = tc_read_u8(c);
	if (pt) {
	    int id = tc_read_i16(c);
	    if (pt == TType_STRING) {
		char err[256];
		const char *es = tc_read_str(c);
		snprintf(err, sizeof(err), "%s", es);
		tc_skip_fields(c); /* ex struct */
		tc_skip_fields(c); /* call struct */
		Rf_error("Cassandra exception: %s", err);
	    } else {
		tc_skip_value(c, pt);
		tc_skip_fields(c);
	    }
	}
    } else if (rest == TType_STRING) { /* exception without a wrapper */
	char err[256];
	const char *es = tc_read_str(c);
	snprintf(err, sizeof(err), "%s", es);
	tc_skip_fields(c); /* call struct */
	Rf_error("Cassandra exception: %s", err);
    } else {
	tc_skip_value(c, rest);
	tc_skip_fields(c);
    }
}

SEXP RC_use(SEXP sc, SEXP key_space) {
    tconn_t *c;
    msg_t m;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(key_space) != STRSXP || LENGTH(key_space) != 1) Rf_error("key space must be a character vector of length one");
    c = (tconn_t*) EXTPTR_PTR(sc);
    tc_write_msg(c, "set_keyspace", TMessageType_CALL, c->seq++);
    tc_write_fstr(c, 1, R2UTF8(key_space)); /* keyspace */
    tc_write_stop(c);
    tc_flush(c);
    if (tc_read_msg(c, &m)) {
	RC_void_ex(c, m.rest);
	return sc;
    }
    Rf_error("error setting keyspace (unrecognized response)");
    return sc;
}

SEXP RC_login(SEXP sc, SEXP user, SEXP pwd) {
    tconn_t *c;
    msg_t m;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(user) != STRSXP || LENGTH(user) != 1) Rf_error("user name must be a character vector of length one");
    if (TYPEOF(pwd) != STRSXP || LENGTH(pwd) != 1) Rf_error("password must be a character vector of length one");
    c = (tconn_t*) EXTPTR_PTR(sc);
    tc_write_msg(c, "login", TMessageType_CALL, c->seq++);
    tc_write_field(c, TType_STRUCT, 1); /* auth_request */
    tc_write_field(c, TType_MAP, 1); /* credentials */
    tc_write_map(c, TType_STRING, TType_STRING, 2);
    tc_write_str(c, "username");
    tc_write_str(c, R2UTF8(user));
    tc_write_str(c, "password");
    tc_write_str(c, R2UTF8(pwd));
    tc_write_stop(c);
    tc_write_stop(c);
    tc_flush(c);
    if (tc_read_msg(c, &m)) {
	RC_void_ex(c, m.rest);
	return sc;
    }
    Rf_error("login error (unrecognized response)");
    return sc;
}

#if RC_DEBUG
static SEXP RC_read_fields(tconn_t *c, int level);

static const char *type_name[] = { "STOP", "void", "bool", "byte", "double", "#5", "i16", "#7", "i32", "#9", "i64", "string", "struct", "map", "set", "list" };

static void read_value(tconn_t *c, int type, int level) {
    switch (type) {
    case TType_BYTE:
	Rprintf("0x%02x", tc_read_u8(c));
	break;
    case TType_BOOL:
	Rprintf((tc_read_u8(c) == 1) ? "TRUE" : "FALSE");
	break;
    case TType_DOUBLE: {
	double d;
	char *e = (char *)&d;
	tc_read(c, 8);
	{ int i; for (i = 7; i >= 0; i--) *(e++) = c->recv_buf[i]; }
	Rprintf("%g", d);
	break;
    }
    case TType_I16:
	Rprintf("%d", tc_read_i16(c));
	break;
    case TType_I32:
	Rprintf("%d", tc_read_i32(c));
	break;
    case TType_I64:
	Rprintf("%ld", (long) tc_read_i64(c));
	break;
    case TType_STRING: {
	const char *s = tc_read_str(c);
	Rprintf("'%s'", s);
	break;
    }
    case TType_MAP: {
	int tk = tc_read_u8(c);
	int tv = tc_read_u8(c);
	int ct = tc_read_i32(c);
	int i;
	for (i = 0; i < ct; i++) {
	    Rprintf("\n");
	    { int i; for (i = 0; i < level; i++) Rprintf(" "); }
	    read_value(c, tk, level); Rprintf(" -> ");
	    read_value(c, tv, level);
	}
	break;
    }
    case TType_LIST: {
	int ty = tc_read_u8(c);
	int ct = tc_read_i32(c);
	int i;
	for (i = 0; i < ct; i++) {
	    Rprintf("\n");
	    { int i; for (i = 0; i < level; i++) Rprintf(" "); }
	    read_value(c, ty, level);
	}
	break;
    }	
    case TType_STRUCT: {
	Rprintf("\n");
	RC_read_fields(c, level + 1);
	break;
    }
    default:
	Rprintf("<???>");
    }
}

static SEXP RC_read_fields(tconn_t *c, int level) {
    int type;
    while (tc_ok(c) && (type = tc_read_u8(c)) != 0) {
	int id = tc_read_i16(c);
	{ int i; for (i = 0; i < level; i++) Rprintf(" "); }
	Rprintf("%d) %d [%s]: ", id, type, (type < 16 && type >= 0) ? type_name[type] : "???");
	read_value(c, type, level);
	Rprintf("\n");
    }
    return R_NilValue;
}

#endif

SEXP RC_get(SEXP sc, SEXP key, SEXP cf, SEXP col) {
    ConsistencyLevel cl = ONE;
    msg_t m;
    tconn_t *c;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(key) != STRSXP || LENGTH(key) != 1) Rf_error("key must be a character vector of length one");
    if (TYPEOF(cf) != STRSXP || LENGTH(cf) != 1) Rf_error("column family must be a character vector of length one");
    if (TYPEOF(col) != STRSXP || LENGTH(col) != 1) Rf_error("column must be a character vector of length one");
    c = (tconn_t*) EXTPTR_PTR(sc);
    tc_write_msg(c, "get", TMessageType_CALL, c->seq++);
    tc_write_fstr(c, 1, R2UTF8(key)); /* key */
    /* ColumnPath */
    tc_write_field(c, TType_STRUCT, 2);
    tc_write_fstr(c, 3, R2UTF8(cf));
    tc_write_fstr(c, 5, R2UTF8(col));
    tc_write_stop(c);
    /* consistency level */
    tc_write_field(c, TType_I32, 3);
    tc_write_i32(c, cl);
    tc_write_stop(c);
    tc_flush(c);
    if (tc_read_msg(c, &m)) {
	if (m.rest)
	    tc_skip_value(c, m.rest);
	tc_skip_fields(c);
	return R_NilValue;
    }
    Rf_error("error reading response");
    return sc;
}

/* read list payload of a result -- either a data frame of R_NilValue
   if fin_call == 0 then it only reads the list, otherwise it skips outof the struct/call containing the list
*/
static SEXP list_result(tconn_t *c, int fin_call) {
    int vt = tc_read_u8(c);
    int i, n = tc_read_i32(c);
#ifdef RC_DEBUG
    fprintf(stderr, "list, n = %d\n", n);
#endif
    if (tc_ok(c) && vt == TType_STRUCT && n >= 0) {
	SEXP sk, sv, st, rnv, res;
	double *ts;
	PROTECT(res = mkNamed(VECSXP, (const char *[]) { "key", "value", "ts", "" }));
	SET_VECTOR_ELT(res, 0, (sk = allocVector(STRSXP, n)));
	SET_VECTOR_ELT(res, 1, (sv = allocVector(STRSXP, n)));
	ts = REAL(SET_VECTOR_ELT(res, 2, (st = allocVector(REALSXP, n))));
	rnv = allocVector(INTSXP, 2);
	INTEGER(rnv)[0] = NA_INTEGER;
	INTEGER(rnv)[1] = -n;
	setAttrib(res, R_RowNamesSymbol, rnv);
	setAttrib(res, R_ClassSymbol, mkString("data.frame"));
	for (i = 0; i < n; i++) {
	    int pt = tc_read_u8(c);
	    tc_read_i16(c); /* id */
	    /* printf(" -- %d) %d\n", i + 1, pt); */
	    if (pt == TType_STRUCT) {
		while ((pt = tc_read_u8(c)) && tc_ok(c)) {
		    int pd = tc_read_i16(c);
		    /* printf(" -- %d) type=%d, id=%d\n", i + 1, pt, pd); */
		    if (pt == TType_STRING) {
			const char *cc = tc_read_str(c);
			if (cc) {
			    if (pd == 1)
				SET_STRING_ELT(sk, i, mkCharCE(cc, CE_UTF8));
			    else if (pd == 2)
				SET_STRING_ELT(sv, i, mkCharCE(cc, CE_UTF8));
			}
		    } else if (pt == TType_I64) {
			int64_t v = tc_read_i64(c);
			if (pd == 3)
			    ts[i] = (double) v;
		    } else tc_skip_value(c, pt);
		}
	    } else
		tc_skip_value(c, pt);
	    tc_skip_fields(c); /* end of the struct in the list */
	}
	if (fin_call) tc_skip_fields(c); /* call struct */
	UNPROTECT(1);
	return res;
    } else if (tc_ok(c) && n > 0) { /* non-struct payload, skip */
	for (i = 0; i < n; i++)
	    tc_skip_value(c, vt);
    }
    if (fin_call) tc_skip_fields(c);
    return R_NilValue;
}

SEXP RC_get_range(SEXP sc, SEXP key, SEXP cf, SEXP first, SEXP last, SEXP limit, SEXP rev) {
    ConsistencyLevel cl = ONE;
    msg_t m;
    tconn_t *c;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(key) != STRSXP || LENGTH(key) != 1) Rf_error("key must be a character vector of length one");
    if (TYPEOF(cf) != STRSXP || LENGTH(cf) != 1) Rf_error("column family must be a character vector of length one");
    if (TYPEOF(first) != STRSXP || LENGTH(first) != 1) Rf_error("column must be a character vector of length one");
    if (TYPEOF(last) != STRSXP || LENGTH(last) != 1) Rf_error("column must be a character vector of length one");
    c = (tconn_t*) EXTPTR_PTR(sc);

    tc_write_msg(c, "get_slice", TMessageType_CALL, c->seq++);
    tc_write_fstr(c, 1, R2UTF8(key)); /* key */
    /* ColumnParent */
    tc_write_field(c, TType_STRUCT, 2);
    tc_write_fstr(c, 3, R2UTF8(cf));
    tc_write_stop(c);
    /* SlicePredicate */
    tc_write_field(c, TType_STRUCT, 3);
    /* SliceRange */
    tc_write_field(c, TType_STRUCT, 2);
    tc_write_fstr(c, 1, R2UTF8(first));
    tc_write_fstr(c, 2, R2UTF8(last));
    tc_write_field(c, TType_BOOL, 3); tc_write_u8(c, (asInteger(rev) == 1) ? 1 : 0);
    tc_write_field(c, TType_I32, 4);  tc_write_i32(c, asInteger(limit));
    tc_write_stop(c); /* SR */
    tc_write_stop(c); /* SP */
    /* consistency level */
    tc_write_field(c, TType_I32, 3);
    tc_write_i32(c, cl);
    tc_write_stop(c);
    tc_flush(c);
    if (tc_read_msg(c, &m)) {
	if (m.rest == TType_STOP)
	    Rf_error("missing result object from Cassandra");
	if (m.rest == TType_LIST) { /* the result should be a list */
	    SEXP res = list_result(c, 1);
	    if (res != R_NilValue)
		return res;
	} else { /* not a list - skip it and raise an error */
	    RC_void_ex(c, m.rest);
	    Rf_error("invalid result type (%d)", m.rest);
	}	
	tc_skip_fields(c); /* this is for the call result struct */
    }
    Rf_error("error obtaining result");
    return R_NilValue;
}

SEXP RC_mget_range(SEXP sc, SEXP keys, SEXP cf, SEXP first, SEXP last, SEXP limit, SEXP rev) {
    ConsistencyLevel cl = ONE;
    int i;
    msg_t m;
    tconn_t *c;

    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(keys) != STRSXP) Rf_error("keys must be a character vector");
    if (TYPEOF(cf) != STRSXP || LENGTH(cf) != 1) Rf_error("column family must be a character vector of length one");
    if (TYPEOF(first) != STRSXP || LENGTH(first) != 1) Rf_error("column must be a character vector of length one");
    if (TYPEOF(last) != STRSXP || LENGTH(last) != 1) Rf_error("column must be a character vector of length one");
    c = (tconn_t*) EXTPTR_PTR(sc);
    tc_write_msg(c, "multiget_slice", TMessageType_CALL, c->seq++);
    tc_write_field(c, TType_LIST, 1);
    tc_write_u8(c, TType_STRING);
    tc_write_i32(c, LENGTH(keys));
    for (i = 0; i < LENGTH(keys); i++) tc_write_str(c, translateCharUTF8(STRING_ELT(keys, i)));
    /* ColumnParent */
    tc_write_field(c, TType_STRUCT, 2);
    tc_write_fstr(c, 3, translateCharUTF8(STRING_ELT(cf, 0)));
    tc_write_stop(c);
    /* SlicePredicate */
    tc_write_field(c, TType_STRUCT, 3);
    /* SliceRange */
    tc_write_field(c, TType_STRUCT, 2);
    tc_write_fstr(c, 1, translateCharUTF8(STRING_ELT(first, 0)));
    tc_write_fstr(c, 2, translateCharUTF8(STRING_ELT(last, 0)));
    tc_write_field(c, TType_BOOL, 3); tc_write_u8(c, asInteger(rev) ? 1 : 0);
    tc_write_field(c, TType_I32, 4);  tc_write_i32(c, asInteger(limit));
    tc_write_stop(c); /* SR */
    tc_write_stop(c); /* SP */
    /* consistency level */
    tc_write_field(c, TType_I32, 3);
    tc_write_i32(c, cl);
    tc_write_stop(c);
    tc_flush(c);
    if (tc_read_msg(c, &m)) {
	SEXP res, nv;
	if (m.rest == TType_STOP)
	    Rf_error("missing result object from Cassandra");
	if (m.rest == TType_MAP) { /* the result should be always a map */
	    int tk = tc_read_u8(c);
	    int tv = tc_read_u8(c);
	    int ct = tc_read_i32(c), i;
	    if (tk != TType_STRING || tv != TType_LIST) {
		for (i = 0; i < ct; i++) {
		    tc_skip_value(c, tk);
		    tc_skip_value(c, tv);
		}
		tc_skip_fields(c); /* call struct */
		if (tk != TType_STRING)
		    Rf_error("Unsupported key type (%d)", tk);
		Rf_error("Invalid value type (%d)", tv);
	    }
	    res = PROTECT(allocVector(VECSXP, ct));
	    nv = allocVector(STRSXP, ct);
	    setAttrib(res, R_NamesSymbol, nv);
	    for (i = 0; i < ct; i++) {
		const char *ns = tc_read_str(c);
		if (ns) SET_STRING_ELT(nv, i, mkCharCE(ns, CE_UTF8));
		SET_VECTOR_ELT(res, i, list_result(c, 0));
	    }
	    tc_skip_fields(c);
	    UNPROTECT(1);
	    return res;
	} else RC_void_ex(c, m.rest); /* otherwise check for exceptions and bail out */
    }
    Rf_error("failed to get result");
    return R_NilValue;
}

SEXP RC_get_range_slices(SEXP sc, SEXP key_f, SEXP key_l, SEXP cf, SEXP first, SEXP last, SEXP limit, SEXP rev, SEXP k_lim, SEXP k_tok) {
    ConsistencyLevel cl = ONE;
    msg_t m;
    tconn_t *c;
    int col_lim = asInteger(limit);

    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(key_f) != STRSXP || TYPEOF(key_l) != STRSXP || LENGTH(key_f) != 1 || LENGTH(key_l) != 1) Rf_error("start/end key must be a character vector of length one");
    if (TYPEOF(cf) != STRSXP || LENGTH(cf) != 1) Rf_error("column family must be a character vector of length one");
    if (TYPEOF(first) != STRSXP || LENGTH(first) != 1) Rf_error("column must be a character vector of length one");
    if (TYPEOF(last) != STRSXP || LENGTH(last) != 1) Rf_error("column must be a character vector of length one");
    c = (tconn_t*) EXTPTR_PTR(sc);
    tc_write_msg(c, "get_range_slices", TMessageType_CALL, c->seq++);
    /* ColumnParent */
    tc_write_field(c, TType_STRUCT, 1);
    tc_write_fstr(c, 3, translateCharUTF8(STRING_ELT(cf, 0)));
    tc_write_stop(c);
    /* SlicePredicate */
    tc_write_field(c, TType_STRUCT, 2);
    /* SliceRange */
    tc_write_field(c, TType_STRUCT, 2);
    tc_write_fstr(c, 1, R2UTF8(first));
    tc_write_fstr(c, 2, R2UTF8(last));
    tc_write_field(c, TType_BOOL, 3); tc_write_u8(c, asInteger(rev) ? 1 : 0);
    tc_write_field(c, TType_I32, 4);  tc_write_i32(c, col_lim);
    tc_write_stop(c); /* SR */
    tc_write_stop(c); /* SP */
    /* KeyRange */
    tc_write_field(c, TType_STRUCT, 3);
    if (asInteger(k_tok) == 1) {
	tc_write_fstr(c, 3, R2UTF8(key_f));
	tc_write_fstr(c, 4, R2UTF8(key_l));
    } else {
	tc_write_fstr(c, 1, R2UTF8(key_f));
	tc_write_fstr(c, 2, R2UTF8(key_l));
    }
    tc_write_field(c, TType_I32, 5);  tc_write_i32(c, asInteger(k_lim));
    tc_write_stop(c); /* KR */    
    /* consistency level */
    tc_write_field(c, TType_I32, 4);
    tc_write_i32(c, cl);
    tc_write_stop(c);
    tc_flush(c);
    if (tc_read_msg(c, &m)) {
	SEXP res, nv;
	if (m.rest == TType_STOP)
	    Rf_error("missing result object from Cassandra");
	if (m.rest == TType_LIST) { /* the result should be always a list (of KeySlice=(1:key, 2:col[])) */
	    int tv = tc_read_u8(c);
	    int ct = tc_read_i32(c), i, inv = 0;
	    if (tv != TType_STRUCT) {
		for (i = 0; i < ct; i++)
		    tc_skip_value(c, tv);
		tc_skip_fields(c); /* call struct */
		Rf_error("Invalid value type (%d) instead of KeySlice", tv);
	    }
	    res = PROTECT(allocVector(VECSXP, ct));
	    nv = allocVector(STRSXP, ct);
	    setAttrib(res, R_NamesSymbol, nv);
	    for (i = 0; i < ct; i++) {
		int ft;
		while (tc_ok(c) && (ft = tc_read_u8(c)) != TType_STOP) {
		    int fi = tc_read_i16(c);
		    if (fi == 1 && ft == TType_STRING) { /* key */
			const char *ns = tc_read_str(c);
			if (ns) SET_STRING_ELT(nv, i, mkCharCE(ns, CE_UTF8));
		    } else if (fi == 2 && ft == TType_LIST) { /* list of columns */
			if (col_lim == 0) /* for flat return don't bother converting the payload */
			    tc_skip_value(c, ft);
			else
			    SET_VECTOR_ELT(res, i, list_result(c, 0));
		    } else {
			inv = 1;
			tc_skip_value(c, fi);
		    }
		}
	    }
	    if (inv) Rf_warning("One or more KeySlices contained unknown or unsupported values");
	    tc_skip_fields(c);
	    UNPROTECT(1);
	    return (col_lim == 0) ? nv: res;
	} else RC_void_ex(c, m.rest); /* otherwise check for exceptions and bail out */
    }
    Rf_error("failed to get result");
    return R_NilValue;
}

/* short-circuit dispatch on [[ for data frames */
SEXP R_get_col(SEXP df, SEXP i) {
    int ii = asInteger(i);
    if (df == R_NilValue) return df;
    if (TYPEOF(df) != VECSXP) Rf_error("`x' is not a list");
    if (ii < 1 || ii > LENGTH(df)) Rf_error("index i = %d is out of bounds (1,...,%d)", ii, LENGTH(df));
    return VECTOR_ELT(df, ii);
}

