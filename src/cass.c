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
#ifdef WIN32
#include <windows.h>
#include <winsock2.h>
static int wsock_up = 0;
#else
#define closesocket(C) close(C)
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#endif
#include <unistd.h>
#include <sys/time.h>

#define USE_RINTERNALS
#include <Rinternals.h>

/* we want to set CL per connection so we (ab)use the conn strcture for it */
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

typedef struct tconn {
    int s, seq;
    unsigned int send_len, send_alloc;
    unsigned int recv_len, recv_alloc, recv_next;
    int recv_frame;
    char *send_buf, *recv_buf, *recv_buf0, next_char;
    ConsistencyLevel cl;
} tconn_t;

#define tc_ok(X) (((X)->s) != -1)

static tconn_t *tc_connect(const char *host, int port) {
    tconn_t *c = (tconn_t*) malloc(sizeof(tconn_t));
#ifdef WIN32
    if (!wsock_up) {
	 WSADATA dt;
	 /* initialize WinSock 1.1 */
	 WSAStartup(0x0101, &dt);
	 wsock_up = 1;
    }
#endif
    c->s = -1;
    c->seq = 0;
    c->recv_frame = -1;
    c->send_alloc = 65536;
    c->send_len = 0;
    c->next_char = 0;
    c->cl = ONE;
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
#ifdef WIN32
	    int al = sizeof(sin);
	    if (WSAStringToAddress((LPSTR)host, sin.sin_family, 0, (struct sockaddr*)&sin, &al) != 0) {
		if (!(haddr = gethostbyname(host))) { /* DNS failed, */
		    closesocket(c->s);
		    c->s = -1;
		}
		sin.sin_addr.s_addr = *((uint32_t*) haddr->h_addr); /* pick first address */
	    }
	    /* for some reason Windows trashes the structure so we need to fill it again */
	    sin.sin_family = AF_INET;
	    sin.sin_port = htons(port);
#else
	    if (inet_pton(sin.sin_family, host, &sin.sin_addr) != 1) { /* invalid, try DNS */
		if (!(haddr = gethostbyname(host))) { /* DNS failed, */
		    closesocket(c->s);
		    c->s = -1;
		}
		sin.sin_addr.s_addr = *((uint32_t*) haddr->h_addr); /* pick first address */
	    }
#endif
	} else
	    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	if (c->s != -1 && connect(c->s, (struct sockaddr*)&sin, sizeof(sin))) {
#ifdef WIN32
	    Rprintf("WSAGetLastError = %d\n", WSAGetLastError());
#endif
	    closesocket(c->s);
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
	closesocket(c->s);
    c->s = -1;
    REprintf("tc_abort: %s\n", reason);
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
	closesocket(c->s);
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

static double tc_read_double(tconn_t *c) {
    double d;
    int j;
    char *u = (char*) &d;
    tc_read(c, 8);
    /* FIXME: this assumes litte endianness */
    for (j = 0; j < 8; j++) u[j] = c->recv_buf[7 - j];
    return d;
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

/* static const char *type_name[] = { "STOP", "void", "bool", "byte", "double", "#5", "i16", "#7", "i32", "#9", "i64", "string", "struct", "map", "set", "list" }; */

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

static void tc_write_i64(tconn_t *c, int64_t i) {
    if (c->send_len + 4 > c->send_alloc)
	tc_flush(c);
    c->send_buf[c->send_len++] = (i >> 56) & 255;
    c->send_buf[c->send_len++] = (i >> 48) & 255;
    c->send_buf[c->send_len++] = (i >> 40) & 255;
    c->send_buf[c->send_len++] = (i >> 32) & 255;    
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

/* --- vanilla R/Thrift type conversion - it is used only where we don't map things natively --- */

static void tc_skip_fields(tconn_t *c);

static SEXP tc_read_value(tconn_t *c, int type) {
    switch (type) {
    case TType_STOP:
    case TType_VOID:
	return R_NilValue;
    case TType_BYTE:
	return ScalarInteger(tc_read_u8(c));
    case TType_BOOL:
	return ScalarLogical(tc_read_u8(c));
    case TType_DOUBLE:
	return ScalarReal(tc_read_double(c));
    case TType_I64:
	return ScalarReal((double)tc_read_i64(c));
    case TType_I16:
	return ScalarInteger(tc_read_i16(c));
    case TType_I32:
	return ScalarInteger(tc_read_i32(c));
    case TType_STRING:
	{
	    const char *cs = tc_read_str(c);
	    return cs ? ScalarString(mkCharCE(cs, CE_UTF8)) : R_NilValue;
	}
    case TType_MAP:
    case TType_SET:
    case TType_LIST:
	{
	    int tk = (type == TType_MAP) ? tc_read_u8(c) : -1;
	    int tv = tc_read_u8(c);
	    int ct = tc_read_i32(c), i;
	    SEXP res, nv = 0;
	    switch (tv) {
	    case TType_STRING:
		res = PROTECT(allocVector(STRSXP, ct)); break;
	    case TType_DOUBLE:
	    case TType_I64:
		res = PROTECT(allocVector(REALSXP, ct)); break;
	    case TType_BYTE:
	    case TType_I16:
	    case TType_I32:
		res = PROTECT(allocVector(INTSXP, ct)); break;
	    case TType_BOOL:
		res = PROTECT(allocVector(LGLSXP, ct)); break;
	    default:
		res = PROTECT(allocVector(VECSXP, ct)); break;
	    }		
	    if (type == TType_MAP) {
		if (tk != TType_STRING)
		    Rf_warning("Only strings are supported as map keys");
		else
		    setAttrib(res, R_NamesSymbol,(nv = allocVector(STRSXP, ct)));
	    }
	    for (i = 0; i < ct; i++) {
		if (type == TType_MAP) {
		    if (tk == TType_STRING) {
			const char *cn = tc_read_str(c);
			if (cn) SET_STRING_ELT(nv, i, mkCharCE(cn, CE_UTF8));
		    } else
			tc_skip_value(c, tk);
		}
		switch (tv) {
		case TType_STRING: {
		    const char *cs = tc_read_str(c);
		    if (cs) SET_STRING_ELT(res, i, mkCharCE(cs, CE_UTF8));
		    break;
		}
		case TType_DOUBLE:
		    REAL(res)[i] = tc_read_double(c); break;
		case TType_I64:
		    REAL(res)[i] = (double) tc_read_i64(c); break;
		case TType_BYTE:
		case TType_BOOL:
		    INTEGER(res)[i] = tc_read_u8(c); break;
		case TType_I16:
		    INTEGER(res)[i] = tc_read_i16(c); break;
		case TType_I32:
		    INTEGER(res)[i] = tc_read_i32(c); break;
		default:
		    SET_VECTOR_ELT(res, i, tc_read_value(c, tv));
		}
	    }
	    UNPROTECT(1);
	    return res;
	}
	break;
    case TType_STRUCT: {
	int ty;
	SEXP head = PROTECT(CONS(R_NilValue, R_NilValue)), tail = head;
	while (tc_ok(c) && ((ty = tc_read_u8(c)) != TType_STOP)) {
	    int id = tc_read_i16(c);
	    SEXP v = PROTECT(list1(tc_read_value(c, ty)));
	    char ids[12];
	    snprintf(ids, sizeof(ids), "%d", id);
	    SET_TAG(v, install(ids));
	    SETCDR((tail == R_NilValue) ? head : tail, v);
	    tail = v;
	    UNPROTECT(1);
	}
	UNPROTECT(1);
	return CDR(head);
	break;
    }
    }
    return R_NilValue;
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
#if RC_DEBUG
	    fprintf(stderr, "INFO: msg '%s', type=%d, seq=%d: '%s'\n", m.name ? m.name : "<NULL>", m.type, m.seq, cn ? cn : "<NULL>");
#endif
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


/* --- R API -- */

#define R2UTF8(X) translateCharUTF8(STRING_ELT(X, 0))

static void tconn_fin(SEXP what) {
    tconn_t *c = (tconn_t*) EXTPTR_PTR(what);
    if (c) tc_close(c);
}

SEXP RC_connect(SEXP sHost, SEXP sPort) {
    int port = asInteger(sPort);
    const char *host;
    tconn_t *c;
    SEXP res;

    if (port < 1 || port > 65534)
	Rf_error("Invalid port number");
    if (sHost == R_NilValue)
	host = "127.0.0.1";
    else {
	if (TYPEOF(sHost) != STRSXP || LENGTH(sHost) != 1)
	    Rf_error("host must be a character vector of length one");
	host = R2UTF8(sHost);
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
    closesocket(c->s);
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
	    /* int id = */ tc_read_i16(c);
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

SEXP RC_get(SEXP sc, SEXP key, SEXP cf, SEXP col) {
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
    tc_write_i32(c, c->cl);
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
    tc_write_i32(c, c->cl);
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

SEXP RC_get_list(SEXP sc, SEXP key, SEXP cf, SEXP cols, SEXP limit, SEXP rev) {
    msg_t m;
    tconn_t *c;
    int n, i;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(key) != STRSXP || LENGTH(key) != 1) Rf_error("key must be a character vector of length one");
    if (TYPEOF(cf) != STRSXP || LENGTH(cf) != 1) Rf_error("column family must be a character vector of length one");
    if (TYPEOF(cols) != STRSXP) Rf_error("columns must be a character vector");
    n = LENGTH(cols);
    c = (tconn_t*) EXTPTR_PTR(sc);

    tc_write_msg(c, "get_slice", TMessageType_CALL, c->seq++);
    tc_write_fstr(c, 1, R2UTF8(key)); /* key */
    /* ColumnParent */
    tc_write_field(c, TType_STRUCT, 2);
    tc_write_fstr(c, 3, R2UTF8(cf));
    tc_write_stop(c);
    /* SlicePredicate */
    tc_write_field(c, TType_STRUCT, 3);
    /* column names list */
    tc_write_field(c, TType_LIST, 1);
    tc_write_u8(c, TType_STRING);
    tc_write_i16(c, n);
    for (i = 0; i < n; i++) tc_write_str(c, translateCharUTF8(STRING_ELT(cols, i)));
    tc_write_stop(c); /* SP */
    /* consistency level */
    tc_write_field(c, TType_I32, 3);
    tc_write_i32(c, c->cl);
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
    tc_write_i32(c, c->cl);
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

SEXP RC_get_range_slices(SEXP sc, SEXP key_f, SEXP key_l, SEXP cf, SEXP first, SEXP last, SEXP limit, SEXP rev, SEXP k_lim, SEXP k_tok, SEXP sFixed) {
    msg_t m;
    tconn_t *c;
    int col_lim = asInteger(limit);
    int fixed = asInteger(sFixed) == 1;

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
    tc_write_i32(c, c->cl);
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
	    if (fixed) {
		SEXP cols = R_NilValue, ctail = cols;
		SEXP rnv = PROTECT(allocVector(STRSXP, ct)), cnv, res;
		int nc = 0;
		for (i = 0; i < ct; i++) {
		    int ft;
		    while (tc_ok(c) && (ft = tc_read_u8(c)) != TType_STOP) {
			int fi = tc_read_i16(c);
			if (fi == 1 && ft == TType_STRING) { /* key */
			    const char *rns = tc_read_str(c);
			    if (rns) SET_STRING_ELT(rnv, i, mkCharCE(rns, CE_UTF8));
			} else if (fi == 2 && ft == TType_LIST) { /* list of columns */
			    int vt = tc_read_u8(c);
			    int j, n = tc_read_i32(c);
			    if (tc_ok(c) && vt == TType_STRUCT && n >= 0) {
				for (j = 0; j < n; j++) {
				    int pt = tc_read_u8(c);
				    SEXP cv = 0;
				    tc_read_i16(c); /* id */
				    /* printf(" -- %d) %d\n", i + 1, pt); */
				    if (pt == TType_STRUCT) {
					while ((pt = tc_read_u8(c)) && tc_ok(c)) {
					    int pd = tc_read_i16(c);
					    /* printf(" -- %d) type=%d, id=%d\n", i + 1, pt, pd); */
					    if (pt == TType_STRING) {
						const char *cc = tc_read_str(c);
						if (cc) {
						    if (pd == 1) { /* column name */
							SEXP cs = cols;
							while (cs != R_NilValue) {
							    if (!strcmp(CHAR(TAG(cs)), cc)) break;
							    cs = CDR(cs);
							}
							if (cs == R_NilValue) { /* new column vector */
							    cv = allocVector(STRSXP, ct);
							    if (cols == R_NilValue) {
								ctail = cols = list1(cv);
								PROTECT(cols);
								SET_TAG(cols, mkCharCE(cc, CE_UTF8));
							    } else {
								SEXP newc = list1(cv);
								SETCDR(ctail, newc);
								SET_TAG(newc, mkCharCE(cc, CE_UTF8));
								ctail = newc;
							    }
							    nc++;
							} else cv = CAR(cs);
						    } else if (pd == 2 && cv) /* value */
							SET_STRING_ELT(cv, i, mkCharCE(cc, CE_UTF8));
						}
					    } else tc_skip_value(c, pt);
					}
				    } else
					tc_skip_value(c, pt);
				    tc_skip_fields(c); /* end of the struct in the list */
				}
			    } else if (tc_ok(c) && n > 0) { /* non-struct payload, skip */
				for (j = 0; j < n; j++)
				    tc_skip_value(c, vt);
			    }
			} else {
			    inv = 1;
			    tc_skip_value(c, fi);
			}
		    }
		}
		res = PROTECT(allocVector(VECSXP, nc));
		setAttrib(res, R_RowNamesSymbol, rnv);
		setAttrib(res, R_NamesSymbol, (cnv = allocVector(STRSXP, nc)));
		for (i = 0; i < nc; i++) {
		    if (TAG(cols) != R_NilValue)
			SET_STRING_ELT(cnv, i, TAG(cols));
		    SET_VECTOR_ELT(res, i, CAR(cols));
		    cols = CDR(cols);
		}
		setAttrib(res, R_ClassSymbol, mkString("data.frame"));
		UNPROTECT(nc ? 3 : 2);
		return res;
	    } else {
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


SEXP RC_describe_splits(SEXP sc, SEXP cf, SEXP s_tok, SEXP e_tok, SEXP nKeys) {
    msg_t m;
    tconn_t *c;

    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(s_tok) != STRSXP || TYPEOF(e_tok) != STRSXP || LENGTH(s_tok) != 1 || LENGTH(e_tok) != 1) Rf_error("token must be a character vector of length one");
    if (TYPEOF(cf) != STRSXP || LENGTH(cf) != 1) Rf_error("column family must be a character vector of length one");
    c = (tconn_t*) EXTPTR_PTR(sc);
    tc_write_msg(c, "describe_splits", TMessageType_CALL, c->seq++);
    tc_write_fstr(c, 1, R2UTF8(cf));
    tc_write_fstr(c, 2, R2UTF8(s_tok));
    tc_write_fstr(c, 3, R2UTF8(e_tok));
    tc_write_field(c, TType_I32, 4); tc_write_i32(c, asInteger(nKeys));
    tc_write_stop(c);
    tc_flush(c);
    if (tc_read_msg(c, &m) && m.rest) {
	SEXP res = tc_read_value(c, m.rest);
	tc_skip_fields(c);
	return res;
    }
    return R_NilValue;    
}

static int64_t now() {
    int64_t v;
    struct timeval tv;
    gettimeofday(&tv, 0);
    v = tv.tv_sec;
    v *= 1000000;
    v += tv.tv_usec;
    return v;
}

SEXP RC_insert(SEXP sc, SEXP key, SEXP cf, SEXP col, SEXP val) {
    msg_t m;
    tconn_t *c;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(key) != STRSXP || LENGTH(key) != 1) Rf_error("key must be a character vector of length one");
    if (TYPEOF(cf) != STRSXP || LENGTH(cf) != 1) Rf_error("column family must be a character vector of length one");
    if (TYPEOF(col) != STRSXP || LENGTH(col) != 1) Rf_error("column must be a character vector of length one");
    if (val != R_NilValue && (TYPEOF(val) != STRSXP || LENGTH(val) != 1)) Rf_error("value must be a character vector of length one or NULL");
    c = (tconn_t*) EXTPTR_PTR(sc);

    tc_write_msg(c, "insert", TMessageType_CALL, c->seq++);
    tc_write_fstr(c, 1, R2UTF8(key)); /* key */
    /* ColumnParent */
    tc_write_field(c, TType_STRUCT, 2);
    tc_write_fstr(c, 3, R2UTF8(cf));
    tc_write_stop(c);
    /* Column */
    tc_write_field(c, TType_STRUCT, 3);
    tc_write_fstr(c, 1, R2UTF8(col));
    if (val != R_NilValue) tc_write_fstr(c, 2, R2UTF8(val));
    tc_write_field(c, TType_I64, 3); tc_write_i64(c, now());
    tc_write_stop(c); /* Col */
    /* consistency level */
    tc_write_field(c, TType_I32, 3);
    tc_write_i32(c, c->cl);
    tc_write_stop(c);
    tc_flush(c);

    if (tc_read_msg(c, &m)) {
	RC_void_ex(c, m.rest);
	return sc;
    }
    Rf_error("error obtaining result");
    return R_NilValue;
}

/* mutation = list(c.family=list(row.key=list(col=value, ...))) */
SEXP RC_mutate(SEXP sc, SEXP mut) {
    msg_t m;
    tconn_t *c;
    int cfs, i, j;
    SEXP cfn;
    int64_t now_ts;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    cfn = getAttrib(mut, R_NamesSymbol);
    if (TYPEOF(mut) != VECSXP || TYPEOF(cfn) != STRSXP) Rf_error("Invalid mutation");
    c = (tconn_t*) EXTPTR_PTR(sc);
    now_ts = now();

    tc_write_msg(c, "batch_mutate", TMessageType_CALL, c->seq++);
    tc_write_field(c, TType_MAP, 1); /* mutation */
    tc_write_u8(c, TType_STRING);
    tc_write_u8(c, TType_MAP);
    tc_write_i32(c, cfs = LENGTH(mut));
    for (i = 0; i < cfs; i++) {
	SEXP rk = VECTOR_ELT(mut, i);
	SEXP rn = getAttrib(rk, R_NamesSymbol);
	int nr = LENGTH(rk);
	if (TYPEOF(rk) != VECSXP || TYPEOF(rn) != STRSXP) {
	    closesocket(c->s);
	    c->s = -1;
	    Rf_error("invalid key list in the mutation, aborting connection");
	}
	tc_write_str(c, translateCharUTF8(STRING_ELT(cfn, i)));
	tc_write_u8(c, TType_STRING);
	tc_write_u8(c, TType_LIST);
	tc_write_i32(c, nr);
	for (j = 0; j < nr; j++) {
	    SEXP cl = VECTOR_ELT(rk, j), cn = getAttrib(cl, R_NamesSymbol);
	    int nc = LENGTH(cl);
	    tc_write_str(c, translateCharUTF8(STRING_ELT(rn, j)));
	    tc_write_u8(c, TType_STRUCT); /* Mutation */
	    if (TYPEOF(cl) == STRSXP || (TYPEOF(cl) == VECSXP && TYPEOF(cn) == STRSXP)) {
		int k;
		tc_write_i32(c, nc);
		for (k = 0; k < nc; k++) {
		    tc_write_field(c, TType_STRUCT, 1); /* CoSC */
		    tc_write_field(c, TType_STRUCT, 1); /* Colunn */
		    if (TYPEOF(cl) == STRSXP) {
			if (TYPEOF(cn) != STRSXP)
			    tc_write_fstr(c, 1, translateCharUTF8(STRING_ELT(cl, k)));
			else {
			    tc_write_fstr(c, 1, translateCharUTF8(STRING_ELT(cn, k)));
			    tc_write_fstr(c, 2, translateCharUTF8(STRING_ELT(cl, k)));
			}
		    } else {
			SEXP v = VECTOR_ELT(cl, k);
			if (TYPEOF(v) != STRSXP) {
			    v = eval(PROTECT(lang2(install("as.character"), v)), R_GlobalEnv);
			    UNPROTECT(1);
			}
			tc_write_fstr(c, 1, translateCharUTF8(STRING_ELT(cn, k)));
			tc_write_fstr(c, 2, R2UTF8(v));
		    }
		    tc_write_field(c, TType_I64, 3); tc_write_i64(c, now_ts);
		    tc_write_stop(c); /* Col */
		    tc_write_stop(c); /* CoSC */
		    tc_write_stop(c); /* Mut */
		}
	    } else {
		closesocket(c->s);
		c->s = -1;
		Rf_error("invalid columt list in the mutation, aborting connection");
	    }
	}
    }
    tc_write_stop(c);
    tc_flush(c);

    if (tc_read_msg(c, &m)) {
	RC_void_ex(c, m.rest);
	return sc;
    }
    Rf_error("error obtaining result");
    return R_NilValue;
}

SEXP RC_write_table(SEXP sc, SEXP cf, SEXP df, SEXP rn, SEXP cn) {
    msg_t m;
    tconn_t *c;
    int64_t now_ts;
    int i, j, nc, nr, conv = 0;
    const char *cfn;

    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(cf) != STRSXP || LENGTH(cf) != 1) Rf_error("column family must be a string");
    if (TYPEOF(df) != VECSXP) Rf_error("The object to write must be a data.frame");
    if (TYPEOF(rn) != STRSXP || TYPEOF(cn) != STRSXP) Rf_error("Both row names and column names must exist and be character vectors");
    cfn = R2UTF8(cf);
    nc = LENGTH(df);
    if (nc == 0 || (nr = LENGTH(VECTOR_ELT(df, 0))) == 0)
	Rf_error("empty data.frame, nothing to do");
    /* check whether we need to convert any column to character */
    for (i = 0; i < nc; i++) if (TYPEOF(VECTOR_ELT(df, i)) != STRSXP) { conv = 1; break; }
    if (conv) { /* call as.character on all non-character columns */
	SEXP ndf = PROTECT(allocVector(VECSXP, nc));
	SEXP ac = install("as.character");
	for (i = 0; i < nc; i++) {
	    if (TYPEOF(VECTOR_ELT(df, i)) != STRSXP) {
		SET_VECTOR_ELT(ndf, i, eval(PROTECT(lang2(ac, VECTOR_ELT(df, i))), R_GlobalEnv));
		UNPROTECT(1);
	    } else
		SET_VECTOR_ELT(ndf, i, VECTOR_ELT(df, i));
	}
	df = ndf;
    }
    
    c = (tconn_t*) EXTPTR_PTR(sc);
    now_ts = now();

    tc_write_msg(c, "batch_mutate", TMessageType_CALL, c->seq++);
    tc_write_field(c, TType_MAP, 1); /* mutation */
    tc_write_u8(c, TType_STRING);
    tc_write_u8(c, TType_MAP);
    tc_write_i32(c, nr);
    for (i = 0; i < nr; i++) {
	tc_write_str(c, translateCharUTF8(STRING_ELT(rn, i)));
	tc_write_u8(c, TType_STRING);
	tc_write_u8(c, TType_LIST);
	tc_write_i32(c, 1); /* map of 1 entry - cf */
	tc_write_str(c, cfn);
	/* list of mutations */
	tc_write_u8(c, TType_STRUCT); /* list<Mutation> */
	tc_write_i32(c, nc);
	for (j = 0; j < nc; j++) {
	    tc_write_field(c, TType_STRUCT, 1); /* CoSC */
	    tc_write_field(c, TType_STRUCT, 1); /* Colunn */
	    tc_write_fstr(c, 1, translateCharUTF8(STRING_ELT(cn, j)));
	    tc_write_fstr(c, 2, translateCharUTF8(STRING_ELT(VECTOR_ELT(df, j), i)));
	    tc_write_field(c, TType_I64, 3); tc_write_i64(c, now_ts);
	    tc_write_stop(c); /* Col */
	    tc_write_stop(c); /* CoSC */
	    tc_write_stop(c); /* Mut */
	}
    }
    tc_write_stop(c);
    tc_flush(c);

    if (tc_read_msg(c, &m)) {
	RC_void_ex(c, m.rest);
	return sc;
    }
    Rf_error("error obtaining result");
    return R_NilValue;
}

SEXP RC_describe_ring(SEXP sc, SEXP ks) {
    msg_t m;
    tconn_t *c;

    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(ks) != STRSXP || LENGTH(ks) != 1) Rf_error("keyspace family must be a character vector of length one");
    c = (tconn_t*) EXTPTR_PTR(sc);
    tc_write_msg(c, "describe_ring", TMessageType_CALL, c->seq++);
    tc_write_fstr(c, 1, R2UTF8(ks));
    tc_write_stop(c);
    tc_flush(c);
    if (tc_read_msg(c, &m) && m.rest) {
	SEXP res = tc_read_value(c, m.rest);
	tc_skip_fields(c);
	return res;
    }
    return R_NilValue;    
}

SEXP RC_call_void(SEXP sc, SEXP method) {
    msg_t m;
    tconn_t *c;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    if (TYPEOF(method) != STRSXP || LENGTH(method) != 1) Rf_error("method must be a character vector of length one");
    c = (tconn_t*) EXTPTR_PTR(sc);
    tc_write_msg(c, R2UTF8(method), TMessageType_CALL, c->seq++);
    tc_write_stop(c);
    tc_flush(c);
    if (tc_read_msg(c, &m) && m.rest) {
	SEXP res = tc_read_value(c, m.rest);
	tc_skip_fields(c);
	return res;
    }
    return R_NilValue;
}

SEXP RC_set_cl(SEXP sc, SEXP cl) {
    tconn_t *c;
    if (!inherits(sc, "CassandraConnection")) Rf_error("invalid connection");
    c = (tconn_t*) EXTPTR_PTR(sc);
    c->cl = (ConsistencyLevel) asInteger(cl);
    return sc;
}


/* short-circuit dispatch on [[ for data frames */
SEXP R_get_col(SEXP df, SEXP i) {
    int ii = asInteger(i);
    if (df == R_NilValue) return df;
    if (TYPEOF(df) != VECSXP) Rf_error("`x' is not a list");
    if (ii < 1 || ii > LENGTH(df)) Rf_error("index i = %d is out of bounds (1,...,%d)", ii, LENGTH(df));
    return VECTOR_ELT(df, ii);
}

