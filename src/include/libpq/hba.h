/*-------------------------------------------------------------------------
 *
 * hba.h
 *	  Interface to hba.c
 *
 *
 * src/include/libpq/hba.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HBA_H
#define HBA_H

#include "fmgr.h"
#include "libpq/pqcomm.h"	/* pgrminclude ignore */	/* needed for NetBSD */
#include "nodes/pg_list.h"
#include "regex/regex.h"


/*
 * The following enum represents the authentication methods that
 * are supported by PostgreSQL.
 *
 * Note: keep this in sync with the UserAuthName array in hba.c.
 */
typedef enum UserAuth
{
	uaReject,
	uaImplicitReject,			/* Not a user-visible option */
	uaTrust,
	uaIdent,
	uaPassword,
	uaMD5,
	uaSCRAM,
	uaGSS,
	uaSSPI,
	uaPAM,
	uaLDAP,
	uaCert,
	uaRADIUS,
	uaPeer
#define USER_AUTH_LAST uaPeer	/* Must be last value of this enum */
} UserAuth;

typedef enum IPCompareMethod
{
	ipCmpMask,
	ipCmpSameHost,
	ipCmpSameNet,
	ipCmpAll
} IPCompareMethod;

typedef enum ConnType
{
	ctLocal,
	ctHost,
	ctHostSSL,
	ctHostNoSSL
} ConnType;

typedef struct HbaLine
{
	int			linenumber;
	char	   *rawline;
	ConnType	conntype;
	List	   *databases;
	List	   *roles;
	struct sockaddr_storage addr;
	struct sockaddr_storage mask;
	IPCompareMethod ip_cmp_method;
	char	   *hostname;
	UserAuth	auth_method;

	char	   *usermap;
	char	   *pamservice;
	bool		ldaptls;
	char	   *ldapserver;
	int			ldapport;
	char	   *ldapbinddn;
	char	   *ldapbindpasswd;
	char	   *ldapsearchattribute;
	char       *ldapsearchfilter;
	char	   *ldapbasedn;
	int			ldapscope;
	char	   *ldapprefix;
	char	   *ldapsuffix;
	bool		clientcert;
	char	   *krb_realm;
	bool		include_realm;
	char	   *radiusserver;
	char	   *radiussecret;
	char	   *radiusidentifier;
	int			radiusport;
} HbaLine;

typedef struct IdentLine
{
	int			linenumber;

	char	   *usermap;
	char	   *ident_user;
	char	   *pg_role;
	regex_t		re;
} IdentLine;

/* kluge to avoid including libpq/libpq-be.h here */
typedef struct Port hbaPort;

extern bool load_hba(void);
extern bool load_ident(void);
extern void hba_getauthmethod(hbaPort *port);
extern int check_usermap(const char *usermap_name,
			  const char *pg_role, const char *auth_user,
			  bool case_sensitive);
extern bool check_same_host_or_net(SockAddr *raddr, IPCompareMethod method);
extern bool pg_isblank(const char c);
extern Datum pg_hba_file_rules(PG_FUNCTION_ARGS);

#endif   /* HBA_H */
