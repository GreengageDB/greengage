%{
/*-------------------------------------------------------------------------
 *
 * specscanner.l
 *	  a lexical scanner for an isolation test specification
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

static int	yyline = 1;			/* line number for error reporting */

#define LITBUF_INIT	1024		/* initial size of litbuf */
static char *litbuf = NULL;
static size_t litbufsize = 0;
static size_t litbufpos = 0;

static void addlitchar(char c);

/* LCOV_EXCL_START */

%}

%option 8bit
%option never-interactive
%option nodefault
%option noinput
%option nounput
%option noyywrap
%option warn
%option prefix="spec_yy"


%x sql
%x qstr

digit			[0123456789]
ident_start		[A-Za-z\200-\377_]
ident_cont		[A-Za-z\200-\377_0-9\$]

identifier		{ident_start}{ident_cont}*

self			[,()*]

non_newline		[^\n\r]
space			[ \t\r\f]

comment			("#"{non_newline}*)

%%

%{
	litbuf = pg_malloc(LITBUF_INIT);
	litbufsize = LITBUF_INIT;
%}

notices			{ return NOTICES; }
permutation		{ return PERMUTATION; }
session			{ return SESSION; }
setup			{ return SETUP; }
step			{ return STEP; }
teardown		{ return TEARDOWN; }

{digit}+		{
					yylval.integer = atoi(yytext);
					return INTEGER;
				}

{self}			{ return yytext[0]; }

[\n]			{ yyline++; }
{comment}		{ /* ignore */ }
{space}			{ /* ignore */ }

 /*
  * Plain identifiers: upstream (PG14+) spec files use unquoted session and
  * step names.  The grammar keeps a single name token, so these are
  * returned as string_literal just like quoted names.  Keywords above take
  * precedence over this rule.
  */
{identifier}	{
					yylval.str = pg_strdup(yytext);
					return(string_literal);
				}

 /* Quoted strings: "foo" */
\"				{
					litbufpos = 0;
					BEGIN(qstr);
				}
<qstr>\"		{
					litbuf[litbufpos] = '\0';
					yylval.str = pg_strdup(litbuf);
					BEGIN(INITIAL);
					return(string_literal);
				}
<qstr>.			{ addlitchar(yytext[0]); }
<qstr>\n		{ yyerror("unexpected newline in quoted string"); }
<qstr><<EOF>>	{ yyerror("unterminated quoted string"); }

 /* SQL blocks: { UPDATE ... } */
"{"{space}*		{

					litbufpos = 0;
					BEGIN(sql);
				}
<sql>{space}*"}" {
					litbuf[litbufpos] = '\0';
					yylval.str = pg_strdup(litbuf);
					BEGIN(INITIAL);
					return(sqlblock);
				}
<sql>.			{
					addlitchar(yytext[0]);
				}
<sql>\n			{
					yyline++;
					addlitchar(yytext[0]);
				}
<sql><<EOF>>	{
					yyerror("unterminated sql block");
				}

.				{
					fprintf(stderr, "syntax error at line %d: unexpected character \"%s\"\n", yyline, yytext);
					exit(1);
				}
%%

/* LCOV_EXCL_STOP */

static void
addlitchar(char c)
{
	/* We must always leave room to add a trailing \0 */
	if (litbufpos >= litbufsize - 1)
	{
		/* Double the size of litbuf if it gets full */
		litbufsize += litbufsize;
		litbuf = pg_realloc(litbuf, litbufsize);
	}
	litbuf[litbufpos++] = c;
}

void
yyerror(const char *message)
{
	fprintf(stderr, "%s at line %d\n", message, yyline);
	exit(1);
}
