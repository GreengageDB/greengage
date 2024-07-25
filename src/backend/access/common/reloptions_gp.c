/*-------------------------------------------------------------------------
 *
 * reloptions_gp.c
 *	  GPDB-specific relation options.
 *
 * These are in a separate file from reloptions.c, in order to reduce
 * conflicts when merging with upstream code.
 *
 *
 * Portions Copyright (c) 2017-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/reloptions_gp.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/bitmap.h"
#include "access/reloptions.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_type.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/gp_compress.h"

/*
 * Helper macro used for validation
 */
#define KIND_IS_APPENDOPTIMIZED(kind) (((kind) & RELOPT_KIND_APPENDOPTIMIZED) != 0)

/*
 * GPDB reloptions specification.
 */

static relopt_bool boolRelOpts_gp[] =
{
	{
		{
			SOPT_CHECKSUM,
			"Append table checksum",
			RELOPT_KIND_APPENDOPTIMIZED,
			AccessExclusiveLock
		},
		AO_DEFAULT_CHECKSUM
	},
	{
		{
			SOPT_ANALYZEHLL,
			"Enable HLL stats collection during analyze",
			RELOPT_KIND_HEAP | RELOPT_KIND_TOAST | RELOPT_KIND_APPENDOPTIMIZED,
			ShareUpdateExclusiveLock
		},
		ANALYZE_DEFAULT_HLL
	},
	/* list terminator */
	{{NULL}}
};

static relopt_int intRelOpts_gp[] =
{
	{
		{
			SOPT_FILLFACTOR,
			"Packs bitmap index pages only to this percentage",
			RELOPT_KIND_BITMAP,
			ShareUpdateExclusiveLock	/* since it applies only to later
										 * inserts */
		},
		BITMAP_DEFAULT_FILLFACTOR, BITMAP_MIN_FILLFACTOR, 100
	},
	{
		{
			SOPT_BLOCKSIZE,
			"AO tables block size in bytes",
			RELOPT_KIND_APPENDOPTIMIZED,
			AccessExclusiveLock
		},
		AO_DEFAULT_BLOCKSIZE, MIN_APPENDONLY_BLOCK_SIZE, MAX_APPENDONLY_BLOCK_SIZE
	},
	{
		{
			SOPT_COMPLEVEL,
			"AO table compression level",
			RELOPT_KIND_APPENDOPTIMIZED,
			ShareUpdateExclusiveLock	/* since it applies only to later
										 * inserts */
		},
		AO_DEFAULT_COMPRESSLEVEL, AO_MIN_COMPRESSLEVEL, AO_MAX_COMPRESSLEVEL
	},
	/* list terminator */
	{{NULL}}
};

static relopt_real realRelOpts_gp[] =
{
	/* list terminator */
	{{NULL}}
};

static relopt_string stringRelOpts_gp[] =
{
	{
		{
			SOPT_COMPTYPE,
			"AO tables compression type",
			RELOPT_KIND_APPENDOPTIMIZED,
			AccessExclusiveLock
		},
		0, true, NULL, ""
	},
	/* list terminator */
	{{NULL}}
};

static relopt_value *get_option_set(relopt_value *options, int num_options, const char *opt_name);
static bool reloption_is_default(const char *optstr, int optlen);

/*
 * initialize_reloptions_gp
 * 		initialization routine for GPDB reloptions
 *
 * We use the add_*_option interface in reloptions.h to add GPDB-specific options.
 */
void
initialize_reloptions_gp(void)
{
	int			i;
	static bool	initialized = false;

	/* only add these on first call. */
	if (initialized)
		return;
	initialized = true;

	/* Set GPDB specific options */
	for (i = 0; boolRelOpts_gp[i].gen.name; i++)
	{
		add_bool_reloption(boolRelOpts_gp[i].gen.kinds,
						   (char *) boolRelOpts_gp[i].gen.name,
						   (char *) boolRelOpts_gp[i].gen.desc,
						   boolRelOpts_gp[i].default_val,
						   boolRelOpts_gp[i].gen.lockmode);
	}

	for (i = 0; intRelOpts_gp[i].gen.name; i++)
	{
		add_int_reloption(intRelOpts_gp[i].gen.kinds,
						  (char *) intRelOpts_gp[i].gen.name,
						  (char *) intRelOpts_gp[i].gen.desc,
						  intRelOpts_gp[i].default_val,
						  intRelOpts_gp[i].min,
						  intRelOpts_gp[i].max,
						  intRelOpts_gp[i].gen.lockmode);
	}

	for (i = 0; realRelOpts_gp[i].gen.name; i++)
	{
		add_real_reloption(realRelOpts_gp[i].gen.kinds,
						   (char *) realRelOpts_gp[i].gen.name,
						   (char *) realRelOpts_gp[i].gen.desc,
						   realRelOpts_gp[i].default_val,
						   realRelOpts_gp[i].min, realRelOpts_gp[i].max,
						   realRelOpts_gp[i].gen.lockmode);
	}

	for (i = 0; stringRelOpts_gp[i].gen.name; i++)
	{
		add_string_reloption(stringRelOpts_gp[i].gen.kinds,
							 (char *) stringRelOpts_gp[i].gen.name,
							 (char *) stringRelOpts_gp[i].gen.desc,
							 NULL,
							 stringRelOpts_gp[i].validate_cb,
							 stringRelOpts_gp[i].gen.lockmode);
	}
}

/*
 * This is set whenever the GUC gp_default_storage_options is set.
 */
static StdRdOptions *ao_storage_opts = NULL;

/*
 * Accumulate a new datum for one AO storage option.
 */
static void
accumAOStorageOpt(char *name, char *value,
				  ArrayBuildState *astate)
{
	text	   *t;
	bool		boolval;
	int			intval;
	StringInfoData buf;

	Assert(astate);

	initStringInfo(&buf);

	if (pg_strcasecmp(SOPT_BLOCKSIZE, name) == 0)
	{
		if (!parse_int(value, &intval, 0 /* unit flags */ ,
					   NULL /* hint message */ ))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid integer value \"%s\" for storage option \"%s\"",
							value, name)));
		appendStringInfo(&buf, "%s=%d", SOPT_BLOCKSIZE, intval);
	}
	else if (pg_strcasecmp(SOPT_COMPTYPE, name) == 0)
	{
		appendStringInfo(&buf, "%s=%s", SOPT_COMPTYPE, value);
	}
	else if (pg_strcasecmp(SOPT_COMPLEVEL, name) == 0)
	{
		if (!parse_int(value, &intval, 0 /* unit flags */ ,
					   NULL /* hint message */ ))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid integer value \"%s\" for storage option \"%s\"",
							value, name)));
		appendStringInfo(&buf, "%s=%d", SOPT_COMPLEVEL, intval);
	}
	else if (pg_strcasecmp(SOPT_CHECKSUM, name) == 0)
	{
		if (!parse_bool(value, &boolval))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid bool value \"%s\" for storage option \"%s\"",
							value, name)));
		appendStringInfo(&buf, "%s=%s", SOPT_CHECKSUM, boolval ? "true" : "false");
	}
	else
	{
		/*
		 * Provide a user friendly message in case that the options are
		 * appendonly and its variants
		 */
		if (!pg_strcasecmp(name, "appendonly") ||
			!pg_strcasecmp(name, "appendoptimized") ||
			!pg_strcasecmp(name, "orientation"))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid storage option \"%s\"", name),
					 errhint("For table access methods use \"default_table_access_method\" instead.")));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid storage option \"%s\"", name)));
	}

	t = cstring_to_text(buf.data);

	accumArrayResult(astate, PointerGetDatum(t), /* disnull */ false,
					 TEXTOID, CurrentMemoryContext);
	pfree(t);
	pfree(buf.data);
}

/*
 * Reset appendonly storage options to factory defaults.  Callers must
 * free ao_opts->compresstype before calling this method.
 */
inline void
resetAOStorageOpts(StdRdOptions *ao_opts)
{
	ao_opts->blocksize = AO_DEFAULT_BLOCKSIZE;
	ao_opts->checksum = AO_DEFAULT_CHECKSUM;
	ao_opts->compresslevel = AO_DEFAULT_COMPRESSLEVEL;
	ao_opts->compresstype[0] = '\0';
}

/*
 * This needs to happen whenever gp_default_storage_options GUC is
 * reset.
 */
void
resetDefaultAOStorageOpts(void)
{
	if (ao_storage_opts)
		resetAOStorageOpts(ao_storage_opts);
}

const StdRdOptions *
currentAOStorageOptions(void)
{
	return (const StdRdOptions *) ao_storage_opts;
}

/*
 * Set global appendonly storage options.
 */
void
setDefaultAOStorageOpts(StdRdOptions *copy)
{
	Assert(copy);

	/* If not allocated yet, do it now */
	if (!ao_storage_opts)
		ao_storage_opts = calloc(sizeof(*ao_storage_opts), 1);
	if (!ao_storage_opts)
		ereport(ERROR,
			(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	memcpy(ao_storage_opts, copy, sizeof(*ao_storage_opts));

	if (pg_strcasecmp(copy->compresstype, "none") == 0)
	{
		/* Represent compresstype=none as an empty string (MPP-25073). */
		ao_storage_opts->compresstype[0] = '\0';
	}
}

static int	setDefaultCompressionLevel(char *compresstype);

/*
 * Accept a string of the form "name=value,name=value,...".  Space
 * around ',' and '=' is allowed.  Parsed values are stored in
 * a text array and returned to caller.  The parser is a
 * finite state machine that changes states for each input character
 * scanned.
 */
Datum
parseAOStorageOpts(const char *opts_str)
{
	int			dims[1];
	int			lbs[1];
	Datum		result;
	ArrayBuildState *astate;

	const char *cp;
	const char *name_st = NULL;
	const char *value_st = NULL;
	char	   *name = NULL,
			   *value = NULL;

	enum state
	{
		/*
		 * Consume whitespace at the beginning of a name token.
		 */
		LEADING_NAME,

		/*
		 * Name token is being scanned.  Allowed characters are alphabets,
		 * whitespace and '='.
		 */
		NAME_TOKEN,

		/*
		 * Name token was terminated by whitespace.  This state scans the
		 * trailing whitespace after name token.
		 */
		TRAILING_NAME,

		/*
		 * Whitespace after '=' and before value token.
		 */
		LEADING_VALUE,

		/*
		 * Value token is being scanned.  Allowed characters are alphabets,
		 * digits, '_'.  Value should be delimited by a ',', whitespace or end
		 * of string '\0'.
		 */
		VALUE_TOKEN,

		/*
		 * Whitespace after value token.
		 */
		TRAILING_VALUE,

		/*
		 * End of string.  This state can only be entered from VALUE_TOKEN or
		 * TRAILING_VALUE.
		 */
		EOS
	};
	enum state	st = LEADING_NAME;

	/*
	 * Initialize ArrayBuildState ourselves rather than leaving it to
	 * accumArrayResult().  This aviods the catalog lookup (pg_type) performed
	 * by accumArrayResult().
	 */
	astate = (ArrayBuildState *) palloc(sizeof(ArrayBuildState));
	astate->mcontext = CurrentMemoryContext;
	astate->alen = 10;			/* Initial number of name=value pairs. */
	astate->dvalues = (Datum *) palloc(astate->alen * sizeof(Datum));
	astate->dnulls = (bool *) palloc(astate->alen * sizeof(bool));
	astate->nelems = 0;
	astate->element_type = TEXTOID;
	astate->typlen = -1;
	astate->typbyval = false;
	astate->typalign = 'i';

	cp = opts_str - 1;
	do
	{
		++cp;
		switch (st)
		{
			case LEADING_NAME:
				if (isalpha(*cp))
				{
					st = NAME_TOKEN;
					name_st = cp;
				}
				else if (!isspace(*cp))
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("invalid storage option name in \"%s\"",
									opts_str)));
				}
				break;
			case NAME_TOKEN:
				if (isspace(*cp))
					st = TRAILING_NAME;
				else if (*cp == '=')
					st = LEADING_VALUE;
				else if (!isalpha(*cp))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("invalid storage option name in \"%s\"",
									opts_str)));
				if (st != NAME_TOKEN)
				{
					name = palloc(cp - name_st + 1);
					strncpy(name, name_st, cp - name_st);
					name[cp - name_st] = '\0';
					for (name_st = name; *name_st != '\0'; ++name_st)
						*(char *) name_st = pg_tolower(*name_st);
				}
				break;
			case TRAILING_NAME:
				if (*cp == '=')
					st = LEADING_VALUE;
				else if (!isspace(*cp))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("invalid value for option \"%s\", expected \"=\"", name)));
				break;
			case LEADING_VALUE:
				if (isalnum(*cp))
				{
					st = VALUE_TOKEN;
					value_st = cp;
				}
				else if (!isspace(*cp))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("invalid value for option \"%s\"", name)));
				break;
			case VALUE_TOKEN:
				if (isspace(*cp))
					st = TRAILING_VALUE;
				else if (*cp == '\0')
					st = EOS;
				else if (*cp == ',')
					st = LEADING_NAME;
				/* Need to check '_' for rle_type */
				else if (!(isalnum(*cp) || *cp == '_'))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid value for option \"%s\"", name)));
				if (st != VALUE_TOKEN)
				{
					value = palloc(cp - value_st + 1);
					strncpy(value, value_st, cp - value_st);
					value[cp - value_st] = '\0';
					for (value_st = value; *value_st != '\0'; ++value_st)
						*(char *) value_st = pg_tolower(*value_st);
					Assert(name);
					accumAOStorageOpt(name, value, astate);
					pfree(name);
					name = NULL;
					pfree(value);
					value = NULL;
				}
				break;
			case TRAILING_VALUE:
				if (*cp == ',')
					st = LEADING_NAME;
				else if (*cp == '\0')
					st = EOS;
				else if (!isspace(*cp))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("syntax error after \"%s\"", value)));
				break;
			case EOS:

				/*
				 * We better get out of the loop right after entering this
				 * state.  Therefore, we should never get here.
				 */
				elog(ERROR, "invalid value \"%s\" for GUC", opts_str);
				break;
		};
	} while (*cp != '\0');
	if (st != EOS)
		elog(ERROR, "invalid value \"%s\" for GUC", opts_str);

	lbs[0] = 1;
	dims[0] = astate->nelems;
	result = makeMdArrayResult(astate, 1, dims, lbs, CurrentMemoryContext, false);
	pfree(astate->dvalues);
	pfree(astate->dnulls);
	pfree(astate);
	return result;
}

/*
 * Return a datum that is array of "name=value" strings for each
 * appendonly storage option in opts.  This datum is used to populate
 * pg_class.reloptions during relation creation.
 *
 * If hasStorage is true, record all attributes to pg_class.reloptions
 * even if not specified in withOpts since they are necessary to perform
 * table scans. In cases where hasStorage is false and the reloption has
 * been modified from server defaults, the parameter is recorded in
 * pg_class.reloptions and used for inheritence purposes only.
 */
Datum
transformAOStdRdOptions(StdRdOptions *opts, Datum withOpts, bool hasStorage)
{
	char	   *strval;
	Datum	   *withDatums = NULL;
	Datum		d;
	text	   *t;
	int			i,
				withLen,
				soptLen,
				nWithOpts = 0;
	ArrayType  *withArr;
	ArrayBuildState *astate = NULL;
	bool		foundBlksz = false,
				foundComptype = false,
				foundComplevel = false,
				foundChecksum = false,
				foundAnalyzeHLL = false;

	/*
	 * withOpts must be parsed to see if an option was spcified in WITH()
	 * clause.
	 */
	if (DatumGetPointer(withOpts) != NULL)
	{
		withArr = DatumGetArrayTypeP(withOpts);
		Assert(ARR_ELEMTYPE(withArr) == TEXTOID);
		deconstruct_array(withArr, TEXTOID, -1, false, 'i', &withDatums,
						  NULL, &nWithOpts);

		/*
		 * Include options specified in WITH() clause in the same order as
		 * they are specified.  Otherwise we will end up with regression
		 * failures due to diff with respect to answer file.
		 */
		for (i = 0; i < nWithOpts; ++i)
		{
			t = DatumGetTextP(withDatums[i]);
			strval = VARDATA(t);

			/*
			 * Text datums are usually not null terminated.  We must never
			 * access beyond their length.
			 */
			withLen = VARSIZE(t) - VARHDRSZ;

			/*
			 * withDatums[i] may not be used directly.  It may be e.g.
			 * "bLoCksiZe=3213".  Therefore we don't set it as reloptions as
			 * is.
			 */
			soptLen = strlen(SOPT_BLOCKSIZE);
			if (withLen > soptLen &&
				pg_strncasecmp(strval, SOPT_BLOCKSIZE, soptLen) == 0)
			{
				foundBlksz = true;
				d = CStringGetTextDatum(psprintf("%s=%d",
												 SOPT_BLOCKSIZE,
												 opts->blocksize));
				astate = accumArrayResult(astate, d, false, TEXTOID,
										  CurrentMemoryContext);
			}
			soptLen = strlen(SOPT_COMPTYPE);
			if (withLen > soptLen &&
				pg_strncasecmp(strval, SOPT_COMPTYPE, soptLen) == 0)
			{
				foundComptype = true;
				/*
				 * Record "none" as compresstype in reloptions if it was
				 * explicitly specified in WITH clause.
				 *
				 * If "quicklz" was explicitly specified in WITH clause and
				 * gp_quicklz_fallback=true, record "zstd" as compresstype
				 * if available, else record AO_DEFAULT_USABLE_COMPRESSTYPE
				 */
				char* compresstype;

				if (opts->compresstype[0])
				{
					if (gp_quicklz_fallback && pg_strcasecmp(opts->compresstype, "quicklz") == 0)
					{						
#ifdef USE_ZSTD
						compresstype = "zstd";
#else
						compresstype = AO_DEFAULT_USABLE_COMPRESSTYPE;
#endif
					}
					else
						compresstype = opts->compresstype;
				}
				else
					compresstype = "none";

				d = CStringGetTextDatum(psprintf("%s=%s",
												 SOPT_COMPTYPE,
												 compresstype));
				astate = accumArrayResult(astate, d, false, TEXTOID,
										  CurrentMemoryContext);
			}
			soptLen = strlen(SOPT_COMPLEVEL);
			if (withLen > soptLen &&
				pg_strncasecmp(strval, SOPT_COMPLEVEL, soptLen) == 0)
			{
				foundComplevel = true;
				d = CStringGetTextDatum(psprintf("%s=%d",
												 SOPT_COMPLEVEL,
												 opts->compresslevel));
				astate = accumArrayResult(astate, d, false, TEXTOID,
										  CurrentMemoryContext);
			}
			soptLen = strlen(SOPT_CHECKSUM);
			if (withLen > soptLen &&
				pg_strncasecmp(strval, SOPT_CHECKSUM, soptLen) == 0)
			{
				foundChecksum = true;
				d = CStringGetTextDatum(psprintf("%s=%s",
												 SOPT_CHECKSUM,
												 (opts->checksum ? "true" : "false")));
				astate = accumArrayResult(astate, d, false, TEXTOID,
										  CurrentMemoryContext);
			}
			soptLen = strlen(SOPT_ANALYZEHLL);
			if (withLen > soptLen &&
				pg_strncasecmp(strval, SOPT_ANALYZEHLL, soptLen) == 0)
			{
				foundAnalyzeHLL = true;
				d = CStringGetTextDatum(psprintf("%s=%s",
												 SOPT_ANALYZEHLL,
												 (opts->analyze_hll_non_part_table ? "true" : "false")));
				astate = accumArrayResult(astate, d, false, TEXTOID,
										  CurrentMemoryContext);
			}
		}
	}


	/* Record AO storage parameters {blocksize,checksum,compresslevel,compresstype}
	 * in pg_class.reloptions even if not provided in WITH clause IFF hasStorage=true
	 * OR the paremeter has been modified from the server default value.
	 */

	if (!foundBlksz && (opts->blocksize != AO_DEFAULT_BLOCKSIZE || hasStorage))
	{
		d = CStringGetTextDatum(psprintf("%s=%d",
										SOPT_BLOCKSIZE,
										opts->blocksize));
		astate = accumArrayResult(astate, d, false, TEXTOID,
									CurrentMemoryContext);
	}
	if (!foundComplevel && (opts->compresslevel != AO_DEFAULT_COMPRESSLEVEL || hasStorage))
	{
			d = CStringGetTextDatum(psprintf("%s=%d",
												SOPT_COMPLEVEL,
												(opts->compresslevel ? opts->compresslevel : AO_DEFAULT_COMPRESSLEVEL)));
			astate = accumArrayResult(astate, d, false, TEXTOID,
										CurrentMemoryContext);
	}
	if (!foundComptype && ((opts->compresstype[0] && pg_strcasecmp(opts->compresstype, "none") != 0) || hasStorage))
	{
		d = CStringGetTextDatum(psprintf("%s=%s",
							SOPT_COMPTYPE,
							(opts->compresstype[0] ? opts->compresstype : "none")));
		astate = accumArrayResult(astate, d, false, TEXTOID,
									CurrentMemoryContext);
	}
	if (!foundChecksum && (!opts->checksum || hasStorage))
	{
	d = CStringGetTextDatum(psprintf("%s=%s",
									SOPT_CHECKSUM,
									(opts->checksum ? "true" : "false")));
	astate = accumArrayResult(astate, d, false, TEXTOID,
								CurrentMemoryContext);
	}
	if ((opts->analyze_hll_non_part_table != ANALYZE_DEFAULT_HLL) && !foundAnalyzeHLL)
	{
		d = CStringGetTextDatum(psprintf("%s=%s",
										 SOPT_ANALYZEHLL,
										 (opts->analyze_hll_non_part_table ? "true" : "false")));
		astate = accumArrayResult(astate, d, false, TEXTOID,
								  CurrentMemoryContext);
	}
	return astate ?
		makeArrayResult(astate, CurrentMemoryContext) :
		PointerGetDatum(NULL);
}

/* 
 * Check if the given reloption string has default value.
 */
static bool
reloption_is_default(const char *optstr, int optlen)
{
	char 		*defaultopt = NULL;
	bool 		res;

	if (optlen > strlen(SOPT_BLOCKSIZE) &&
		pg_strncasecmp(optstr, SOPT_BLOCKSIZE, strlen(SOPT_BLOCKSIZE)) == 0)
	{
		defaultopt = psprintf("%s=%d",
										 SOPT_BLOCKSIZE,
										 AO_DEFAULT_BLOCKSIZE);
	}
	else if (optlen > strlen(SOPT_COMPTYPE) &&
		pg_strncasecmp(optstr, SOPT_COMPTYPE, strlen(SOPT_COMPTYPE)) == 0)
	{
		defaultopt = psprintf("%s=%s",
										 SOPT_COMPTYPE,
										 AO_DEFAULT_COMPRESSTYPE);
	}
	else if (optlen > strlen(SOPT_COMPLEVEL) &&
		pg_strncasecmp(optstr, SOPT_COMPLEVEL, strlen(SOPT_COMPLEVEL)) == 0)
	{
		defaultopt = psprintf("%s=%d",
										 SOPT_COMPLEVEL,
										 AO_DEFAULT_COMPRESSLEVEL);
	}
	else if (optlen > strlen(SOPT_CHECKSUM) &&
		pg_strncasecmp(optstr, SOPT_CHECKSUM, strlen(SOPT_CHECKSUM)) == 0)
	{
		defaultopt = psprintf("%s=%s",
										 SOPT_CHECKSUM,
										 AO_DEFAULT_CHECKSUM ? "true" : "false");
	}
	else if (optlen > strlen(SOPT_ANALYZEHLL) &&
		pg_strncasecmp(optstr, SOPT_ANALYZEHLL, strlen("analyze_hll_non_part_table")) == 0)
	{
		defaultopt = psprintf("%s=%s",
							  SOPT_ANALYZEHLL,
										 ANALYZE_DEFAULT_HLL ? "true" : "false");
	}
	if (defaultopt != NULL)
		res = strlen(defaultopt) == optlen && 
				pg_strncasecmp(optstr, defaultopt, optlen) == 0;
	else
		res = false;

	pfree(defaultopt);
	return res;
}

/* 
 * Check if two string arrays of reloptions are the same.
 *
 * Note that this will not handle the case where the option doesn't contain 
 * the '=' sign in it, e.g. "checksum" vs. "checksum=true". But it seems 
 * that at this point we should always have both options as "x=y" anyways.
 */
bool
relOptionsEquals(Datum oldOptions, Datum newOptions)
{
	ArrayType 	*oldoptarray, *newoptarray;
	Datum 		*opts1, *opts2;
	int		noldoptions = 0, nnewoptions = 0;
	int		i, j;

	/* Deconstruct both options. */
	if (PointerIsValid(DatumGetPointer(oldOptions)))
	{
		oldoptarray = DatumGetArrayTypeP(oldOptions);
		deconstruct_array(oldoptarray, TEXTOID, -1, false, 'i',
						  &opts1, NULL, &noldoptions);
	}
	if (PointerIsValid(DatumGetPointer(newOptions)))
	{
		newoptarray = DatumGetArrayTypeP(newOptions);
		deconstruct_array(newoptarray, TEXTOID, -1, false, 'i',
						  &opts2, NULL, &nnewoptions);
	}

	for (i = 0; i < nnewoptions; i++)
	{
		char 	*newopt_str = VARDATA(opts2[i]);
		int	newopt_len = VARSIZE(opts2[i]) - VARHDRSZ;
		int 	keylen;

		/* Should be "x=y" but better panic here rather than returning wrong result. */
		Assert(strchr(newopt_str, '=') != 0);

		keylen = strchr(newopt_str, '=') - newopt_str;

		/* Search for a match in old options. */
		for (j = 0; j < noldoptions; j++)
		{
			char 	*oldopt_str = VARDATA(opts1[j]);
			int	oldopt_len = VARSIZE(opts1[j]) - VARHDRSZ;

			/* Not the same option. */
			if (oldopt_len <= keylen || 
					pg_strncasecmp(oldopt_str, newopt_str, keylen) != 0)
				continue;

			/* Old option should be as "x=y" too. */
			Assert(oldopt_str[keylen] == '=');

			/* Key found, now they must match exactly otherwise it's a changed option. */
			if (oldopt_len != newopt_len ||
					pg_strncasecmp(oldopt_str, newopt_str, oldopt_len) != 0)
				return false;
			else
				break;
		}

		/* 
		 * If key not found, then it must've changed unless it's a default value 
		 * that doesn't appear in the old reloptions.
		 */
		if (j == noldoptions && !reloption_is_default(newopt_str, newopt_len))
			return false;
	}
	return true;
}

void
validate_and_adjust_options(StdRdOptions *result,
							relopt_value *options,
							int num_options, relopt_kind kind, bool validate)
{
	int			i;
	relopt_value *blocksize_opt;
	relopt_value *comptype_opt;
	relopt_value *complevel_opt;
	relopt_value *checksum_opt;

	/* 
	 * Firstly, for AO/CO tables, if anything is not set in the options but has 
	 * been specified by gp_default_storage_options before, use them. 
	 */
	if (ao_storage_opts &&
		KIND_IS_APPENDOPTIMIZED(kind))
	{
		if (!(get_option_set(options, num_options, SOPT_BLOCKSIZE)))
			result->blocksize = ao_storage_opts->blocksize;

		if (!(get_option_set(options, num_options, SOPT_COMPLEVEL)))
			result->compresslevel = ao_storage_opts->compresslevel;

		if (!(get_option_set(options, num_options, SOPT_COMPTYPE)))
			strlcpy(result->compresstype, ao_storage_opts->compresstype, sizeof(result->compresstype));

		if (!(get_option_set(options, num_options, SOPT_CHECKSUM)))
			result->checksum = ao_storage_opts->checksum;
	}

	/* blocksize */
	blocksize_opt = get_option_set(options, num_options, SOPT_BLOCKSIZE);
	if (blocksize_opt != NULL)
	{
		if (!KIND_IS_APPENDOPTIMIZED(kind) && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("usage of parameter \"blocksize\" in a non relation object is not supported")));

		result->blocksize = blocksize_opt->values.int_val;

		if (result->blocksize < MIN_APPENDONLY_BLOCK_SIZE ||
			result->blocksize > MAX_APPENDONLY_BLOCK_SIZE ||
			result->blocksize % MIN_APPENDONLY_BLOCK_SIZE != 0)
		{
			if (validate)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("block size must be between 8KB and 2MB and be a multiple of 8KB"),
						 errdetail("Got block size %d.", result->blocksize)));

			result->blocksize = DEFAULT_APPENDONLY_BLOCK_SIZE;
		}

	}

	/* compression type */
	comptype_opt = get_option_set(options, num_options, SOPT_COMPTYPE);
	if (comptype_opt != NULL)
	{
		if (!KIND_IS_APPENDOPTIMIZED(kind) && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("usage of parameter \"compresstype\" in a non relation object is not supported")));

		if (!compresstype_is_valid(comptype_opt->values.string_val))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("unknown compresstype \"%s\"",
							comptype_opt->values.string_val)));
		/*
		 * GPDB7 has dropped support for quicklz.
		 * If compresstype passed the above validity check, we want to fallback to using
		 * "zstd" as compresstype if available, else the default usable compresstype.
		 */
		if (pg_strcasecmp(comptype_opt->values.string_val, "quicklz") == 0)
		{
#ifdef USE_ZSTD
			StrNCpy(result->compresstype, "zstd", NAMEDATALEN);
#else
			StrNCpy(result->compresstype, AO_DEFAULT_USABLE_COMPRESSTYPE, NAMEDATALEN);
#endif
		}
		else
		{
			for (i = 0; i < strlen(comptype_opt->values.string_val); i++)
				result->compresstype[i] = pg_tolower(comptype_opt->values.string_val[i]);
			result->compresstype[i] = '\0';
		}
	}

	/* compression level */
	complevel_opt = get_option_set(options, num_options, SOPT_COMPLEVEL);
	if (complevel_opt != NULL)
	{
		if (!KIND_IS_APPENDOPTIMIZED(kind) && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("usage of parameter \"compresslevel\" in a non relation object is not supported")));

		result->compresslevel = complevel_opt->values.int_val;

		if (result->compresstype[0] &&
			pg_strcasecmp(result->compresstype, "none") != 0 &&
			result->compresslevel == 0 && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("compresstype \"%s\" can\'t be used with compresslevel 0",
							result->compresstype)));
		if (result->compresslevel < 0)
		{
			if (validate)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("compresslevel=%d is out of range (should be positive)",
								result->compresslevel)));

			result->compresslevel = setDefaultCompressionLevel(result->compresstype);
		}

		/* Check upper bound of compresslevel for each compression type */

		if (result->compresstype[0] &&
			(pg_strcasecmp(result->compresstype, "zlib") == 0))
		{
#ifndef HAVE_LIBZ
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("zlib compression is not supported by this build"),
					 errhint("Compile without --without-zlib to use zlib compression.")));
#endif
			if (result->compresslevel > 9)
			{
				if (validate)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("compresslevel=%d is out of range for zlib (should be in the range 1 to 9)",
									result->compresslevel)));

				result->compresslevel = setDefaultCompressionLevel(result->compresstype);
			}
		}

		if (result->compresstype[0] &&
			(pg_strcasecmp(result->compresstype, "zstd") == 0))
		{
#ifndef USE_ZSTD
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Zstandard library is not supported by this build"),
					 errhint("Compile with --with-zstd to use Zstandard compression.")));
#endif
			if (result->compresslevel > 19)
			{
				if (validate)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("compresslevel=%d is out of range for zstd (should be in the range 1 to 19)",
									result->compresslevel)));

				result->compresslevel = setDefaultCompressionLevel(result->compresstype);
			}
		}

		if (result->compresstype[0] &&
			(pg_strcasecmp(result->compresstype, "rle_type") == 0) &&
			(result->compresslevel > RLE_MAX_LEVEL))
		{
			if (validate)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("compresslevel=%d is out of range for rle_type (should be in the range 1 to 6)",
								result->compresslevel)));

			result->compresslevel = setDefaultCompressionLevel(result->compresstype);
		}
	}

	/* checksum */
	checksum_opt = get_option_set(options, num_options, SOPT_CHECKSUM);
	if (checksum_opt != NULL)
	{
		if (!KIND_IS_APPENDOPTIMIZED(kind) && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("usage of parameter \"checksum\" in a non relation object is not supported")));

		result->checksum = checksum_opt->values.bool_val;
	}

	/* More adjustment for compression settings: */
	/*
	 * use the default compressor if compresslevel was indicated but not
	 * compresstype. must make a copy otherwise str_tolower below will
	 * crash.
	 */
	if (result->compresslevel > 0 && !result->compresstype[0])
		strlcpy(result->compresstype, AO_DEFAULT_USABLE_COMPRESSTYPE, sizeof(result->compresstype));
	/*
	 * use compresslevel=1 if the compresstype is not none
	 */
	if (result->compresstype[0] && result->compresslevel == 0)
	{
		result->compresslevel = setDefaultCompressionLevel(result->compresstype);
	}
}

/*
 * validateOrientationRelOptions
 *
 *		Checks validity of orientation-specific reloption rules, currently only one. 
 * 		Other appendonly-specific rules should've been done in default_reloptions().
 */
void
validateOrientationRelOptions(char *comptype,
							 bool co)
{
	if (!co &&
		pg_strcasecmp(comptype, "rle_type") == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s cannot be used with Append Only relations row orientation",
						comptype)));
	}
}

/*
 * if no compressor type was specified, we set to no compression (level 0)
 * otherwise default for both zlib, quicklz, zstd and RLE to level 1.
 */
static int
setDefaultCompressionLevel(char *compresstype)
{
	if (!compresstype || pg_strcasecmp(compresstype, "none") == 0)
		return AO_DEFAULT_COMPRESSLEVEL;
	else
		return AO_DEFAULT_USABLE_COMPRESSLEVEL;
}

/*
 * It's used to prevent persistent memory leaks when parseRelOptions() is called repeatedly.
 */
void
free_options_deep(relopt_value *options, int num_options)
{
	int			i;

	for (i = 0; i < num_options; ++i)
	{
		if (options[i].isset &&
			options[i].gen->type == RELOPT_TYPE_STRING &&
			options[i].values.string_val != NULL)
		{
			pfree(options[i].values.string_val);
		}
	}
	pfree(options);
}

relopt_value *
get_option_set(relopt_value *options, int num_options, const char *opt_name)
{
	int			i;
	int			opt_name_len;
	int			cmp_len;

	opt_name_len = strlen(opt_name);
	for (i = 0; i < num_options; ++i)
	{
		cmp_len = options[i].gen->namelen > opt_name_len ? opt_name_len : options[i].gen->namelen;
		if (options[i].isset && pg_strncasecmp(options[i].gen->name, opt_name, cmp_len) == 0)
			return &options[i];
	}
	return NULL;
}

/* ------------------------------------------------------------------------
 * Attribute Encoding specific functions
 * ------------------------------------------------------------------------
 */

/*
 * Check if the name is one of the ENCODING clauses.
 */
bool
is_storage_encoding_directive(char *name)
{
	/* names we expect to see in ENCODING clauses */
	static char *storage_directive_names[] = {"compresstype", 
						"compresslevel",
						"blocksize"};

	int i = 0;

	for (i = 0; i < lengthof(storage_directive_names); i++)
	{
		if (strcmp(name, storage_directive_names[i]) == 0)
			return true;
	}
	return false;
}

/*
 * Add any missing encoding attributes (compresstype = none,
 * blocksize=...).  The column specific encoding attributes supported
 * today are compresstype, compresslevel and blocksize.  Refer to
 * pg_compression.c for more info.
 *
 */
static List *
fillin_encoding(List *aocoColumnEncoding)
{
	bool foundCompressType = false;
	bool foundCompressTypeNone = false;
	char *cmplevel = NULL;
	bool foundBlockSize = false;
	char *arg;
	List *retList = list_copy(aocoColumnEncoding);
	ListCell *lc;
	DefElem *el;
	const StdRdOptions *ao_opts = currentAOStorageOptions();

	foreach(lc, aocoColumnEncoding)
	{
		el = lfirst(lc);

		if (pg_strcasecmp("compresstype", el->defname) == 0)
		{
			foundCompressType = true;
			arg = defGetString(el);
			if (pg_strcasecmp("none", arg) == 0)
				foundCompressTypeNone = true;
		}
		else if (pg_strcasecmp("compresslevel", el->defname) == 0)
		{
			cmplevel = defGetString(el);
		}
		else if (pg_strcasecmp("blocksize", el->defname) == 0)
			foundBlockSize = true;
	}

	if (foundCompressType == false && cmplevel == NULL)
	{
		/* No compression option specified, use current defaults. */
		arg = ao_opts->compresstype[0] ?
				pstrdup(ao_opts->compresstype) : "none";
		el = makeDefElem("compresstype", (Node *) makeString(arg), -1);
		retList = lappend(retList, el);
		el = makeDefElem("compresslevel",
						 (Node *) makeInteger(ao_opts->compresslevel),
						 -1);
		retList = lappend(retList, el);
	}
	else if (foundCompressType == false && cmplevel)
	{
		if (strcmp(cmplevel, "0") == 0)
		{
			/*
			 * User wants to disable compression by specifying
			 * compresslevel=0.
			 */
			el = makeDefElem("compresstype", (Node *) makeString("none"), -1);
			retList = lappend(retList, el);
		}
		else
		{
			/*
			 * User wants to enable compression by specifying non-zero
			 * compresslevel.  Therefore, choose default compresstype
			 * if configured, otherwise use zlib.
			 */
			if (ao_opts->compresstype[0] &&
				strcmp(ao_opts->compresstype, "none") != 0)
			{
				arg = pstrdup(ao_opts->compresstype);
			}
			else
			{
				arg = AO_DEFAULT_USABLE_COMPRESSTYPE;
			}
			el = makeDefElem("compresstype", (Node *) makeString(arg), -1);
			retList = lappend(retList, el);
		}
	}
	else if (foundCompressType && cmplevel == NULL)
	{
		if (foundCompressTypeNone)
		{
			/*
			 * User wants to disable compression by specifying
			 * compresstype=none.
			 */
			el = makeDefElem("compresslevel", (Node *) makeInteger(0), -1);
			retList = lappend(retList, el);
		}
		else
		{
			/*
			 * Valid compresstype specified.  Use default
			 * compresslevel if it's non-zero, otherwise use 1.
			 */
			el = makeDefElem("compresslevel",
							 (Node *) makeInteger(ao_opts->compresslevel > 0 ?
												  ao_opts->compresslevel : 1),
							 -1);
			retList = lappend(retList, el);
		}
	}
	if (foundBlockSize == false)
	{
		el = makeDefElem("blocksize", (Node *) makeInteger(ao_opts->blocksize), -1);
		retList = lappend(retList, el);
	}
	return retList;
}

/*
 * Make encoding (compresstype = ..., blocksize=...) based on
 * currently configured defaults.
 * For blocksize, it is impossible for the value to be unset
 * if an appendonly relation, hence the default is always ignored.
 */
static List *
default_column_encoding_clause(Relation rel)
{
	DefElem *e1, *e2, *e3;
	const StdRdOptions *ao_opts = currentAOStorageOptions();
	bool		appendonly;
	int32		blocksize = -1;
	int16		compresslevel = 0;
	char	   *compresstype = NULL;
	NameData	compresstype_nd;

	appendonly = rel && RelationIsAppendOptimized(rel);
	if (appendonly)
	{
		GetAppendOnlyEntryAttributes(RelationGetRelid(rel),
									 &blocksize,
									 &compresslevel,
									 NULL,
									 &compresstype_nd);
		compresstype = NameStr(compresstype_nd);
	}

	compresstype = compresstype && compresstype[0] ? pstrdup(compresstype) : 
					(ao_opts->compresstype[0] ? pstrdup(ao_opts->compresstype) : "none");
	e1 = makeDefElem("compresstype", (Node *) makeString(pstrdup(compresstype)), -1);

	blocksize = appendonly ? blocksize : 
					(ao_opts->blocksize != 0 ? ao_opts->blocksize : AO_DEFAULT_BLOCKSIZE);
	e2 = makeDefElem("blocksize", (Node *) makeInteger(blocksize), -1);

	compresslevel = appendonly && compresslevel != 0 ? compresslevel :
					(ao_opts->compresslevel != 0 ? ao_opts->compresslevel : AO_DEFAULT_COMPRESSLEVEL);
	e3 = makeDefElem("compresslevel", (Node *) makeInteger(compresslevel), -1);

	return list_make3(e1, e2, e3);
}

/*
 * See if two encodings attempt to set the same parameters.
 */
static bool
encodings_overlap(List *a, List *b)
{
	ListCell *lca;
	foreach(lca, a)
	{
		ListCell *lcb;
		DefElem *ela = lfirst(lca);

		foreach(lcb, b)
		{
			DefElem *elb = lfirst(lcb);
			if (pg_strcasecmp(ela->defname, elb->defname) == 0)
				return true;
		}
	}
	return false;
}

/*
 * Validate the sanity of column reference storage clauses.
 *
 * 1. Ensure that we only refer to columns that exist.
 * 2. Ensure that each column is referenced either zero times or once.
 * 3. Ensure that the column reference storage clauses do not clash with the
 * 	  gp_default_storage_options
 */
static void
validateColumnStorageEncodingClauses(List *aocoColumnEncoding,
									 List *tableElts)
{
	ListCell *lc;
	struct HTAB *ht = NULL;
	struct colent {
		char colname[NAMEDATALEN];
		int count;
	} *ce = NULL;

	/* Generate a hash table for all the columns */
	foreach(lc, tableElts)
	{
		Node *n = lfirst(lc);

		if (IsA(n, ColumnDef))
		{
			ColumnDef *c = (ColumnDef *)n;
			char *colname;
			bool found = false;
			size_t n = NAMEDATALEN - 1 < strlen(c->colname) ?
							NAMEDATALEN - 1 : strlen(c->colname);

			colname = palloc0(NAMEDATALEN);
			MemSet(colname, 0, NAMEDATALEN);
			memcpy(colname, c->colname, n);
			colname[n] = '\0';

			if (!ht)
			{
				HASHCTL  cacheInfo;
				int      cacheFlags;

				memset(&cacheInfo, 0, sizeof(cacheInfo));
				cacheInfo.keysize = NAMEDATALEN;
				cacheInfo.entrysize = sizeof(*ce);
				cacheFlags = HASH_ELEM;

				ht = hash_create("column info cache",
								 list_length(tableElts),
								 &cacheInfo, cacheFlags);
			}

			ce = hash_search(ht, colname, HASH_ENTER, &found);

			/*
			 * The user specified a duplicate column name. We check duplicate
			 * column names VERY late (under MergeAttributes(), which is called
			 * by DefineRelation(). For the specific case here, it is safe to
			 * call out that this is a duplicate. We don't need to delay until
			 * we look at inheritance.
			 */
			if (found)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" duplicated",
								colname)));
			}
			ce->count = 0;
		}
	}

	/*
	 * If the table has no columns -- usually in the partitioning case -- then
	 * we can short circuit.
	 */
	if (!ht)
		return;

	/*
	 * All column reference storage directives without the DEFAULT
	 * clause should refer to real columns.
	 */
	foreach(lc, aocoColumnEncoding)
	{
		ColumnReferenceStorageDirective *c = lfirst(lc);

		Assert(IsA(c, ColumnReferenceStorageDirective));

		if (c->deflt)
			continue;
		else
		{
			bool found = false;
			char colname[NAMEDATALEN];
			size_t collen = strlen(c->column);
			size_t n = NAMEDATALEN - 1 < collen ? NAMEDATALEN - 1 : collen;
			MemSet(colname, 0, NAMEDATALEN);
			memcpy(colname, c->column, n);
			colname[n] = '\0';

			ce = hash_search(ht, colname, HASH_FIND, &found);

			if (!found)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("column \"%s\" does not exist", colname)));

			ce->count++;

			if (ce->count > 1)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("column \"%s\" referenced in more than one COLUMN ENCODING clause",
								colname)));
		}
	}

	hash_destroy(ht);

	foreach(lc, aocoColumnEncoding)
	{
		ColumnReferenceStorageDirective *crsd = lfirst(lc);

		Datum d = transformRelOptions(PointerGetDatum(NULL),
									  crsd->encoding,
									  NULL, NULL,
									  true, false);
		StdRdOptions *stdRdOptions = (StdRdOptions *)default_reloptions(d,
																	true,
																	RELOPT_KIND_APPENDOPTIMIZED);

		validateOrientationRelOptions(stdRdOptions->compresstype, true);
	}
}

/*
 * Make a default column storage directive from a WITH clause
 * Ignore options in the WITH clause that don't appear in
 * storage_directives for column-level compression.
 */
List *
form_default_storage_directive(List *enc)
{
	List *out = NIL;
	ListCell *lc;

	foreach(lc, enc)
	{
		DefElem *el = lfirst(lc);

		if (!el->defname)
			out = lappend(out, copyObject(el));

		if (pg_strcasecmp("oids", el->defname) == 0)
			continue;
		if (pg_strcasecmp("fillfactor", el->defname) == 0)
			continue;
		if (pg_strcasecmp("tablename", el->defname) == 0)
			continue;
		/* checksum is not a column specific attribute. */
		if (pg_strcasecmp("checksum", el->defname) == 0)
			continue;
		out = lappend(out, copyObject(el));
	}
	return out;
}

/*
 * Transform and validate the actual encoding clauses.
 *
 * We need tell the underlying system that these are AO/CO tables too,
 * hence the concatenation of the extra elements.
 *
 * If 'validate' is true, we validate that the optionsa are valid WITH options
 * for an AO table. Otherwise, any unrecognized options are passed through as
 * is.
 */
List *
transformStorageEncodingClause(List *aocoColumnEncoding, bool validate)
{
	ListCell   *lc;
	DefElem	   *dl = NULL;
	int c = 0;

	foreach_with_count(lc, aocoColumnEncoding, c)
	{
		dl = (DefElem *) lfirst(lc);
		if (pg_strncasecmp(dl->defname, SOPT_CHECKSUM, strlen(SOPT_CHECKSUM)) == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"%s\" is not a column specific option",
							SOPT_CHECKSUM)));
		}
		/* 
		 * For compresstype, the value must be modified from the value passed
		 * into the encoding clause if gp_quicklz_fallback is enabled and "quicklz"
		 * is specified. The value will instead fallback to "zstd" if available, else
		 * the default usable compresstype.
		 */
		if (pg_strncasecmp(dl->defname, SOPT_COMPTYPE, strlen(SOPT_COMPTYPE)) == 0
				&& gp_quicklz_fallback)
		{
			char *name = defGetString(dl);
			if (pg_strcasecmp(name, "quicklz") == 0)
			{
#ifdef USE_ZSTD
				char *compresstype = "zstd";
#else
				char *compresstype = AO_DEFAULT_USABLE_COMPRESSTYPE;
#endif
				dl = makeDefElem("compresstype", (Node *) makeString(compresstype), -1);
			}
				list_nth_replace(aocoColumnEncoding, c, dl);
		}
	}

	/* add defaults for missing values */
	aocoColumnEncoding = fillin_encoding(aocoColumnEncoding);

	/*
	 * The following two statements validate that the encoding clause is well
	 * formed.
	 */
	if (validate)
	{
		Datum		d;

		d = transformRelOptions(PointerGetDatum(NULL),
								aocoColumnEncoding,
								NULL, NULL,
								true, false);
		(void) default_reloptions(d, true, RELOPT_KIND_APPENDOPTIMIZED);
	}

	return aocoColumnEncoding;
}

/*
 * Find the column reference storage encoding clause for `column'.
 *
 * This is called by transformColumnEncoding() in a loop but stenc should be
 * quite small in practice.
 */
ColumnReferenceStorageDirective *
find_crsd(const char *column, List *stenc)
{
	ListCell *lc;

	foreach(lc, stenc)
	{
		ColumnReferenceStorageDirective *c = lfirst(lc);

		if (c->deflt == false && strcmp(column, c->column) == 0)
			return c;
	}
	return NULL;
}

/*
 * Parse and validate COLUMN <col> ENCODING ... directives.
 *
 * The 'colDefs', 'stenc' and 'taboptions' arguments are parts of the
 * CREATE TABLE or ALTER TABLE command:
 *
 * 'colDefs' - list of ColumnDefs
 * 'stenc' - list of ColumnReferenceStorageDirectives
 * 'withOptions' - list of WITH options
 * 'parentenc' - list of ColumnReferenceStorageDirectives explicitly defined for
 * parent partition
 * 'explicitOnly' - Only return explicitly defined column encoding values
 *  to be used for child partitions
 *
 * ENCODING options can be attached to column definitions, like
 * "mycolumn integer ENCODING ..."; these go into ColumnDefs. They
 * can also be specified with the "COLUMN mycolumn ENCODING ..." syntax;
 * they go into the ColumnReferenceStorageDirectives. And table-wide
 * defaults can be given in the WITH clause.
 *
 * Normally if any ENCODING clause was given for a non-AO/CO table,
 * we should report an error. However, exception exists in DefineRelation()
 * where we allow that to happen, so we pass in errorOnEncodingClause to
 * indicate whether we should report this error. 
 *
 * This function is called for RELKIND_PARTITIONED_TABLE as well even if we
 * don't store entries in pg_attribute_encoding for rootpartition. The reason
 * is to transformColumnEncoding for parent as need to use them later while
 * creating partitions in GPDB legacy partitioning syntax. Hence, if
 * rootpartition add to the list, only encoding elements specified in command,
 * defaults based on GUCs and such are skipped. Each child partition would
 * independently later run through this logic and that time add those GUC
 * specific defaults if required. Reason to avoid adding defaults for
 * rootpartition is need to first merge partition level user specified options
 * and then need to add defaults only for remaining columns.
 *
 * NOTE: This is *not* performed during the parse analysis phase, like
 * most transformation, but only later in DefineRelation() or ATExecAddColumn(). 
 * This needs access to possible inherited columns, so it can only be done after
 * expanding them.
 */
List* transformColumnEncoding(Relation rel, List *colDefs, List *stenc, List *withOptions, List *parentenc, bool explicitOnly, bool errorOnEncodingClause)
{
	ColumnReferenceStorageDirective *deflt = NULL;
	ListCell   *lc;
	List	   *result = NIL;

	if (stenc)
		validateColumnStorageEncodingClauses(stenc, colDefs);

	/* get the default clause, if there is one. */
	foreach(lc, stenc)
	{
		ColumnReferenceStorageDirective *c = lfirst(lc);
		Assert(IsA(c, ColumnReferenceStorageDirective));

		if (errorOnEncodingClause)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("ENCODING clause only supported with column oriented tables")));
		if (c->deflt)
		{
			/*
			 * Some quick validation: there should only be one default
			 * clause
			 */
			if (deflt)
				elog(ERROR, "only one default column encoding may be specified");

			deflt = copyObject(c);
			deflt->encoding = transformStorageEncodingClause(deflt->encoding, true);

			/*
			 * The default encoding and the with clause better not
			 * try and set the same options!
			 */
			if (encodings_overlap(withOptions, deflt->encoding))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("DEFAULT COLUMN ENCODING clause cannot override values set in WITH clause")));
		}

	}

	/*
	 * If no default has been specified, we might create one out of the
	 * WITH clause.
	 */
	if (!deflt)
	{
		List	   *tmpenc;
		if ((tmpenc = form_default_storage_directive(withOptions)) != NULL)
		{
			deflt = makeNode(ColumnReferenceStorageDirective);
			deflt->deflt = true;
			deflt->encoding = transformStorageEncodingClause(tmpenc, false);
		}
	}

	foreach(lc, colDefs)
	{
		Node	   *elem = (Node *) lfirst(lc);
		ColumnDef *d;
		ColumnReferenceStorageDirective *c;
		List *encoding = NIL;

		Assert(IsA(elem, ColumnDef));

		d = (ColumnDef *) elem;

		/*
		 * Find a storage encoding for this column, in this order:
		 *
		 * 1. An explicit encoding clause in the ColumnDef
		 * 2. A column reference storage directive for this column
		 * 3. A default column encoding in the statement
		 * 4. Parent partition's column encoding values
		 * 5. A default for the type.
		 */
		if (d->encoding)
		{
			encoding = transformStorageEncodingClause(d->encoding, true);
			if (errorOnEncodingClause)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("ENCODING clause only supported with column oriented tables")));
		}
		else
		{
			ColumnReferenceStorageDirective *s = find_crsd(d->colname, stenc);

			if (s)
				encoding = transformStorageEncodingClause(s->encoding, true);
			else
			{
				if (deflt)
					encoding = copyObject(deflt->encoding);
				else if (!explicitOnly)
				{
					if (parentenc)
					{
						ColumnReferenceStorageDirective *parent_col_encoding = find_crsd(d->colname, parentenc);
						encoding = transformStorageEncodingClause(parent_col_encoding->encoding, true);
					}
					else if (d->typeName)
						encoding = get_type_encoding(d->typeName);
					if (!encoding)
						encoding = default_column_encoding_clause(rel);
				}
			}
		}

		if (encoding)
		{
			c = makeNode(ColumnReferenceStorageDirective);
			c->column = pstrdup(d->colname);
			c->encoding = encoding;

			result = lappend(result, c);
		}
	}

	return result;
}

/*
 * Update the corresponding ColumnReferenceStorageDirective clause
 * in a list of such clauses: current_encodings.
 *
 * If anything is really updated (either existing one is changed or
 * a new crsd is added), set is_updated to true. Otherwise false.
 *
 * Return the updated or original current_encodings.
 */
List*
updateEncodingList(List *current_encodings, ColumnReferenceStorageDirective *new_crsd, bool *is_updated)
{
	ListCell *lc_current;
	ColumnReferenceStorageDirective *crsd = NULL;

	Assert(is_updated);
	foreach(lc_current, current_encodings)
	{
		ColumnReferenceStorageDirective *current_crsd = (ColumnReferenceStorageDirective *) lfirst(lc_current);

		if (current_crsd->deflt == false &&
			strcmp(new_crsd->column, current_crsd->column) == 0)
		{
			crsd = current_crsd;
			break;
		}
	}
	if (crsd)
	{
		ListCell *lc1;
		List *merged_encodings = NIL;
		*is_updated = false;

		/*
		 * Create a new list of encodings merging the existing and new values.
		 *
		 * Assuming crsd->encoding is complete list of all encoding attributes,
		 * but new_crsd->encoding may or may not be complete list.
		 */
		foreach(lc1, crsd->encoding)
		{
			ListCell *lc2;
			DefElem  *el1        = lfirst(lc1);
			DefElem  *el2;
			bool current_updated = false;
			foreach (lc2, new_crsd->encoding)
			{
				el2 = lfirst(lc2);
				if ((strcmp(el1->defname, el2->defname) == 0) &&
					(strcmp(defGetString(el1), defGetString(el2)) != 0))
				{
					current_updated  = true;
					*is_updated       = true;
					merged_encodings = lappend(merged_encodings, copyObject(el2));
				}
			}
			if (!current_updated)
				merged_encodings = lappend(merged_encodings, copyObject(el1));
		}
		/*
		 * Validate the merged encodings to weed out duplicate parameters and/or
		 * invalid parameter values.
		 * We can have duplicate parameters if user enters for eg:
		 * ALTER COLUMN a SET ENCODING (compresslevel=3, compresslevel=4);
		 */
		merged_encodings =
			transformStorageEncodingClause(merged_encodings, true);

		/*
		 * Update current_encodings in place with the merged and validated merged_encodings
		 */
		list_free_deep(crsd->encoding);
		crsd->encoding = merged_encodings;
	}
	else
	{
		/*
		 * new_crsd->column not found in current_encodings
		 * Must be coming from a newly added column
		 */

		new_crsd->encoding = transformStorageEncodingClause(new_crsd->encoding, true);
		current_encodings = lappend(current_encodings, new_crsd);
		*is_updated = true;
	}
	return current_encodings;
}

/*
 * GPDB: Convenience function to judge a relation option whether already in opts
 */
bool
reloptions_has_opt(List *opts, const char *name)
{
	ListCell *lc;
	foreach(lc, opts)
	{
		DefElem *de = lfirst(lc);
		if (pg_strcasecmp(de->defname, name) == 0)
			return true;
	}
	return false;
}

/*
 * GPDB: Convenience function to build storage reloptions for a given relation, just for AO table.
 */
List *
build_ao_rel_storage_opts(List *opts, Relation rel)
{
	bool		checksum = true;
	int32		blocksize = -1;
	int16		compresslevel = 0;
	char	   *compresstype = NULL;
	NameData	compresstype_nd;

	GetAppendOnlyEntryAttributes(RelationGetRelid(rel),
								 &blocksize,
								 &compresslevel,
								 &checksum,
								 &compresstype_nd);
	compresstype = NameStr(compresstype_nd);

	if (!reloptions_has_opt(opts, "blocksize"))
		opts = lappend(opts, makeDefElem("blocksize", (Node *) makeInteger(blocksize), -1));

	if (!reloptions_has_opt(opts, "compresslevel"))
		opts = lappend(opts, makeDefElem("compresslevel", (Node *) makeInteger(compresslevel), -1));

	if (!reloptions_has_opt(opts, "checksum"))
		opts = lappend(opts, makeDefElem("checksum", (Node *) makeInteger(checksum), -1));

	if (!reloptions_has_opt(opts, "compresstype"))
	{
		compresstype = (compresstype && compresstype[0]) ? pstrdup(compresstype) : "none";
		opts = lappend(opts, makeDefElem("compresstype", (Node *) makeString(compresstype), -1));
	}

	return opts;
}
