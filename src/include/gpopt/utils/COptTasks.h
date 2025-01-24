//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2011 EMC Greenplum, Inc.
//
//	@filename:
//		COptTasks.h
//
//	@doc:
//		Tasks that will perform optimization and related tasks
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef COptTasks_H
#define COptTasks_H

#include "gpos/error/CException.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/search/CSearchStage.h"
#include "gpopt/translate/CTranslatorUtils.h"



// fwd decl
namespace gpos
{
class CMemoryPool;
class CBitSet;
}  // namespace gpos

namespace gpdxl
{
class CDXLNode;
}

namespace gpopt
{
class CExpression;
class CMDAccessor;
class CQueryContext;
class COptimizerConfig;
class ICostModel;
class CPlanHint;
}  // namespace gpopt

struct PlannedStmt;
struct Query;
struct List;
struct MemoryContextData;

using namespace gpos;
using namespace gpdxl;
using namespace gpopt;

// context of optimizer input and output objects
struct SOptContext
{
	// mark which pointer member should NOT be released
	// when calling Free() function
	enum EPin
	{
		epinQueryDXL,  // keep m_query_dxl
		epinQuery,	   // keep m_query
		epinPlanDXL,   // keep m_plan_dxl
		epinPlStmt,	   // keep m_plan_stmt
		epinErrorMsg   // keep m_error_msg
	};

	// query object serialized to DXL
	CHAR *m_query_dxl{nullptr};

	// query object
	Query *m_query{nullptr};

	// plan object serialized to DXL
	CHAR *m_plan_dxl{nullptr};

	// plan object
	PlannedStmt *m_plan_stmt{nullptr};

	// is generating a plan object required ?
	BOOL m_should_generate_plan_stmt{false};

	// is serializing a plan to DXL required ?
	BOOL m_should_serialize_plan_dxl{false};

	// did the optimizer fail unexpectedly?
	BOOL m_is_unexpected_failure{false};

	// buffer for optimizer error messages
	CHAR *m_error_msg{nullptr};

	// ctor
	SOptContext();

	// If there is an error print as warning and throw exception to abort
	// plan generation
	void HandleError(BOOL *had_unexpected_failure);

	// free all members except input and output pointers
	void Free(EPin input, EPin epinOutput) const;

	// Clone the error message in given context.
	CHAR *CloneErrorMsg(struct MemoryContextData *context) const;

	// casting function
	static SOptContext *Cast(void *ptr);

};	// struct SOptContext

class COptTasks
{
private:
	// execute a task given the argument
	static void Execute(void *(*func)(void *), void *func_arg);

	// map GPOS log severity level to GPDB, print error and delete the given error buffer
	static void LogExceptionMessageAndDelete(CHAR *err_buf);

	// create optimizer configuration object
	static COptimizerConfig *CreateOptimizerConfig(CMemoryPool *mp,
												   ICostModel *cost_model,
												   CPlanHint *plan_hints);

	// optimize a query to a physical DXL
	static void *OptimizeTask(void *ptr);

	// translate a DXL tree into a planned statement
	static PlannedStmt *ConvertToPlanStmtFromDXL(
		CMemoryPool *mp, CMDAccessor *md_accessor, const Query *orig_query,
		const CDXLNode *dxlnode, bool can_set_tag,
		DistributionHashOpsKind distribution_hashops);

	// load search strategy from given path
	static CSearchStageArray *LoadSearchStrategy(CMemoryPool *mp, char *path);

	// helper for converting wide character string to regular string
	static CHAR *CreateMultiByteCharStringFromWCString(const WCHAR *wcstr);

	// set cost model parameters
	static void SetCostModelParams(ICostModel *cost_model);

	// generate an instance of optimizer cost model
	static ICostModel *GetCostModel(CMemoryPool *mp, ULONG num_segments);

	// create optimizer plan hints
	static CPlanHint *GetPlanHints(CMemoryPool *mp, Query *query);

	// print warning messages for columns with missing statistics
	static void PrintMissingStatsWarning(CMemoryPool *mp,
										 CMDAccessor *md_accessor,
										 IMdIdArray *col_stats,
										 MdidHashSet *phsmdidRel);

public:
	// convert Query->DXL->LExpr->Optimize->PExpr->DXL
	static char *Optimize(Query *query);

	// optimize Query->DXL->LExpr->Optimize->PExpr->DXL->PlannedStmt
	static PlannedStmt *GPOPTOptimizedPlan(Query *query,
										   SOptContext *gpopt_context);

	// enable/disable a given xforms
	static bool SetXform(char *xform_str, bool should_disable);
};

#endif	// COptTasks_H

// EOF
