//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		dxltokens.h
//
//	@doc:
//		Constants for the tokens used in the DXL document.
//		Tokens are represented in both in CWStringConst format, and as XMLCh
//		arrays - the native format of the Xerces parser.
//		Constants are dynamically created during loading of the dxl library.
//---------------------------------------------------------------------------
#ifndef GPDXL_dxltokens_H
#define GPDXL_dxltokens_H

#include <xercesc/sax2/DefaultHandler.hpp>
#include <xercesc/util/XMLUniDefs.hpp>

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/string/CWStringConst.h"

namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//fwd decl
class CDXLMemoryManager;

enum Edxltoken
{
	EdxltokenDXLMessage,

	EdxltokenComment,

	EdxltokenPlan,
	EdxltokenPlanId,
	EdxltokenPlanSpaceSize,
	EdxltokenSamplePlans,
	EdxltokenSamplePlan,
	EdxltokenCostDistr,
	EdxltokenRelativeCost,
	EdxltokenX,
	EdxltokenY,

	EdxltokenOptimizerConfig,
	EdxltokenEnumeratorConfig,
	EdxltokenStatisticsConfig,
	EdxltokenDampingFactorFilter,
	EdxltokenDampingFactorJoin,
	EdxltokenDampingFactorGroupBy,
	EdxltokenCTEConfig,
	EdxltokenCTEInliningCutoff,
	EdxltokenCostModelConfig,
	EdxltokenCostModelType,
	EdxltokenSegmentsForCosting,
	EdxltokenHint,
	EdxltokenPlanHint,
	EdxltokenScanHint,
	EdxltokenRowHint,
	EdxltokenJoinHint,
	EdxltokenLeading,
	EdxltokenJoinArityForAssociativityCommutativity,
	EdxltokenArrayExpansionThreshold,
	EdxltokenJoinOrderDPThreshold,
	EdxltokenBroadcastThreshold,
	EdxltokenEnforceConstraintsOnDML,
	EdxltokenPushGroupByBelowSetopThreshold,
	EdxltokenXformBindThreshold,
	EdxltokenSkewFactor,
	EdxltokenMaxStatsBuckets,
	EdxltokenWindowOids,
	EdxltokenOidRowNumber,
	EdxltokenOidRank,

	EdxltokenPlanSamples,

	EdxltokenMetadata,
	EdxltokenTraceFlags,
	EdxltokenMDRequest,

	EdxltokenSysids,
	EdxltokenSysid,

	EdxltokenThread,

	EdxltokenPhysical,

	EdxltokenPhysicalTableScan,
	EdxltokenPhysicalBitmapTableScan,
	EdxltokenPhysicalDynamicBitmapTableScan,
	EdxltokenPhysicalForeignScan,
	EdxltokenPhysicalIndexScan,
	EdxltokenPhysicalIndexOnlyScan,
	EdxltokenPhysicalHashJoin,
	EdxltokenPhysicalNLJoin,
	EdxltokenPhysicalNLJoinIndex,
	EdxltokenPhysicalMergeJoin,
	EdxltokenPhysicalGatherMotion,
	EdxltokenPhysicalBroadcastMotion,
	EdxltokenPhysicalRedistributeMotion,
	EdxltokenPhysicalRoutedDistributeMotion,
	EdxltokenPhysicalRandomMotion,
	EdxltokenPhysicalSort,
	EdxltokenPhysicalLimit,
	EdxltokenPhysicalResult,
	EdxltokenPhysicalAggregate,
	EdxltokenPhysicalAppend,
	EdxltokenPhysicalMaterialize,
	EdxltokenPhysicalDynamicForeignScan,
	EdxltokenPhysicalSequence,
	EdxltokenPhysicalDynamicTableScan,
	EdxltokenPhysicalDynamicIndexScan,
	EdxltokenPhysicalTVF,
	EdxltokenPhysicalWindow,
	EdxltokenPhysicalPartitionSelector,
	EdxltokenPhysicalPartitionSelectorId,
	EdxltokenPhysicalPartitionSelectorScanId,
	EdxltokenPhysicalValuesScan,

	EdxltokenPhysicalCTEProducer,
	EdxltokenPhysicalCTEConsumer,

	EdxltokenDuplicateSensitive,

	EdxltokenSegmentIdCol,

	EdxltokenScalar,

	EdxltokenScalarProjList,
	EdxltokenScalarFilter,
	EdxltokenScalarAggref,
	EdxltokenScalarWindowref,
	EdxltokenScalarArrayComp,
	EdxltokenScalarBoolTestIsTrue,
	EdxltokenScalarBoolTestIsNotTrue,
	EdxltokenScalarBoolTestIsFalse,
	EdxltokenScalarBoolTestIsNotFalse,
	EdxltokenScalarBoolTestIsUnknown,
	EdxltokenScalarBoolTestIsNotUnknown,
	EdxltokenScalarBoolAnd,
	EdxltokenScalarBoolOr,
	EdxltokenScalarBoolNot,
	EdxltokenScalarMin,
	EdxltokenScalarMax,
	EdxltokenScalarCaseTest,
	EdxltokenScalarCoalesce,
	EdxltokenScalarComp,
	EdxltokenScalarConstValue,
	EdxltokenScalarDistinctComp,
	EdxltokenScalarFuncExpr,
	EdxltokenScalarIsNull,
	EdxltokenScalarIsNotNull,
	EdxltokenScalarNullIf,
	EdxltokenScalarHashCondList,
	EdxltokenScalarMergeCondList,
	EdxltokenScalarHashExprList,
	EdxltokenScalarHashExpr,
	EdxltokenScalarIdent,
	EdxltokenScalarIfStmt,
	EdxltokenScalarSwitch,
	EdxltokenScalarSwitchCase,
	EdxltokenScalarSubPlan,
	EdxltokenScalarJoinFilter,
	EdxltokenScalarRecheckCondFilter,
	EdxltokenScalarLimitCount,
	EdxltokenScalarLimitOffset,
	EdxltokenScalarOneTimeFilter,
	EdxltokenScalarOpExpr,
	EdxltokenScalarParam,
	EdxltokenScalarProjElem,
	EdxltokenScalarCast,
	EdxltokenScalarCoerceToDomain,
	EdxltokenScalarCoerceViaIO,
	EdxltokenScalarArrayCoerceExpr,
	EdxltokenScalarSortCol,
	EdxltokenScalarSortColList,
	EdxltokenScalarGroupingColList,
	EdxltokenScalarSortGroupClause,

	EdxltokenScalarBitmapAnd,
	EdxltokenScalarBitmapOr,

	EdxltokenScalarArray,
	EdxltokenScalarArrayRef,
	EdxltokenScalarArrayRefIndexList,
	EdxltokenScalarArrayRefIndexListBound,
	EdxltokenScalarArrayRefIndexListLower,
	EdxltokenScalarArrayRefIndexListUpper,
	EdxltokenScalarArrayRefExpr,
	EdxltokenScalarArrayRefAssignExpr,

	EdxltokenScalarAssertConstraint,
	EdxltokenScalarAssertConstraintList,

	EdxltokenScalarSubPlanType,
	EdxltokenScalarSubPlanTypeScalar,
	EdxltokenScalarSubPlanTypeExists,
	EdxltokenScalarSubPlanTypeNotExists,
	EdxltokenScalarSubPlanTypeAny,
	EdxltokenScalarSubPlanTypeAll,
	EdxltokenScalarSubPlanTestExpr,
	EdxltokenScalarSubPlanParamList,
	EdxltokenScalarSubPlanParam,

	EdxltokenScalarSubquery,
	EdxltokenScalarSubqueryAll,
	EdxltokenScalarSubqueryAny,
	EdxltokenScalarSubqueryExists,
	EdxltokenScalarSubqueryNotExists,

	EdxltokenScalarDMLAction,
	EdxltokenScalarOpList,

	EdxltokenPartLevelEqFilterList,
	EdxltokenPartLevelFilterList,
	EdxltokenPartLevel,
	EdxltokenScalarPartDefault,
	EdxltokenScalarResidualFilter,
	EdxltokenScalarPartFilterExpr,
	EdxltokenScalarBitmapIndexProbe,
	EdxltokenScalarValuesList,

	EdxltokenWindowFrame,
	EdxltokenScalarWindowFrameLeadingEdge,
	EdxltokenScalarWindowFrameTrailingEdge,
	EdxltokenWindowKeyList,
	EdxltokenWindowKey,

	EdxltokenWindowSpecList,
	EdxltokenWindowSpec,

	EdxltokenWindowFrameSpec,
	EdxltokenWindowFSRow,
	EdxltokenWindowFSRange,
	EdxltokenWindowFSGroups,

	EdxltokenWindowLeadingBoundary,
	EdxltokenWindowTrailingBoundary,
	EdxltokenWindowBoundaryUnboundedPreceding,
	EdxltokenWindowBoundaryBoundedPreceding,
	EdxltokenWindowBoundaryCurrentRow,
	EdxltokenWindowBoundaryUnboundedFollowing,
	EdxltokenWindowBoundaryBoundedFollowing,
	EdxltokenWindowBoundaryDelayedBoundedPreceding,
	EdxltokenWindowBoundaryDelayedBoundedFollowing,

	EdxltokenWindowExclusionStrategy,
	EdxltokenWindowESNone,
	EdxltokenWindowESNulls,
	EdxltokenWindowESCurrentRow,
	EdxltokenWindowESGroup,
	EdxltokenWindowESTies,

	EdxltokenWindowStartInRangeOid,
	EdxltokenWindowEndInRangeOid,
	EdxltokenWindowInRangeColl,
	EdxltokenWindowInRangeAsc,
	EdxltokenWindowInRangeNullsFirst,

	EdxltokenWindowrefOid,
	EdxltokenWindowrefDistinct,
	EdxltokenWindowrefStarArg,
	EdxltokenWindowrefSimpleAgg,
	EdxltokenWindowrefStrategy,
	EdxltokenWindowrefStageImmediate,
	EdxltokenWindowrefStagePreliminary,
	EdxltokenWindowrefStageRowKey,
	EdxltokenWindowrefWinSpecPos,

	// FIELDSELECT
	EdxltokenScalarFieldSelect,
	EdxltokenScalarFieldSelectFieldType,
	EdxltokenScalarFieldSelectFieldCollation,
	EdxltokenScalarFieldSelectFieldNumber,
	EdxltokenScalarFieldSelectTypeModifier,

	EdxltokenValue,
	EdxltokenTypeId,
	EdxltokenTypeIds,

	EdxltokenConstTuple,
	EdxltokenDatum,

	// CoerceToDomain and CoerceViaIO and ArrayCoerceExpr related tokens
	EdxltokenTypeMod,
	EdxltokenCoercionForm,
	EdxltokenLocation,
	EdxltokenIsExplicit,

	EdxltokenJoinType,
	EdxltokenJoinInner,
	EdxltokenJoinLeft,
	EdxltokenJoinFull,
	EdxltokenJoinRight,
	EdxltokenJoinIn,
	EdxltokenJoinLeftAntiSemiJoin,
	EdxltokenJoinLeftAntiSemiJoinNotIn,

	EdxltokenMergeJoinUniqueOuter,

	EdxltokenAggStrategy,
	EdxltokenAggStrategyPlain,
	EdxltokenAggStrategySorted,
	EdxltokenAggStrategyHashed,
	EdxltokenAggStreamSafe,

	EdxltokenAggrefOid,
	EdxltokenAggrefDistinct,
	EdxltokenAggrefArgTypes,
	EdxltokenAggrefKind,
	EdxltokenAggrefStage,
	EdxltokenAggrefLookups,
	EdxltokenAggrefStageNormal,
	EdxltokenAggrefStagePartial,
	EdxltokenAggrefStageIntermediate,
	EdxltokenAggrefStageFinal,
	EdxltokenAggrefKindNormal,
	EdxltokenAggrefKindOrderedSet,
	EdxltokenAggrefKindHypothetical,

	EdxltokenArrayType,
	EdxltokenArrayElementType,
	EdxltokenArrayMultiDim,

	EdxltokenTableDescr,
	EdxltokenProperties,
	EdxltokenOutputCols,
	EdxltokenCost,
	EdxltokenStartupCost,
	EdxltokenTotalCost,
	EdxltokenRows,
	EdxltokenWidth,
	EdxltokenRelPages,
	EdxltokenRelAllVisible,
	EdxltokenCTASOptions,
	EdxltokenCTASOption,

	EdxltokenExecuteAsUser,

	EdxltokenAlias,
	EdxltokenTableName,
	EdxltokenDerivedTableName,

	EdxltokenColDescr,
	EdxltokenColRef,

	EdxltokenColumns,
	EdxltokenColumn,
	EdxltokenColName,
	EdxltokenTagColType,
	EdxltokenColId,
	EdxltokenAttno,
	EdxltokenColDropped,
	EdxltokenColWidth,
	EdxltokenColNullFreq,
	EdxltokenColNdvRemain,
	EdxltokenColFreqRemain,
	EdxltokenColStatsMissing,

	EdxltokenParamId,

	EdxltokenCtidColName,
	EdxltokenOidColName,
	EdxltokenXminColName,
	EdxltokenCminColName,
	EdxltokenXmaxColName,
	EdxltokenCmaxColName,
	EdxltokenTableOidColName,
	EdxltokenGpSegmentIdColName,

	// For Logical Select
	EdxltokenSecurityQuals,

	EdxltokenActionColId,
	EdxltokenCtidColId,
	EdxltokenGpSegmentIdColId,
	EdxltokenTupleOidColId,
	EdxltokenSplitUpdate,

	EdxltokenInputSegments,
	EdxltokenOutputSegments,
	EdxltokenSegment,
	EdxltokenSegId,

	EdxltokenGroupingCols,
	EdxltokenGroupingCol,

	EdxltokenParamKind,

	EdxltokenAppendIsTarget,
	EdxltokenAppendIsZapped,
	EdxltokenSelectorIds,

	EdxltokenOpNo,
	EdxltokenOpName,

	EdxltokenOpType,
	EdxltokenOpTypeAny,
	EdxltokenOpTypeAll,

	EdxltokenTypeName,
	EdxltokenUnknown,

	EdxltokenFuncId,
	EdxltokenFuncRetSet,
	EdxltokenFuncVariadic,


	EdxltokenSortOpId,
	EdxltokenSortOpName,
	EdxltokenSortDiscardDuplicates,
	EdxltokenSortNullsFirst,

	EdxltokenMaterializeEager,
	EdxltokenSpoolId,
	EdxltokenSpoolType,
	EdxltokenSpoolMaterialize,
	EdxltokenSpoolSort,
	EdxltokenSpoolMultiSlice,
	EdxltokenExecutorSliceId,
	EdxltokenConsumerSliceCount,

	EdxltokenComparisonOp,

	EdxltokenXMLDocHeader,
	EdxltokenNamespaceAttr,
	EdxltokenNamespacePrefix,
	EdxltokenNamespaceURI,

	EdxltokenBracketOpenTag,
	EdxltokenBracketCloseTag,
	EdxltokenBracketOpenEndTag,
	EdxltokenBracketCloseSingletonTag,
	EdxltokenColon,
	EdxltokenSemicolon,
	EdxltokenComma,
	EdxltokenDot,
	EdxltokenDotSemicolon,
	EdxltokenSpace,
	EdxltokenQuote,
	EdxltokenEq,
	EdxltokenIndent,

	EdxltokenTrue,
	EdxltokenFalse,

	// nest params related constants
	EdxltokenNLJIndexParamList,
	EdxltokenNLJIndexParam,
	EdxltokenNLJIndexOuterRefAsParam,

	// metadata-related constants
	EdxltokenRelation,
	EdxltokenRelationCTAS,
	EdxltokenName,
	EdxltokenSchema,
	EdxltokenTablespace,
	EdxltokenOid,
	EdxltokenKind,
	EdxltokenVersion,
	EdxltokenMdid,
	EdxltokenLockMode,
	EdxltokenAclMode,
	EdxltokenMDTypeRequest,
	EdxltokenTypeInfo,
	EdxltokenFuncInfo,
	EdxltokenRelationMdid,
	EdxltokenRelationStats,
	EdxltokenColumnStats,
	EdxltokenColumnStatsBucket,
	EdxltokenRelationExtendedStats,
	EdxltokenExtendedStats,
	EdxltokenExtendedStatsInfo,
	EdxltokenMVDependencyList,
	EdxltokenMVDependency,
	EdxltokenMVNDistinctList,
	EdxltokenMVNDistinct,
	EdxltokenDegree,
	EdxltokenFrom,
	EdxltokenTo,
	EdxltokenEmptyRelation,
	EdxltokenIsNull,
	EdxltokenLintValue,
	EdxltokenDoubleValue,
	EdxltokenAssignedQueryIdForTargetRel,

	EdxltokenRelTemporary,

	EdxltokenRelStorageType,
	EdxltokenRelStorageHeap,
	EdxltokenRelStorageAppendOnlyCols,
	EdxltokenRelStorageAppendOnlyRows,
	EdxltokenRelStorageMixedPartitioned,
	EdxltokenRelStorageForeign,
	EdxltokenRelStorageCompositeType,

	EdxltokenPartKeys,
	EdxltokenPartTypes,

	EdxltokenRelDistrPolicy,
	EdxltokenRelDistrCoordinatorOnly,
	EdxltokenRelDistrHash,
	EdxltokenRelDistrRandom,
	EdxltokenRelDistrReplicated,
	EdxltokenRelDistrUniversal,
	EdxltokenConvertHashToRandom,

	EdxltokenRelDistrOpfamilies,
	EdxltokenRelDistrOpfamily,

	EdxltokenRelDistrOpclasses,
	EdxltokenRelDistrOpclass,

	EdxltokenRelForeignServer,

	EdxltokenMetadataColumns,
	EdxltokenMetadataColumn,
	EdxltokenColumnNullable,

	EdxltokenKeys,
	EdxltokenDistrColumns,

	EdxltokenIndexKeyCols,
	EdxltokenIndexIncludedCols,
	EdxltokenIndexReturnableCols,
	EdxltokenIndexClustered,
	EdxltokenIndexAmCanOrder,
	EdxltokenIndexPartial,
	EdxltokenIndexType,
	EdxltokenIndexTypeBtree,
	EdxltokenIndexTypeHash,
	EdxltokenIndexTypeBitmap,
	EdxltokenIndexTypeGist,
	EdxltokenIndexTypeGin,
	EdxltokenIndexItemType,
	EdxltokenIndexKeysSortDirection,
	EdxltokenIndexKeysNullsDirection,
	EdxltokenIndexKeySortASC,
	EdxltokenIndexKeySortDESC,
	EdxltokenIndexKeyNullsFirst,
	EdxltokenIndexKeyNullsLast,

	EdxltokenOpfamily,
	EdxltokenOpfamilies,

	EdxltokenTypeInt4,
	EdxltokenTypeBool,

	EdxltokenMetadataIdList,
	EdxltokenIndexInfoList,
	EdxltokenIndexInfo,

	EdxltokenIndex,
	EdxltokenPartitions,
	EdxltokenPartition,

	EdxltokenConstraints,
	EdxltokenConstraint,

	EdxltokenCheckConstraints,
	EdxltokenCheckConstraint,
	EdxltokenPartConstraint,
	EdxltokenDefaultPartition,
	EdxltokenPartConstraintUnbounded,

	EdxltokenMDType,
	EdxltokenMDTypeRedistributable,
	EdxltokenMDTypeHashable,
	EdxltokenMDTypeMergeJoinable,
	EdxltokenMDTypeComposite,
	EdxltokenMDTypeIsTextRelated,
	EdxltokenMDTypeFixedLength,
	EdxltokenMDTypeLength,
	EdxltokenMDTypeByValue,
	EdxltokenMDTypeDistrOpfamily,
	EdxltokenMDTypeLegacyDistrOpfamily,
	EdxltokenMDTypePartOpfamily,
	EdxltokenMDTypeEqOp,
	EdxltokenMDTypeNEqOp,
	EdxltokenMDTypeLTOp,
	EdxltokenMDTypeLEqOp,
	EdxltokenMDTypeGTOp,
	EdxltokenMDTypeGEqOp,
	EdxltokenMDTypeCompOp,
	EdxltokenMDTypeHashOp,
	EdxltokenMDTypeArray,
	EdxltokenMDTypeRelid,

	EdxltokenMDTypeAggMin,
	EdxltokenMDTypeAggMax,
	EdxltokenMDTypeAggAvg,
	EdxltokenMDTypeAggSum,
	EdxltokenMDTypeAggCount,

	EdxltokenGPDBScalarOp,
	EdxltokenGPDBScalarOpLeftTypeId,
	EdxltokenGPDBScalarOpRightTypeId,
	EdxltokenGPDBScalarOpResultTypeId,
	EdxltokenGPDBScalarOpFuncId,
	EdxltokenGPDBScalarOpCommOpId,
	EdxltokenGPDBScalarOpInverseOpId,
	EdxltokenGPDBScalarOpLTOpId,
	EdxltokenGPDBScalarOpGTOpId,
	EdxltokenGPDBScalarOpCmpType,
	EdxltokenGPDBScalarOpHashOpfamily,
	EdxltokenGPDBScalarOpLegacyHashOpfamily,

	EdxltokenCmpEq,
	EdxltokenCmpNeq,
	EdxltokenCmpLt,
	EdxltokenCmpLeq,
	EdxltokenCmpGt,
	EdxltokenCmpGeq,
	EdxltokenCmpIDF,
	EdxltokenCmpOther,

	EdxltokenReturnsNullOnNullInput,
	EdxltokenIsNDVPreserving,

	EdxltokenGPDBFunc,
	EdxltokenGPDBFuncStability,
	EdxltokenGPDBFuncStable,
	EdxltokenGPDBFuncImmutable,
	EdxltokenGPDBFuncVolatile,

	EdxltokenGPDBFuncResultTypeId,
	EdxltokenGPDBFuncReturnsSet,
	EdxltokenGPDBFuncStrict,
	EdxltokenGPDBFuncNDVPreserving,
	EdxltokenGPDBFuncIsAllowedForPS,

	EdxltokenGPDBCast,
	EdxltokenGPDBCastBinaryCoercible,
	EdxltokenGPDBCastSrcType,
	EdxltokenGPDBCastDestType,
	EdxltokenGPDBCastFuncId,
	EdxltokenGPDBCastSrcElemType,
	EdxltokenGPDBCastCoercePathType,
	EdxltokenGPDBArrayCoerceCast,

	EdxltokenGPDBMDScCmp,

	EdxltokenGPDBAgg,
	EdxltokenGPDBIsAggOrdered,
	EdxltokenGPDBIsAggRepSafe,
	EdxltokenGPDBAggResultTypeId,
	EdxltokenGPDBAggIntermediateResultTypeId,
	EdxltokenGPDBAggSplittable,
	EdxltokenGPDBAggHashAggCapable,

	EdxltokenEntireRow,

	EdxltokenScalarExpr,  // top level scalar expression
	EdxltokenQuery,
	EdxltokenQueryOutput,
	EdxltokenLogical,
	EdxltokenLogicalGet,
	EdxltokenLogicalForeignGet,
	EdxltokenLogicalProject,
	EdxltokenLogicalSelect,
	EdxltokenLogicalJoin,
	EdxltokenLogicalCTEProducer,
	EdxltokenLogicalCTEConsumer,
	EdxltokenCTEList,
	EdxltokenLogicalCTEAnchor,
	EdxltokenLogicalLimit,
	EdxltokenLogicalOutput,
	EdxltokenLogicalConstTable,
	EdxltokenLogicalGrpBy,
	EdxltokenLogicalSetOperation,
	EdxltokenLogicalTVF,
	EdxltokenLogicalWindow,

	EdxltokenLogicalInsert,
	EdxltokenLogicalDelete,
	EdxltokenLogicalUpdate,
	EdxltokenLogicalCTAS,
	EdxltokenPhysicalCTAS,
	EdxltokenPhysicalDMLInsert,
	EdxltokenPhysicalDMLDelete,
	EdxltokenPhysicalDMLUpdate,
	EdxltokenDirectDispatchInfo,
	EdxltokenDirectDispatchIsRaw,
	EdxltokenDirectDispatchKeyValue,
	EdxltokenPhysicalSplit,
	EdxltokenPhysicalAssert,

	EdxltokenErrorCode,
	EdxltokenErrorMessage,

	EdxltokenOnCommitAction,
	EdxltokenOnCommitNOOP,
	EdxltokenOnCommitPreserve,
	EdxltokenOnCommitDelete,
	EdxltokenOnCommitDrop,

	EdxltokenInsertCols,
	EdxltokenDeleteCols,
	EdxltokenNewCols,
	EdxltokenOldCols,

	EdxltokenCTEId,

	EdxltokenLogicalGrpCols,

	EdxltokenInputCols,
	EdxltokenCastAcrossInputs,

	EdxltokenLogicalUnion,
	EdxltokenLogicalUnionAll,
	EdxltokenLogicalIntersect,
	EdxltokenLogicalIntersectAll,
	EdxltokenLogicalDifference,
	EdxltokenLogicalDifferenceAll,

	EdxltokenIndexDescr,
	EdxltokenIndexName,
	EdxltokenIndexScanDirection,
	EdxltokenIndexScanDirectionForward,
	EdxltokenIndexScanDirectionBackward,
	EdxltokenIndexScanDirectionNoMovement,
	EdxltokenScalarIndexCondList,

	EdxltokenStackTrace,

	EdxltokenStatistics,
	EdxltokenStatsBaseRelation,
	EdxltokenStatsDerivedRelation,
	EdxltokenStatsDerivedColumn,
	EdxltokenStatsBucketLowerBound,
	EdxltokenStatsBucketUpperBound,
	EdxltokenStatsFrequency,
	EdxltokenStatsDistinct,
	EdxltokenStatsBoundClosed,

	// search strategy
	EdxltokenSearchStrategy,
	EdxltokenSearchStage,
	EdxltokenXform,
	EdxltokenTimeThreshold,
	EdxltokenCostThreshold,

	// cost model parameters
	EdxltokenCostParams,
	EdxltokenCostParam,
	EdxltokenCostParamLowerBound,
	EdxltokenCostParamUpperBound,

	EdxltokenTopLimitUnderDML,

	EdxltokenCtasOptionType,
	EdxltokenVarTypeModList,

	EdxltokenIndexTypeBrin,

	EdxltokenForeignServerOid,
	EdxltokenPhysicalDynamicIndexOnlyScan,
	EdxltokenRelAppendOnlyVersion,

	EdxltokenAbsolute,
	EdxltokenAdd,
	EdxltokenSubtract,
	EdxltokenMultiply,

	EdxltokenSentinel
};

//---------------------------------------------------------------------------
//	@class:
//		CDXLTokens
//
//	@doc:
//		DXL tokens.
//
//---------------------------------------------------------------------------
class CDXLTokens
{
private:
	// element for mapping Edxltoken to WCHARs
	struct SWszMapElem
	{
		Edxltoken m_edxlt;
		const WCHAR *m_wsz;
	};

	// element for mapping Edxltoken to CWStringConst
	struct SStrMapElem
	{
		Edxltoken m_edxlt{EdxltokenSentinel};
		CWStringConst *m_pstr{nullptr};

		// ctor
		SStrMapElem() = default;

		// ctor
		SStrMapElem(Edxltoken edxlt, CWStringConst *str)
			: m_edxlt(edxlt), m_pstr(str)
		{
			GPOS_ASSERT(edxlt < EdxltokenSentinel);
			GPOS_ASSERT(str->IsValid());
		}

		//dtor
		~SStrMapElem()
		{
			GPOS_DELETE(m_pstr);
		}
	};

	// element for mapping Edxltoken to XML string
	struct SXMLStrMapElem
	{
		Edxltoken m_edxlt{EdxltokenSentinel};
		XMLCh *m_xmlsz{nullptr};

		// ctor
		SXMLStrMapElem() = default;

		// ctor
		SXMLStrMapElem(Edxltoken edxlt, XMLCh *xml_val)
			: m_edxlt(edxlt), m_xmlsz(xml_val)
		{
			GPOS_ASSERT(edxlt < EdxltokenSentinel);
			GPOS_ASSERT(nullptr != xml_val);
		}

		//dtor
		~SXMLStrMapElem()
		{
			GPOS_DELETE_ARRAY(reinterpret_cast<BYTE *>(m_xmlsz));
		}
	};

	// array maintaining the mapping Edxltoken -> CWStringConst
	static SStrMapElem *m_pstrmap;

	// array maintaining the mapping Edxltoken -> XML string
	static SXMLStrMapElem *m_pxmlszmap;

	// memory pool -- not owned
	static CMemoryPool *m_mp;

	// local dxl memory manager
	static CDXLMemoryManager *m_dxl_memory_manager;

	// create a string in Xerces XMLCh* format
	static XMLCh *XmlstrFromWsz(const WCHAR *wsz);

public:
	// retrieve a token in CWStringConst and XMLCh* format, respectively
	static const CWStringConst *GetDXLTokenStr(Edxltoken token_type);

	static const XMLCh *XmlstrToken(Edxltoken token_type);

	// initialize constants. Must be called before constants are accessed.
	static void Init(CMemoryPool *mp);

	// cleanup tokens
	static void Terminate();
};

}  // namespace gpdxl

#endif	// !GPDXL_dxltokens_H

// EOF
