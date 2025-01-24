//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDTypeBoolGPDB.h
//
//	@doc:
//		Class for representing BOOL types in GPDB
//---------------------------------------------------------------------------



#ifndef GPMD_CMDTypeBoolGPDB_H
#define GPMD_CMDTypeBoolGPDB_H

#include "gpos/base.h"

#include "naucrates/base/IDatumBool.h"
#include "naucrates/md/CGPDBTypeHelper.h"
#include "naucrates/md/IMDTypeBool.h"

#define GPDB_BOOL_OID OID(16)
#define GPDB_BOOL_OPFAMILY OID(2222)
#define GPDB_BOOL_LEGACY_OPFAMILY OID(7124)
#define GPDB_BOOL_LENGTH 1
#define GPDB_BOOL_EQ_OP OID(91)
#define GPDB_BOOL_NEQ_OP OID(85)
#define GPDB_BOOL_LT_OP OID(58)
#define GPDB_BOOL_LEQ_OP OID(1694)
#define GPDB_BOOL_GT_OP OID(59)
#define GPDB_BOOL_GEQ_OP OID(1695)
#define GPDB_BOOL_COMP_OP OID(1693)
#define GPDB_BOOL_ARRAY_TYPE OID(1000)
#define GPDB_BOOL_AGG_MIN OID(0)
#define GPDB_BOOL_AGG_MAX OID(0)
#define GPDB_BOOL_AGG_AVG OID(0)
#define GPDB_BOOL_AGG_SUM OID(0)
#define GPDB_BOOL_AGG_COUNT OID(2147)

// fwd decl
namespace gpdxl
{
class CXMLSerializer;
}

namespace gpmd
{
using namespace gpos;
using namespace gpnaucrates;


//---------------------------------------------------------------------------
//	@class:
//		CMDTypeBoolGPDB
//
//	@doc:
//		Class for representing BOOL types in GPDB
//
//---------------------------------------------------------------------------
class CMDTypeBoolGPDB : public IMDTypeBool
{
	friend class CGPDBTypeHelper<CMDTypeBoolGPDB>;

private:
	// memory pool
	CMemoryPool *m_mp;

	// type id
	IMDId *m_mdid;
	IMDId *m_distr_opfamily;
	IMDId *m_legacy_distr_opfamily;

	// mdids of different operators
	IMDId *m_mdid_op_eq;
	IMDId *m_mdid_op_neq;
	IMDId *m_mdid_op_lt;
	IMDId *m_mdid_op_leq;
	IMDId *m_mdid_op_gt;
	IMDId *m_mdid_op_geq;
	IMDId *m_mdid_op_cmp;
	IMDId *m_mdid_type_array;

	// min aggregate
	IMDId *m_mdid_min;

	// max aggregate
	IMDId *m_mdid_max;

	// avg aggregate
	IMDId *m_mdid_avg;

	// sum aggregate
	IMDId *m_mdid_sum;

	// count aggregate
	IMDId *m_mdid_count;

	// DXL for object
	const CWStringDynamic *m_dxl_str;

	// type name and id
	static CWStringConst m_str;
	static CMDName m_mdname;

	// a null datum of this type (used for statistics comparison)
	IDatum *m_datum_null;

	// private copy ctor
	CMDTypeBoolGPDB(const CMDTypeBoolGPDB &);

public:
	// ctor
	explicit CMDTypeBoolGPDB(CMemoryPool *mp);

	// dtor
	virtual ~CMDTypeBoolGPDB();

	// accessors
	virtual const CWStringDynamic *
	GetStrRepr() const
	{
		return m_dxl_str;
	}

	// type id
	virtual IMDId *MDId() const;

	IMDId *GetDistrOpfamilyMdid() const;

	// type name
	virtual CMDName Mdname() const;

	// is type redistributable
	virtual BOOL
	IsRedistributable() const
	{
		return true;
	}

	// is type fixed length
	virtual BOOL
	IsFixedLength() const
	{
		return true;
	}

	// is type composite
	virtual BOOL
	IsComposite() const
	{
		return false;
	}

	// type length
	virtual ULONG
	Length() const
	{
		return GPDB_BOOL_LENGTH;
	}

	// return the GPDB length
	virtual INT
	GetGPDBLength() const
	{
		return GPDB_BOOL_LENGTH;
	}

	// is type passed by value
	virtual BOOL
	IsPassedByValue() const
	{
		return true;
	}

	// id of specified comparison operator type
	virtual IMDId *GetMdidForCmpType(ECmpType cmp_type) const;

	virtual const IMDId *
	CmpOpMdid() const
	{
		return m_mdid_op_cmp;
	}

	// id of specified specified aggregate type
	virtual IMDId *GetMdidForAggType(EAggType agg_type) const;

	// is type hashable
	virtual BOOL
	IsHashable() const
	{
		return true;
	}

	// is type merge joinable
	virtual BOOL
	IsMergeJoinable() const
	{
		return true;
	}

	// array type id
	virtual IMDId *
	GetArrayTypeMdid() const
	{
		return m_mdid_type_array;
	}

	// id of the relation corresponding to a composite type
	virtual IMDId *
	GetBaseRelMdid() const
	{
		return NULL;
	}

	// return the null constant for this type
	virtual IDatum *
	DatumNull() const
	{
		return m_datum_null;
	}

	// factory method for creating constants
	virtual IDatumBool *CreateBoolDatum(CMemoryPool *mp, BOOL fValue,
										BOOL is_null) const;

	// create typed datum from DXL datum
	virtual IDatum *GetDatumForDXLDatum(CMemoryPool *mp,
										const CDXLDatum *dxl_datum) const;

	// serialize object in DXL format
	virtual void Serialize(gpdxl::CXMLSerializer *xml_serializer) const;

	// transformation function to generate datum from CDXLScalarConstValue
	virtual IDatum *GetDatumForDXLConstVal(
		const CDXLScalarConstValue *dxl_op) const;

	// generate the DXL datum from IDatum
	virtual CDXLDatum *GetDatumVal(CMemoryPool *mp, IDatum *datum) const;

	// generate the DXL scalar constant from IDatum
	virtual CDXLScalarConstValue *GetDXLOpScConst(CMemoryPool *mp,
												  IDatum *datum) const;

	// generate the DXL datum representing null value
	virtual CDXLDatum *GetDXLDatumNull(CMemoryPool *mp) const;

#ifdef GPOS_DEBUG
	// debug print of the type in the provided stream
	virtual void DebugPrint(IOstream &os) const;
#endif
};
}  // namespace gpmd

#endif	// !GPMD_CMDTypeBoolGPDB_H

// EOF
