//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IDatum.cpp
//
//	@doc:
//
//---------------------------------------------------------------------------

#include "naucrates/base/IDatum.h"

#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpnaucrates;
using namespace gpmd;

FORCE_GENERATE_DBGSTR(gpmd::IDatum);

// Cross-type date-vs-timestamp statistics comparisons.
//
// A date datum maps to LINT (days since the PG epoch) while a timestamp
// datum maps to CDouble (microseconds since the same epoch, see
// convert_timevalue_to_scalar), so the generic LINT/double comparison
// paths below cannot compare them.  The scales are exactly related
// (1 day = 86400e6 usecs, same epoch), so support this pair explicitly
// by lifting the date side onto the timestamp microsecond scale.
// Timestamptz is deliberately excluded: its comparison against date
// depends on the session timezone.
#define GPDB_STATS_USECS_PER_DAY 86400000000.0

static BOOL
FDateVsTimestampComparison(const IDatum *datum1, const IDatum *datum2)
{
	return (datum1->MDId()->Equals(&CMDIdGPDB::m_mdid_date) &&
			datum2->MDId()->Equals(&CMDIdGPDB::m_mdid_timestamp)) ||
		   (datum1->MDId()->Equals(&CMDIdGPDB::m_mdid_timestamp) &&
			datum2->MDId()->Equals(&CMDIdGPDB::m_mdid_date));
}

// value of a date or timestamp datum on the timestamp microsecond scale
static CDouble
DTimestampScaleValue(const IDatum *datum)
{
	if (datum->MDId()->Equals(&CMDIdGPDB::m_mdid_date))
	{
		return CDouble(static_cast<DOUBLE>(datum->GetLINTMapping()) *
					   GPDB_STATS_USECS_PER_DAY);
	}
	return datum->GetDoubleMapping();
}

//---------------------------------------------------------------------------
//	@function:
//		IDatum::StatsAreEqual
//
//	@doc:
//		Equality based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
BOOL
IDatum::StatsAreEqual(const IDatum *datum) const
{
	GPOS_ASSERT(nullptr != datum);

	if (FDateVsTimestampComparison(this, datum))
	{
		if (this->IsNull())
		{
			// nulls are equal from stats point of view
			return datum->IsNull();
		}
		if (datum->IsNull())
		{
			return false;
		}
		CDouble diff = DTimestampScaleValue(this) - DTimestampScaleValue(datum);
		return diff.Absolute() <= CStatistics::Epsilon;
	}

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL is_double_comparison =
		this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
#endif	// GPOS_DEBUG
	BOOL is_lint_comparison =
		this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();

	GPOS_ASSERT(is_double_comparison || is_lint_comparison);

	if (this->IsNull())
	{
		// nulls are equal from stats point of view
		return datum->IsNull();
	}

	if (datum->IsNull())
	{
		return false;
	}

	if (is_lint_comparison)
	{
		LINT l1 = this->GetLINTMapping();
		LINT l2 = datum->GetLINTMapping();
		return l1 == l2;
	}

	GPOS_ASSERT(is_double_comparison);

	CDouble d1 = this->GetDoubleMapping();
	CDouble d2 = datum->GetDoubleMapping();
	CDouble diff = d1 - d2;
	return diff.Absolute() <= CStatistics::Epsilon;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatum::StatsAreLessThan
//
//	@doc:
//		Less-than based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
BOOL
IDatum::StatsAreLessThan(const IDatum *datum) const
{
	GPOS_ASSERT(nullptr != datum);

	if (FDateVsTimestampComparison(this, datum))
	{
		if (this->IsNull())
		{
			// nulls are less than everything else except nulls
			return !(datum->IsNull());
		}
		if (datum->IsNull())
		{
			return false;
		}
		CDouble diff = DTimestampScaleValue(datum) - DTimestampScaleValue(this);
		return diff > CStatistics::Epsilon;
	}

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL is_double_comparison =
		this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
#endif	// GPOS_DEBUG
	BOOL is_lint_comparison =
		this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();

	GPOS_ASSERT(is_double_comparison || is_lint_comparison);

	if (this->IsNull())
	{
		// nulls are less than everything else except nulls
		return !(datum->IsNull());
	}

	if (datum->IsNull())
	{
		return false;
	}

	if (is_lint_comparison)
	{
		LINT l1 = this->GetLINTMapping();
		LINT l2 = datum->GetLINTMapping();
		return l1 < l2;
	}

	GPOS_ASSERT(is_double_comparison);

	CDouble d1 = this->GetDoubleMapping();
	CDouble d2 = datum->GetDoubleMapping();
	CDouble diff = d2 - d1;
	return diff > CStatistics::Epsilon;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatum::GetStatsDistanceFrom
//
//	@doc:
//		Distance function based on mapping to LINT or CDouble
//
//---------------------------------------------------------------------------
CDouble
IDatum::GetStatsDistanceFrom(const IDatum *datum) const
{
	GPOS_ASSERT(nullptr != datum);

	if (FDateVsTimestampComparison(this, datum))
	{
		if (this->IsNull())
		{
			// nulls are equal from stats point of view
			return datum->IsNull();
		}
		if (datum->IsNull())
		{
			return false;
		}
		return DTimestampScaleValue(this) - DTimestampScaleValue(datum);
	}

	// datums can be compared based on either LINT or Doubles or BYTEA values
#ifdef GPOS_DEBUG
	BOOL is_double_comparison =
		this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
#endif	// GPOS_DEBUG
	BOOL is_lint_comparison =
		this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();

	GPOS_ASSERT(is_double_comparison || is_lint_comparison);

	if (this->IsNull())
	{
		// nulls are equal from stats point of view
		return datum->IsNull();
	}

	if (datum->IsNull())
	{
		return false;
	}

	if (is_lint_comparison)
	{
		LINT l1 = this->GetLINTMapping();
		LINT l2 = datum->GetLINTMapping();
		return l1 - l2;
	}

	GPOS_ASSERT(is_double_comparison);

	CDouble d1 = this->GetDoubleMapping();
	CDouble d2 = datum->GetDoubleMapping();
	return d1 - d2;
}

//---------------------------------------------------------------------------
//	@function:
//		IDatum::GetValAsDouble
//
//	@doc:
//		 Return double representation of mapping value
//
//---------------------------------------------------------------------------
CDouble
IDatum::GetValAsDouble() const
{
	if (IsNull())
	{
		return CDouble(0.0);
	}

	if (IsDatumMappableToLINT())
	{
		return CDouble(GetLINTMapping());
	}

	return CDouble(GetDoubleMapping());
}


//---------------------------------------------------------------------------
//	@function:
//		IDatum::StatsAreComparable
//
//	@doc:
//		Check if the given pair of datums are stats comparable
//
//---------------------------------------------------------------------------
BOOL
IDatum::StatsAreComparable(const IDatum *datum) const
{
	GPOS_ASSERT(nullptr != datum);

	BOOL is_types_match = this->MDId()->Equals(datum->MDId());

	// the statistics for different time related types can't be directly compared, eg: timestamp vs timestamp with time zone.
	// to prevent inaccurate statistics, mark as non-comparable
	if (!is_types_match)
	{
		BOOL is_time_comparison =
			CMDTypeGenericGPDB::IsTimeRelatedType(this->MDId()) &&
			CMDTypeGenericGPDB::IsTimeRelatedType(datum->MDId());
		if (is_time_comparison)
		{
			// date vs timestamp maps exactly onto a common scale;
			// other mixed time-type pairs stay non-comparable
			return FDateVsTimestampComparison(this, datum);
		}
	}
	// datums can be compared based on either LINT or Doubles or BYTEA values
	BOOL is_double_comparison =
		this->IsDatumMappableToDouble() && datum->IsDatumMappableToDouble();
	BOOL is_lint_comparison =
		this->IsDatumMappableToLINT() && datum->IsDatumMappableToLINT();

	return is_double_comparison || is_lint_comparison;
}

//EOF
