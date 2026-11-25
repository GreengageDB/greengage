//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2018 VMware, Inc. or its affiliates.
//
//	@filename:
//		CHistogramTest.cpp
//
//	@doc:
//		Testing operations on histogram objects
//---------------------------------------------------------------------------

#ifndef __STDC_CONSTANT_MACROS
#define __STDC_CONSTANT_MACROS
#endif

#include "unittest/dxl/statistics/CHistogramTest.h"

#include <stdint.h>

#include "gpos/error/CAutoTrace.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CPoint.h"

#include "unittest/base.h"
#include "unittest/dxl/statistics/CCardinalityTestUtils.h"
#include "unittest/gpopt/CTestUtils.h"

using namespace gpopt;

// unittest for statistics objects
GPOS_RESULT
CHistogramTest::EresUnittest()
{
	// tests that use shared optimization context
	CUnittest rgutSharedOptCtxt[] = {
		GPOS_UNITTEST_FUNC(CHistogramTest::EresUnittest_CHistogramInt4),
		GPOS_UNITTEST_FUNC(CHistogramTest::EresUnittest_CHistogramBool),
		GPOS_UNITTEST_FUNC(CHistogramTest::EresUnittest_Skew),
		GPOS_UNITTEST_FUNC(CHistogramTest::EresUnittest_CHistogramValid),
		GPOS_UNITTEST_FUNC(CHistogramTest::EresUnittest_MergeUnion),
		GPOS_UNITTEST_FUNC(
			CHistogramTest::EresUnittest_MergeUnionDoubleLessThanEpsilon),
		GPOS_UNITTEST_FUNC(CHistogramTest::EresUnittest_ContainsRange)};


	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(mp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc(mp, &mda, nullptr /* pceeval */,
					 CTestUtils::GetCostModel(mp));

	return CUnittest::EresExecute(rgutSharedOptCtxt,
								  GPOS_ARRAY_SIZE(rgutSharedOptCtxt));
}

// histogram of int4
GPOS_RESULT
CHistogramTest::EresUnittest_CHistogramInt4()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// original histogram
	CHistogram *histogram = CCardinalityTestUtils::PhistExampleInt4(mp);
	CCardinalityTestUtils::PrintHist(mp, "histogram", histogram);

	// test edge case of MakeBucketGreaterThan
	CPoint *ppoint0 = CTestUtils::PpointInt4(mp, 9);
	CHistogram *phist0 =
		histogram->MakeHistogramFilter(CStatsPred::EstatscmptG, ppoint0);
	CCardinalityTestUtils::PrintHist(mp, "phist0", phist0);
	GPOS_UNITTEST_ASSERT(phist0->GetNumBuckets() == 9);

	CPoint *point1 = CTestUtils::PpointInt4(mp, 35);
	CHistogram *histogram1 =
		histogram->MakeHistogramFilter(CStatsPred::EstatscmptL, point1);
	CCardinalityTestUtils::PrintHist(mp, "histogram1", histogram1);
	GPOS_UNITTEST_ASSERT(histogram1->GetNumBuckets() == 4);

	// edge case where point is equal to upper bound
	CPoint *point2 = CTestUtils::PpointInt4(mp, 50);
	CHistogram *histogram2 =
		histogram->MakeHistogramFilter(CStatsPred::EstatscmptL, point2);
	CCardinalityTestUtils::PrintHist(mp, "histogram2", histogram2);
	GPOS_UNITTEST_ASSERT(histogram2->GetNumBuckets() == 5);

	// equality check
	CPoint *point3 = CTestUtils::PpointInt4(mp, 100);
	CHistogram *phist3 =
		histogram->MakeHistogramFilter(CStatsPred::EstatscmptEq, point3);
	CCardinalityTestUtils::PrintHist(mp, "phist3", phist3);
	GPOS_UNITTEST_ASSERT(phist3->GetNumBuckets() == 1);

	// normalized output after filter
	CPoint *ppoint4 = CTestUtils::PpointInt4(mp, 100);
	CDouble scale_factor(0.0);
	CHistogram *phist4 = histogram->MakeHistogramFilterNormalize(
		CStatsPred::EstatscmptEq, ppoint4, &scale_factor);
	CCardinalityTestUtils::PrintHist(mp, "phist4", phist4);
	GPOS_UNITTEST_ASSERT(phist4->IsValid());

	// lasj
	CHistogram *phist5 =
		histogram->MakeLASJHistogram(CStatsPred::EstatscmptEq, histogram2);
	CCardinalityTestUtils::PrintHist(mp, "phist5", phist5);
	GPOS_UNITTEST_ASSERT(phist5->GetNumBuckets() == 5);

	// inequality check
	CHistogram *phist6 =
		histogram->MakeHistogramFilter(CStatsPred::EstatscmptNEq, point2);
	CCardinalityTestUtils::PrintHist(mp, "phist6", phist6);
	GPOS_UNITTEST_ASSERT(phist6->GetNumBuckets() == 10);

	// histogram with null fraction and remaining tuples
	CHistogram *phist7 = PhistExampleInt4Remain(mp);
	CCardinalityTestUtils::PrintHist(mp, "phist7", phist7);
	CPoint *ppoint5 = CTestUtils::PpointInt4(mp, 20);

	// equality check, hitting remaining tuples
	CHistogram *phist8 =
		phist7->MakeHistogramFilter(CStatsPred::EstatscmptEq, point3);
	GPOS_UNITTEST_ASSERT(fabs((phist8->GetFrequency() - 0.2).Get()) <
						 CStatistics::Epsilon);
	GPOS_UNITTEST_ASSERT(fabs((phist8->GetNumDistinct() - 1.0).Get()) <
						 CStatistics::Epsilon);

	// greater than, hitting remaining tuples
	CHistogram *phist9 =
		phist7->MakeHistogramFilter(CStatsPred::EstatscmptG, point1);
	CCardinalityTestUtils::PrintHist(mp, "phist9", phist9);
	GPOS_UNITTEST_ASSERT(fabs((phist9->GetFrequency() - 0.26).Get()) <
						 CStatistics::Epsilon);
	GPOS_UNITTEST_ASSERT(fabs((phist9->GetNumDistinct() - 1.8).Get()) <
						 CStatistics::Epsilon);

	// equality join, hitting remaining tuples
	CHistogram *phist10 =
		phist7->MakeJoinHistogram(CStatsPred::EstatscmptEq, phist7);
	GPOS_UNITTEST_ASSERT(phist10->GetNumBuckets() == 5);
	GPOS_UNITTEST_ASSERT(fabs((phist10->GetDistinctRemain() - 2.0).Get()) <
						 CStatistics::Epsilon);
	GPOS_UNITTEST_ASSERT(fabs((phist10->GetFreqRemain() - 0.08).Get()) <
						 CStatistics::Epsilon);

	// clean up
	ppoint0->Release();
	point1->Release();
	point2->Release();
	point3->Release();
	ppoint4->Release();
	ppoint5->Release();
	GPOS_DELETE(histogram);
	GPOS_DELETE(phist0);
	GPOS_DELETE(histogram1);
	GPOS_DELETE(histogram2);
	GPOS_DELETE(phist3);
	GPOS_DELETE(phist4);
	GPOS_DELETE(phist5);
	GPOS_DELETE(phist6);
	GPOS_DELETE(phist7);
	GPOS_DELETE(phist8);
	GPOS_DELETE(phist9);
	GPOS_DELETE(phist10);

	return GPOS_OK;
}

// histogram on bool
GPOS_RESULT
CHistogramTest::EresUnittest_CHistogramBool()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// generate histogram of the form [false, false), [true,true)
	CBucketArray *histogram_buckets = GPOS_NEW(mp) CBucketArray(mp);
	CBucket *pbucketFalse =
		CCardinalityTestUtils::PbucketSingletonBoolVal(mp, false, 0.1);
	CBucket *pbucketTrue =
		CCardinalityTestUtils::PbucketSingletonBoolVal(mp, false, 0.9);
	histogram_buckets->Append(pbucketFalse);
	histogram_buckets->Append(pbucketTrue);
	CHistogram *histogram = GPOS_NEW(mp) CHistogram(mp, histogram_buckets);

	// equality check
	CPoint *point1 = CTestUtils::PpointBool(mp, false);
	CDouble scale_factor(0.0);
	CHistogram *histogram1 = histogram->MakeHistogramFilterNormalize(
		CStatsPred::EstatscmptEq, point1, &scale_factor);
	CCardinalityTestUtils::PrintHist(mp, "histogram1", histogram1);
	GPOS_UNITTEST_ASSERT(histogram1->GetNumBuckets() == 1);

	// clean up
	point1->Release();
	GPOS_DELETE(histogram);
	GPOS_DELETE(histogram1);

	return GPOS_OK;
}


// check for well-formed histogram. Expected to fail
GPOS_RESULT
CHistogramTest::EresUnittest_CHistogramValid()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	CBucketArray *histogram_buckets = GPOS_NEW(mp) CBucketArray(mp);

	// generate histogram of the form [0, 10), [9, 20)
	CBucket *bucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 0, 10, 0.1, 2.0);
	histogram_buckets->Append(bucket1);
	CBucket *bucket2 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 9, 20, 0.1, 2.0);
	histogram_buckets->Append(bucket2);

	// original histogram
	CHistogram *histogram = GPOS_NEW(mp) CHistogram(mp, histogram_buckets);

	// create an auto object
	CAutoP<CHistogram> ahist;
	ahist = histogram;

	{
		CAutoTrace at(mp);
		at.Os() << std::endl << "Invalid Histogram" << std::endl;
		histogram->OsPrint(at.Os());
	}

	if (histogram->IsValid())
	{
		return GPOS_FAILED;
	}

	return GPOS_OK;
}

// generates example int histogram having tuples not covered by buckets,
// including null fraction and nDistinctRemain
CHistogram *
CHistogramTest::PhistExampleInt4Remain(CMemoryPool *mp)
{
	// generate histogram of the form [0, 0], [10, 10], [20, 20] ...
	CBucketArray *histogram_buckets = GPOS_NEW(mp) CBucketArray(mp);
	for (ULONG idx = 0; idx < 5; idx++)
	{
		INT iLower = INT(idx * 10);
		INT iUpper = iLower;
		CDouble frequency(0.1);
		CDouble distinct(1.0);
		CBucket *bucket = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
			mp, iLower, iUpper, frequency, distinct);
		histogram_buckets->Append(bucket);
	}

	return GPOS_NEW(mp)
		CHistogram(mp, histogram_buckets, true, 0.1 /*null_freq*/,
				   2.0 /*distinct_remaining*/, 0.4 /*freq_remaining*/);
}

// basis skew test
GPOS_RESULT
CHistogramTest::EresUnittest_Skew()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	CBucket *bucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 1, 100, CDouble(0.6), CDouble(100.0));
	CBucket *bucket2 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 101, 200, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket3 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 201, 300, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket4 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 301, 400, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket5 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 401, 500, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket6 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 501, 600, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket7 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 601, 700, CDouble(0.2), CDouble(100.0));
	CBucket *pbucket8 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 701, 800, CDouble(0.2), CDouble(100.0));

	CBucketArray *pdrgppbucket1 = GPOS_NEW(mp) CBucketArray(mp);
	pdrgppbucket1->Append(bucket1);
	pdrgppbucket1->Append(bucket2);
	pdrgppbucket1->Append(pbucket3);
	CHistogram *histogram1 = GPOS_NEW(mp) CHistogram(mp, pdrgppbucket1);

	CBucketArray *pdrgppbucket2 = GPOS_NEW(mp) CBucketArray(mp);
	pdrgppbucket2->Append(pbucket4);
	pdrgppbucket2->Append(pbucket5);
	pdrgppbucket2->Append(pbucket6);
	pdrgppbucket2->Append(pbucket7);
	pdrgppbucket2->Append(pbucket8);
	CHistogram *histogram2 = GPOS_NEW(mp) CHistogram(mp, pdrgppbucket2);
	GPOS_UNITTEST_ASSERT(histogram1->GetSkew() > histogram2->GetSkew());

	{
		CAutoTrace at(mp);
		histogram1->OsPrint(at.Os());
		histogram2->OsPrint(at.Os());
	}

	GPOS_DELETE(histogram1);
	GPOS_DELETE(histogram2);

	return GPOS_OK;
}

// basic merge commutativity test
GPOS_RESULT
CHistogramTest::EresUnittest_MergeUnion()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// 1000 rows
	CBucket *bucket1 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 1, 100, CDouble(0.6), CDouble(100.0));
	CBucket *bucket2 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 150, 200, CDouble(0.4), CDouble(100.0));

	// 600 rows
	CBucket *pbucket3 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 50, 150, CDouble(0.3), CDouble(100.0));
	CBucket *pbucket4 = CCardinalityTestUtils::PbucketIntegerClosedLowerBound(
		mp, 200, 400, CDouble(0.7), CDouble(100.0));

	CBucketArray *pdrgppbucket1 = GPOS_NEW(mp) CBucketArray(mp);
	pdrgppbucket1->Append(bucket1);
	pdrgppbucket1->Append(bucket2);
	CHistogram *histogram1 = GPOS_NEW(mp) CHistogram(mp, pdrgppbucket1);

	CBucketArray *pdrgppbucket2 = GPOS_NEW(mp) CBucketArray(mp);
	pdrgppbucket2->Append(pbucket3);
	pdrgppbucket2->Append(pbucket4);
	CHistogram *histogram2 = GPOS_NEW(mp) CHistogram(mp, pdrgppbucket2);

	CDouble output_rows1(0.0);
	CHistogram *result1 = histogram1->MakeUnionHistogramNormalize(
		1000, histogram2, 600, &output_rows1);


	CDouble output_rows2(0.0);
	CHistogram *result2 = histogram2->MakeUnionHistogramNormalize(
		600, histogram1, 1000, &output_rows2);

	{
		CAutoTrace at(mp);
		result1->OsPrint(at.Os());
		result2->OsPrint(at.Os());
		at.Os() << "Result 1: " << output_rows1 << std::endl;
		at.Os() << "Result 2: " << output_rows2 << std::endl;
	}

	GPOS_UNITTEST_ASSERT(output_rows1 == output_rows2);
	GPOS_DELETE(histogram1);
	GPOS_DELETE(histogram2);
	GPOS_DELETE(result1);
	GPOS_DELETE(result2);

	return GPOS_OK;
}

// merge union test with double values differing by less than epsilon
GPOS_RESULT
CHistogramTest::EresUnittest_MergeUnionDoubleLessThanEpsilon()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// [631.82140700000002, 631.82140700000002]
	CPoint *ppLower1 = CCardinalityTestUtils::PpointDouble(
		mp, GPDB_FLOAT8, CDouble(631.82140700000002));
	CPoint *ppUpper1 = CCardinalityTestUtils::PpointDouble(
		mp, GPDB_FLOAT8, CDouble(631.82140700000002));

	CBucket *bucket1 = GPOS_NEW(mp)
		CBucket(ppLower1, ppUpper1, true /* is_lower_closed */,
				true /*is_upper_closed*/, CDouble(0.2), CDouble(50));

	// (631.82140500000003, 645.05197699999997)
	CPoint *ppLower2 = CCardinalityTestUtils::PpointDouble(
		mp, GPDB_FLOAT8, CDouble(631.82140500000003));
	CPoint *ppUpper2 = CCardinalityTestUtils::PpointDouble(
		mp, GPDB_FLOAT8, CDouble(645.05197699999997));
	CBucket *bucket2 = GPOS_NEW(mp)
		CBucket(ppLower2, ppUpper2, false /* is_lower_closed */,
				false /*is_upper_closed*/, CDouble(0.2), CDouble(50));

	CBucketArray *pdrgppbucket1 = GPOS_NEW(mp) CBucketArray(mp);
	pdrgppbucket1->Append(bucket1);
	CHistogram *histogram1 = GPOS_NEW(mp) CHistogram(mp, pdrgppbucket1);

	CBucketArray *pdrgppbucket2 = GPOS_NEW(mp) CBucketArray(mp);
	pdrgppbucket2->Append(bucket2);
	CHistogram *histogram2 = GPOS_NEW(mp) CHistogram(mp, pdrgppbucket2);

	CDouble output_rows1(0.0);
	CHistogram *result1 = histogram1->MakeUnionHistogramNormalize(
		1000, histogram2, 600, &output_rows1);


	{
		CAutoTrace at(mp);
		result1->OsPrint(at.Os());
		at.Os() << "Result 1: " << output_rows1 << std::endl;
	}

	GPOS_DELETE(histogram1);
	GPOS_DELETE(histogram2);
	GPOS_DELETE(result1);

	return GPOS_OK;
}

// range containment has to honor open and closed bucket bounds
GPOS_RESULT
CHistogramTest::EresUnittest_ContainsRange()
{
	// create memory pool
	CAutoMemoryPool amp;
	CMemoryPool *mp = amp.Pmp();

	// bounds of a single integer bucket
	struct SBucketBounds
	{
		INT m_lower;
		INT m_upper;
		BOOL m_is_lower_closed;
		BOOL m_is_upper_closed;
	};

	const ULONG max_buckets = 3;
	struct STestCase
	{
		// buckets of the histogram that has to contain the other one
		ULONG m_num_buckets;
		SBucketBounds m_buckets[max_buckets];

		// buckets of the histogram to be contained
		ULONG m_num_other_buckets;
		SBucketBounds m_other_buckets[max_buckets];

		BOOL m_is_contained;
	};

	const STestCase test_cases[] = {
		// same bounds
		{1, {{1, 10, true, true}}, 1, {{1, 10, true, true}}, true},
		{1, {{1, 10, false, false}}, 1, {{1, 10, false, false}}, true},
		// a bound is contained only if it is included wherever other has it
		{1, {{1, 10, false, false}}, 1, {{1, 10, true, true}}, false},
		{1, {{1, 10, false, true}}, 1, {{1, 10, true, true}}, false},
		{1, {{1, 10, true, false}}, 1, {{1, 10, true, true}}, false},
		{1, {{1, 10, true, true}}, 1, {{1, 10, false, false}}, true},
		// a wider bucket covers a closed bound in any case
		{1, {{0, 11, false, false}}, 1, {{1, 10, true, true}}, true},
		// adjacent buckets have to include their common bound
		{2,
		 {{1, 5, true, false}, {5, 10, true, true}},
		 1,
		 {{1, 10, true, true}},
		 true},
		{2,
		 {{1, 5, true, true}, {5, 10, false, true}},
		 1,
		 {{1, 10, true, true}},
		 true},
		{2,
		 {{1, 5, true, false}, {5, 10, false, true}},
		 1,
		 {{1, 10, true, true}},
		 false},
		{3,
		 {{1, 5, true, false}, {5, 5, true, true}, {5, 10, false, true}},
		 1,
		 {{1, 10, true, true}},
		 true},
		// unless other does not have it either
		{2,
		 {{1, 5, true, false}, {5, 10, false, true}},
		 2,
		 {{1, 5, true, false}, {5, 10, false, true}},
		 true},
		// a gap is fine only where other has a gap too
		{2,
		 {{1, 3, true, true}, {7, 10, true, true}},
		 2,
		 {{1, 3, true, true}, {7, 10, true, true}},
		 true},
		{2,
		 {{1, 3, true, true}, {7, 10, true, true}},
		 1,
		 {{1, 10, true, true}},
		 false},
		// a bucket ending where other starts does not reach into it
		{1, {{0, 5, true, true}}, 1, {{5, 10, false, true}}, false},
		{2,
		 {{0, 5, true, true}, {5, 10, false, true}},
		 1,
		 {{5, 10, false, true}},
		 true},
		// singleton buckets
		{1, {{1, 10, true, true}}, 1, {{5, 5, true, true}}, true},
		{1, {{1, 5, true, true}}, 1, {{5, 5, true, true}}, true},
		{1, {{1, 5, true, false}}, 1, {{5, 5, true, true}}, false},
		{1, {{5, 10, false, true}}, 1, {{5, 5, true, true}}, false},
		{1, {{5, 5, true, true}}, 1, {{5, 5, true, true}}, true},
		{1, {{5, 5, true, true}}, 1, {{5, 10, true, true}}, false},
	};

	GPOS_RESULT eres = GPOS_OK;
	const ULONG num_test_cases = GPOS_ARRAY_SIZE(test_cases);
	for (ULONG ul = 0; ul < num_test_cases; ul++)
	{
		const STestCase &test_case = test_cases[ul];

		CBucketArray *buckets = GPOS_NEW(mp) CBucketArray(mp);
		for (ULONG ul_bucket = 0; ul_bucket < test_case.m_num_buckets;
			 ul_bucket++)
		{
			const SBucketBounds &bounds = test_case.m_buckets[ul_bucket];
			buckets->Append(CCardinalityTestUtils::PbucketInteger(
				mp, bounds.m_lower, bounds.m_upper, bounds.m_is_lower_closed,
				bounds.m_is_upper_closed, CDouble(0.1), CDouble(1.0)));
		}
		CHistogram *histogram = GPOS_NEW(mp) CHistogram(mp, buckets);

		CBucketArray *other_buckets = GPOS_NEW(mp) CBucketArray(mp);
		for (ULONG ul_bucket = 0; ul_bucket < test_case.m_num_other_buckets;
			 ul_bucket++)
		{
			const SBucketBounds &bounds = test_case.m_other_buckets[ul_bucket];
			other_buckets->Append(CCardinalityTestUtils::PbucketInteger(
				mp, bounds.m_lower, bounds.m_upper, bounds.m_is_lower_closed,
				bounds.m_is_upper_closed, CDouble(0.1), CDouble(1.0)));
		}
		CHistogram *other_histogram =
			GPOS_NEW(mp) CHistogram(mp, other_buckets);

		BOOL is_contained = histogram->ContainsRange(other_histogram);
		if (is_contained != test_case.m_is_contained)
		{
			CAutoTrace at(mp);
			at.Os() << "Test case " << ul << ": expected "
					<< test_case.m_is_contained << ", got " << is_contained
					<< std::endl;
			histogram->OsPrint(at.Os());
			other_histogram->OsPrint(at.Os());
			eres = GPOS_FAILED;
		}

		GPOS_DELETE(histogram);
		GPOS_DELETE(other_histogram);
	}

	return eres;
}
// EOF
