/*
 *  faultinjector.h
 *  
 *
 *  Copyright 2009-2010, Greenplum Inc. All rights reserved.
 *
 */

#ifndef FAULTINJECTOR_H
#define FAULTINJECTOR_H

#include "pg_config.h"

#define FAULTINJECTOR_MAX_SLOTS	16

#define FAULT_NAME_MAX_LENGTH	256

#define INFINITE_END_OCCURRENCE -1

#define Natts_fault_message_response 1
#define Anum_fault_message_response_status 0

/* Fault name that matches all faults */
#define FaultInjectorNameAll "all"

typedef enum FaultInjectorType_e {
#define FI_TYPE(id, str) id,
#include "utils/faultinjector_lists.h"
#undef FI_TYPE
	FaultInjectorTypeMax
} FaultInjectorType_e;

/*
 *
 */
typedef enum DDLStatement_e {
#define FI_DDL_STATEMENT(id, str) id,
#include "utils/faultinjector_lists.h"
#undef FI_DDL_STATEMENT
	DDLMax
} DDLStatement_e;

/*
 *
 */
typedef enum FaultInjectorState_e {
#define FI_STATE(id, str) id,
#include "utils/faultinjector_lists.h"
#undef FI_STATE
	FaultInjectorStateMax
} FaultInjectorState_e;

/* for DtxProtocolCommand type */
#include "postgres.h"
#include "cdb/cdbtm.h"

/*
 *
 */
typedef struct FaultInjectorEntry_s {
	
	char						faultName[FAULT_NAME_MAX_LENGTH];

	FaultInjectorType_e		faultInjectorType;
	
	int						extraArg;
		/*
		 * - in seconds, in use if fault injection type is sleep
		 * - exit code, in use if fault injection type is exit
		 */
	int						gpSessionid;
		/* -1 means the fault could be triggered by any process */

	DDLStatement_e			ddlStatement;
	
	char					databaseName[NAMEDATALEN];
	
	char					tableName[NAMEDATALEN];
	int			nestingLevel;
	volatile	int			startOccurrence;
	volatile	int			endOccurrence;
	volatile	 int	numTimesTriggered;
	volatile	FaultInjectorState_e	faultInjectorState;

		/* the state of the fault injection */
	char					bufOutput[2500];
	
} FaultInjectorEntry_s;

extern void InjectFaultInit(void);

extern Size FaultInjector_ShmemSize(void);

extern void FaultInjector_ShmemInit(void);

/*
 * To check if a fault has been injected, use FaultInjector_InjectFaultIfSet().
 * It is designed to fall through as quickly as possible, when no faults are
 * activated.
 */
extern FaultInjectorType_e FaultInjector_InjectFaultIfSet_out_of_line(
							   const char*				 faultName,
							   DDLStatement_e			 ddlStatement,
							   const char*				 databaseName,
							   const char*				 tableName,
							   int						 nestingLevel);

/* 
 * Use macro FaultInjector_InjectFaultIfSet_SQL instead of direct call
 * of this function
 */
FaultInjectorType_e FaultInjector_InjectFaultIfSet_out_of_line_SQL(
	const char*		faultName,
	const char*		statement,
	int 			nestingLevel);

/* 
 * Use macro FaultInjector_InjectFaultIfSet_DTX instead of direct call
 * of this function
 */
FaultInjectorType_e FaultInjector_InjectFaultIfSet_out_of_line_DTX(
	const char* 		faultName,
	DtxProtocolCommand	dtxProtocolCommand,
	int					nestingLevel);
#define FaultInjector_InjectFaultIfSet(faultName, ddlStatement, databaseName, tableName) \
	(((*numActiveFaults_ptr) > 0) ? \
	 FaultInjector_InjectFaultIfSet_out_of_line(faultName, ddlStatement, databaseName, tableName, 0) : \
	 FaultInjectorTypeNotSpecified)

#define FaultInjector_InjectFaultIfSet_DTX(faultName, ddlStatement, nestingLevel) \
	(((*numActiveFaults_ptr) > 0) ? \
	 FaultInjector_InjectFaultIfSet_out_of_line_DTX(faultName, ddlStatement,nestingLevel) : \
	 FaultInjectorTypeNotSpecified)
  
#define FaultInjector_InjectFaultIfSet_SQL(faultName, ddlStatement, nestingLevel) \
	(((*numActiveFaults_ptr) > 0) ? \
	 FaultInjector_InjectFaultIfSet_out_of_line_SQL(faultName, ddlStatement,nestingLevel) : \
	 FaultInjectorTypeNotSpecified)
extern int *numActiveFaults_ptr;


extern char *InjectFault(
	char *faultName, char *type, char *ddlStatement, char *databaseName,
	char *tableName, int startOccurrence, int endOccurrence, int extraArg,
	int gpSessionid, int nestingLevel);

extern void HandleFaultMessage(const char* msg);

typedef void (*fault_injection_warning_function)(FaultInjectorEntry_s faultEntry);
void register_fault_injection_warning(fault_injection_warning_function warning);

#ifdef FAULT_INJECTOR
extern bool am_faulthandler;
#define SIMPLE_FAULT_INJECTOR(FaultName) \
	FaultInjector_InjectFaultIfSet(FaultName, DDLNotSpecified, "", "")
#else
#define am_faulthandler false
#define SIMPLE_FAULT_INJECTOR(FaultName)
#endif

#endif	/* FAULTINJECTOR_H */
