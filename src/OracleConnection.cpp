/*
 * OracleConnection.cpp
 *
 *  Created on: Mar 7, 2013
 *      Author: gby18020
 */

#include "OracleConnection.h"
#include <cstring>
#include <cstdio>
#include "Logger.h"

char *OracleConnection::getError()
{
	text errbuf[512] = { '\0' };
	sb4 errcode = 0;
	static char returnString[600] = { '\0' };

	switch (this->oraStat)
	{
		case OCI_ERROR: /* Intentional drop through */
		case OCI_SUCCESS_WITH_INFO:
			oraStat = OCIErrorGet((dvoid *) this->errhp, (ub4) 1, (text *) NULL, &errcode, errbuf, (ub4) sizeof(errbuf),
			OCI_HTYPE_ERROR);
			sprintf(returnString, "%s", errbuf);
			break;
		case OCI_NEED_DATA:
			sprintf(returnString, "Error OCI_NEED_DATA");
			break;
		case OCI_NO_DATA:
			sprintf(returnString, "Error OCI_NO_DATA");
			break;
		case OCI_INVALID_HANDLE:
			sprintf(returnString, "Error OCI_INVALID_HANDLE");
			break;
		case OCI_STILL_EXECUTING:
			sprintf(returnString, "Error OCI_STILL_EXECUTING");
			break;
		case OCI_CONTINUE:
			sprintf(returnString, "Error OCI_CONTINUE");
			break;
		default:
			sprintf(returnString, "%d: Unknown oci return status", this->oraStat);
			break;
	}

	return returnString;
}

OracleConnection::OracleConnection()
{
	this->oraStat = OCI_SUCCESS;
	this->errhp = (OCIError *) 0;
	this->envhp = (OCIEnv *) 0;
	this->svchp = (OCISvcCtx *) 0;
	this->authp = (OCIAuthInfo *) 0;

	this->vStatements.clear();
	this->vStatements.reserve(100);
	this->bConnected = 0;
}

OracleConnection::~OracleConnection()
{
	// TODO Auto-generated destructor stub
	this->vStatements.clear();
}

int OracleConnection::Connect(string sUsername, string sPassword, string sDatabase, int bSQLTrace, string sInterface, int iThreadId)
{
	char selectst1[1024] = { '\0' };
	OCIStmt *stmthp = (OCIStmt *) 0;
	ub4 plsz = 67108864; /* 64mb - TEMP */
	ub4 mode = OCI_THREADED | OCI_OBJECT;

	//OCIAttrSet((dvoid *)0, (ub4)OCI_HTYPE_PROC, (dvoid *)&plsz, (ub4)0, (ub4) OCI_ATTR_MEMPOOL_SIZE, 0);

	this->oraStat = OCIEnvCreate(&(this->envhp), mode, (dvoid *) 0, NULL,
	NULL, NULL, 0, (dvoid **) 0);
	if (this->oraStat == OCI_SUCCESS) {
		this->oraStat = OCIHandleAlloc((dvoid *) this->envhp, (dvoid **) &(this->errhp), OCI_HTYPE_ERROR, (size_t) 0, (dvoid **) 0);

		if (this->oraStat == OCI_SUCCESS) {
			this->oraStat = OCILogon2(this->envhp, this->errhp, &(this->svchp), (OraText *) sUsername.c_str(),
					(ub4) strlen((char *) sUsername.c_str()), (OraText *) sPassword.c_str(), (ub4) strlen((char *) sPassword.c_str()),
					(OraText *) sDatabase.c_str(), (ub4) strlen((char *) sDatabase.c_str()),
					OCI_LOGON2_STMTCACHE);

			if (this->oraStat == OCI_SUCCESS) {
				this->bConnected = 1;

				if (bSQLTrace == 1) {
					/* Turn on SQL tracing */
					this->oraStat = OCIHandleAlloc(this->envhp, (dvoid **) &stmthp, OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
					if (this->oraStat == OCI_SUCCESS) {
						sprintf(selectst1, "ALTER SESSION SET EVENTS '10046 trace name context forever, level 12'");

						this->oraStat = OCIStmtPrepare(stmthp, this->errhp, (text *) selectst1, (ub4) strlen(selectst1),
						OCI_NTV_SYNTAX, OCI_DEFAULT);
						if (this->oraStat == OCI_SUCCESS) {
							this->oraStat = OCIStmtExecute(this->svchp, stmthp, this->errhp, (ub4) 1, (ub4) 0, (OCISnapshot *) 0,
									(OCISnapshot *) 0,
									OCI_DEFAULT);
							if (this->oraStat == OCI_SUCCESS) {
								this->oraStat = OCIHandleFree((dvoid *) stmthp,
								OCI_HTYPE_STMT);

								if (this->oraStat == OCI_SUCCESS) {
									/* Set trace file identifier */
									sprintf(selectst1, "ALTER SESSION SET tracefile_identifier = %s_%d_", sInterface.c_str(), iThreadId);
									this->oraStat = OCIHandleAlloc(this->envhp, (dvoid **) &stmthp, OCI_HTYPE_STMT, (size_t) 0,
											(dvoid **) 0);

									this->oraStat = OCIStmtPrepare(stmthp, this->errhp, (text *) selectst1, (ub4) strlen(selectst1),
									OCI_NTV_SYNTAX, OCI_DEFAULT);
									if (this->oraStat == OCI_SUCCESS) {
										this->oraStat = OCIStmtExecute(this->svchp, stmthp, this->errhp, (ub4) 1, (ub4) 0,
												(OCISnapshot *) 0, (OCISnapshot *) 0, OCI_DEFAULT);
										if (this->oraStat == OCI_SUCCESS) {
											this->oraStat = OCIHandleFree((dvoid *) stmthp,
											OCI_HTYPE_STMT);

										}
									}
								}
							}
						}
					}
				}
			}
			else {
				gl_error("Logon Failed : %s", this->getError());
			}
		}
		else {
			gl_error("Error handle create Failed : %s", this->getError());
		}
	}
	else {
		gl_error("Env create Failed : %s", this->getError());
	}

	return this->oraStat;
}

int OracleConnection::Disconnect(int bSQLTrace)
{
	vector<OracleStatement*>::iterator it;

	/* Close down any open statement handles */
	for (it = this->vStatements.begin(); it != this->vStatements.end(); it++) {
		oraStat = OCIHandleFree((dvoid *) (*it)->stmthp, OCI_HTYPE_STMT);
		if (oraStat != OCI_SUCCESS) {
			gl_error("Statement free failed : %s", getError());
		}

		delete (*it);
	}

	this->oraStat = OCILogoff(this->svchp, this->errhp);
	if (this->oraStat == OCI_SUCCESS) {
		this->bConnected = 0;
		this->oraStat = OCIHandleFree(this->errhp, OCI_HTYPE_ERROR);
		if (this->oraStat == OCI_SUCCESS) {
			this->oraStat = OCIHandleFree(this->envhp, OCI_HTYPE_ENV);
		}
	}

	/* COMPLETED RELEASING SESSION */
	return oraStat;
}

int OracleConnection::runSimpleStringSelect(char *pSQL, char **caSQLResult)
{
	int iExitCode = 0;
	OCIStmt *stmthp = (OCIStmt *) 0;
	OCIDefine *defhp1 = (OCIDefine *) 0;
	char caResult[MAX_SIMPLE_STRING + 1] = { '\0' };
	short ind = 0;

	gl_trace(_DETAILED, "SQL <%s>", pSQL);

	this->oraStat = OCIHandleAlloc(this->envhp, (dvoid **) &stmthp, OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
	if (this->oraStat == OCI_SUCCESS) {
		this->oraStat = OCIStmtPrepare(stmthp, this->errhp, (text *) pSQL, (ub4) strlen(pSQL), OCI_NTV_SYNTAX,
		OCI_DEFAULT);
		if (this->oraStat == OCI_SUCCESS) {
			this->oraStat = OCIDefineByPos(stmthp, &defhp1, this->errhp, (ub4) 1, (dvoid *) &caResult, (sb4) sizeof(caResult),
					(ub2) SQLT_STR, (dvoid *) &ind, (ub2 *) 0, (ub2 *) 0, OCI_DEFAULT);
			if (this->oraStat == OCI_SUCCESS) {
				this->oraStat = OCIStmtExecute(this->svchp, stmthp, this->errhp, (ub4) 0, (ub4) 0, (OCISnapshot *) 0, (OCISnapshot *) 0,
				OCI_DEFAULT);
				if (this->oraStat == OCI_SUCCESS) {
					this->oraStat = OCIStmtFetch(stmthp, this->errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
					if (this->oraStat != OCI_SUCCESS) {
						iExitCode = 1;
						gl_error("Fetch Failed : %s", this->getError());
					}
					else {
						if (ind != 0)
							caResult[0] = '\0';
					}
				}
				else {
					iExitCode = 1;
					gl_error("Execute Failed : %s", this->getError());
				}
			}
			else {
				iExitCode = 1;
				gl_error("Output Bind Failed : %s", this->getError());
			}
		}
		else {
			iExitCode = 1;
			gl_error("Statement Prepare Failed : %s", this->getError());
		}

		this->oraStat = OCIHandleFree((dvoid *) stmthp, OCI_HTYPE_STMT);
		if (this->oraStat != OCI_SUCCESS) {
			iExitCode = 1;
			gl_error("Handle Free Failed : %s", this->getError());
		}
	}
	else {
		iExitCode = 1;
		gl_error("Handle Alloc Failed : %s", this->getError());
	}

	sprintf(*caSQLResult, "%s", caResult);

	return iExitCode;
}

int OracleConnection::runSimpleStringSelect(char *pSQL)
{
	int iExitCode = 0;
	OCIStmt *stmthp = (OCIStmt *) 0;

	gl_trace(_DETAILED, "SQL <%s>", pSQL);

	this->oraStat = OCIHandleAlloc(this->envhp, (dvoid **) &stmthp, OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
	if (this->oraStat == OCI_SUCCESS) {
		this->oraStat = OCIStmtPrepare(stmthp, this->errhp, (text *) pSQL, (ub4) strlen(pSQL), OCI_NTV_SYNTAX,
		OCI_DEFAULT);
		if (this->oraStat == OCI_SUCCESS) {
			this->oraStat = OCIStmtExecute(this->svchp, stmthp, this->errhp, (ub4) 1, (ub4) 0, (OCISnapshot *) 0, (OCISnapshot *) 0,
			OCI_DEFAULT);
			if (this->oraStat != OCI_SUCCESS) {
				iExitCode = 1;
				gl_error("Execute Failed : %s", this->getError());
			}
		}
		else {
			iExitCode = 1;
			gl_error("Statement Prepare Failed : %s", this->getError());
		}

		this->oraStat = OCIHandleFree((dvoid *) stmthp, OCI_HTYPE_STMT);
		if (this->oraStat != OCI_SUCCESS) {
			iExitCode = 1;
			gl_error("Handle Free Failed : %s", this->getError());
		}
	}
	else {
		iExitCode = 1;
		gl_error("Handle Alloc Failed : %s", this->getError());
	}

	return iExitCode;
}

int OracleConnection::createSavePoint(char *pSavePointName)
{
	int iExitCode = 0;
	char caSQL[1024 + 1] = { '\0' };

	sprintf(caSQL, "SAVEPOINT %s", pSavePointName);
	iExitCode = this->runSimpleStringSelect(caSQL);

	return iExitCode;
}

int OracleConnection::rollbackToSavePoint(char *pSavePointName)
{
	int iExitCode = 0;
	char caSQL[1024 + 1] = { '\0' };

	sprintf(caSQL, "ROLLBACK TO SAVEPOINT %s", pSavePointName);
	iExitCode = this->runSimpleStringSelect(caSQL);

	return iExitCode;
}

int OracleConnection::runSimpleLongSelect(char *pSQL, long *piSQLResult)
{
	int iExitCode = 0;
	OCIStmt *stmthp = (OCIStmt *) 0;
	OCIDefine *defhp1 = (OCIDefine *) 0;
	char caResult[MAX_SIMPLE_STRING + 1] = { '\0' };
	short ind = 0;

	gl_trace(_DETAILED, "SQL <%s>", pSQL);

	this->oraStat = OCIHandleAlloc(this->envhp, (dvoid **) &stmthp, OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
	if (this->oraStat == OCI_SUCCESS) {
		this->oraStat = OCIStmtPrepare(stmthp, this->errhp, (text *) pSQL, (ub4) strlen(pSQL), OCI_NTV_SYNTAX,
		OCI_DEFAULT);
		if (this->oraStat == OCI_SUCCESS) {
			this->oraStat = OCIDefineByPos(stmthp, &defhp1, this->errhp, (ub4) 1, (dvoid *) piSQLResult, (sb4) sizeof(long), (ub2) SQLT_INT,
					(dvoid *) &ind, (ub2 *) 0, (ub2 *) 0, OCI_DEFAULT);
			if (this->oraStat == OCI_SUCCESS) {
				this->oraStat = OCIStmtExecute(this->svchp, stmthp, this->errhp, (ub4) 0, (ub4) 0, (OCISnapshot *) 0, (OCISnapshot *) 0,
				OCI_DEFAULT);
				if (this->oraStat == OCI_SUCCESS) {
					this->oraStat = OCIStmtFetch(stmthp, this->errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
					if (this->oraStat != OCI_SUCCESS) {
						iExitCode = 1;
						gl_error("Fetch Failed : %s", this->getError());
					}
					else {
						if (ind != 0)
							*piSQLResult = 0;
					}
				}
				else {
					iExitCode = 1;
					gl_error("Execute Failed : %s", this->getError());
				}
			}
			else {
				iExitCode = 1;
				gl_error("Output Bind Failed : %s", this->getError());
			}
		}
		else {
			iExitCode = 1;
			gl_error("Statement Prepare Failed : %s", this->getError());
		}

		this->oraStat = OCIHandleFree((dvoid *) stmthp, OCI_HTYPE_STMT);
		if (this->oraStat != OCI_SUCCESS) {
			iExitCode = 1;
			gl_error("Handle Free Failed : %s", this->getError());
		}
	}
	else {
		iExitCode = 1;
		gl_error("Handle Alloc Failed : %s", this->getError());
	}

	return iExitCode;
}

int OracleConnection::createCursor(char *pSQL, Delegate<int, OCIBind **, OCIStmt *> *pBindFn,
		Delegate<int, OCIDefine **, OCIStmt *> *pDefineFn, OCIStmt **ppStmthp, OCIDefine **ppDefine, OCIBind **ppBind)
{
	int iExitCode = 0;

	gl_trace(_DETAILED, "SQL <%s>", pSQL);

	this->oraStat = OCIHandleAlloc(this->envhp, (dvoid**) ppStmthp, OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);

	if (this->oraStat == OCI_SUCCESS) {
		this->oraStat = OCIStmtPrepare(*ppStmthp, this->errhp, (text *) pSQL, (ub4) strlen(pSQL), OCI_NTV_SYNTAX,
		OCI_DEFAULT);

		if (this->oraStat == OCI_SUCCESS) {
			if (pBindFn != NULL && (*pBindFn)(ppBind, *ppStmthp) != 0) {
				iExitCode = 1;
				gl_error("Binding of input to select failed");
			}

			if (pDefineFn != NULL && (*pDefineFn)(ppDefine, *ppStmthp) != 0) {
				iExitCode = 1;
				gl_error("Binding of output to select failed");
			}

			if (iExitCode == 0) {
				this->oraStat = OCIStmtExecute(this->svchp, *ppStmthp, this->errhp, (ub4) 0, (ub4) 0, (OCISnapshot *) 0, (OCISnapshot *) 0,
						OCI_DEFAULT);
				if (this->oraStat != OCI_SUCCESS) {
					gl_error("Statement Execution Failed : %s", this->getError());
					iExitCode = 1;
				}
			}
		}
		else {
			iExitCode = 1;
			gl_error("Statement Prepare Failed : %s", this->getError());
		}
	}

	return iExitCode;
}

int OracleConnection::closeCursor(OCIStmt *pStmt)
{
	int iExitCode = 0;

	this->oraStat = OCIHandleFree((dvoid *) pStmt, OCI_HTYPE_STMT);
	if (this->oraStat != OCI_SUCCESS) {
		iExitCode = 1;
		gl_error("Handle Free Failed : %s", this->getError());
	}

	return iExitCode;
}

int OracleConnection::insert(char *pSQL, Delegate<int, OCIBind **, OCIStmt *> *pBindFn, Delegate<int, OCIDefine **, OCIStmt *> *pDefineFn,
		long *lRowsInserted)
{
	int iExitCode = 0;
	OCIBind *bndhp1 = (OCIBind *) 0;
	OCIDefine *defhp1 = (OCIDefine *) 0;
	OCIStmt *stmthp = (OCIStmt *) 0;
	ub4 rows = 0;

	gl_trace(_DETAILED, "SQL <%s>", pSQL);

	this->oraStat = OCIHandleAlloc(this->envhp, (dvoid**) &stmthp, OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);

	if (this->oraStat == OCI_SUCCESS) {
		this->oraStat = OCIStmtPrepare(stmthp, this->errhp, (text *) pSQL, (ub4) strlen(pSQL), OCI_NTV_SYNTAX,
		OCI_DEFAULT);

		if (this->oraStat == OCI_SUCCESS) {
			if (pBindFn != NULL && (*pBindFn)(&bndhp1, stmthp) != 0) {
				iExitCode = 1;
				gl_error("Binding of input to select failed");
			}

			if (pDefineFn != NULL && (*pDefineFn)(&defhp1, stmthp) != 0) {
				iExitCode = 1;
				gl_error("Binding of output to select failed");
			}

			if (iExitCode == 0) {
				this->oraStat = OCIStmtExecute(this->svchp, stmthp, this->errhp, (ub4) 1, (ub4) 0, (OCISnapshot *) 0, (OCISnapshot *) 0,
						OCI_DEFAULT);
				if (this->oraStat != OCI_SUCCESS) {
					gl_error("Error during execute : %s", this->getError());
					iExitCode = 1;
				}
				else {
					this->oraStat = OCIAttrGet(stmthp, OCI_HTYPE_STMT, &rows, 0, OCI_ATTR_ROW_COUNT, this->errhp);

					*lRowsInserted = rows;
				}
			}
		}
		else {
			iExitCode = 1;
			gl_error("Statement Prepare Failed : %s", this->getError());
		}

		this->oraStat = OCIHandleFree((dvoid *) stmthp, OCI_HTYPE_STMT);
		if (this->oraStat != OCI_SUCCESS) {
			iExitCode = 1;
			gl_error("Handle Free Failed : %s", this->getError());
		}
	}

	return iExitCode;
}

int OracleConnection::select(char *pSQL, Delegate<int, OCIBind **, OCIStmt *> *pBindFn, Delegate<int, OCIDefine **, OCIStmt *> *pDefineFn)
{
	int iExitCode = 0;
	OCIBind *bndhp1 = (OCIBind *) 0;
	OCIDefine *defhp1 = (OCIDefine *) 0;
	OCIStmt *stmthp = (OCIStmt *) 0;

	gl_trace(_DETAILED, "SQL <%s>", pSQL);

	this->oraStat = OCIHandleAlloc(this->envhp, (dvoid**) &stmthp, OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);

	if (this->oraStat == OCI_SUCCESS) {
		this->oraStat = OCIStmtPrepare(stmthp, this->errhp, (text *) pSQL, (ub4) strlen(pSQL), OCI_NTV_SYNTAX,
		OCI_DEFAULT);

		if (this->oraStat == OCI_SUCCESS) {
			if (pBindFn != NULL && (*pBindFn)(&bndhp1, stmthp) != 0) {
				iExitCode = 1;
				gl_error("Binding of input to select failed");
			}

			if (pDefineFn != NULL && (*pDefineFn)(&defhp1, stmthp) != 0) {
				iExitCode = 1;
				gl_error("Binding of output to select failed");
			}

			if (iExitCode == 0) {
				this->oraStat = OCIStmtExecute(this->svchp, stmthp, this->errhp, (ub4) 0, (ub4) 0, (OCISnapshot *) 0, (OCISnapshot *) 0,
						OCI_DEFAULT);
				if (this->oraStat == OCI_SUCCESS) {
					this->oraStat = OCIStmtFetch(stmthp, this->errhp, 1, OCI_FETCH_NEXT, OCI_DEFAULT);
					if (this->oraStat != OCI_SUCCESS && this->oraStat != OCI_NO_DATA) {
						gl_error("Error during select : %s", this->getError());
						iExitCode = 1;
					}

					if (this->oraStat == OCI_NO_DATA)
						iExitCode = this->oraStat;
				}
				else {
					gl_error("Error during execute : %s", this->getError());
					iExitCode = 1;
				}
			}
		}
		else {
			iExitCode = 1;
			gl_error("Statement Prepare Failed : %s", this->getError());
		}

		this->oraStat = OCIHandleFree((dvoid *) stmthp, OCI_HTYPE_STMT);
		if (this->oraStat != OCI_SUCCESS) {
			iExitCode = 1;
			gl_error("Handle Free Failed : %s", this->getError());
		}
	}

	return iExitCode;
}

int OracleConnection::getOracleStatement(int * piStatementId, OracleStatement **ppOraStatement, string sSQLStatement, int iNumBinds,
		int iNumDefines)
{
	int iExitCode = 0;
	OracleStatement *pOraStmt = NULL;

	if (*piStatementId == -1) {
		gl_trace(_DETAILED, "Adding new statement to cache");
		// new statement
		pOraStmt = new OracleStatement(sSQLStatement, iNumBinds, iNumDefines);

		oraStat = OCIHandleAlloc(this->envhp, (dvoid **) &(pOraStmt->stmthp),
		OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);
		if (oraStat == OCI_SUCCESS) {
			oraStat = OCIStmtPrepare(pOraStmt->stmthp, this->errhp, (text *) pOraStmt->sSQLStatement.c_str(),
					(ub4) strlen(pOraStmt->sSQLStatement.c_str()), OCI_NTV_SYNTAX,
					OCI_DEFAULT);
			if (oraStat != OCI_SUCCESS) {
				gl_error("Error preparing statement: %s", this->getError());
				iExitCode = 1;
			}
			else {
				this->vStatements.push_back(pOraStmt);
				*ppOraStatement = this->vStatements.back();
				*piStatementId = this->vStatements.size() - 1; // it will be used as an array pos hence -1
			}
		}
		else {
			gl_error("Error allocating statement: %s", this->getError());
			iExitCode = 1;
		}
	}
	else {
		gl_trace(_DETAILED, "Reusing statement from cache");
		// return statement from the cache
		if ((*piStatementId) < 0 || (*piStatementId) > this->vStatements.size()) {
			gl_error("Error statement id <%d> is not valid", (*piStatementId));
			iExitCode = 1;
		}
		else
			*ppOraStatement = this->vStatements.at((*piStatementId));

	}

	return iExitCode;
}

int OracleConnection::bulkInsert(char *pSQL, Delegate<int, OCIBind **, OCIStmt *> *pBindFn,
		Delegate<int, OCIDefine **, OCIStmt *> *pDefineFn, long lNumRowsToInsert, long *lRowsInserted)
{
	int iExitCode = 0;
	OCIBind *bndhp1 = (OCIBind *) 0;
	OCIDefine *defhp1 = (OCIDefine *) 0;
	OCIStmt *stmthp = (OCIStmt *) 0;
	ub4 rows = 0;

	gl_trace(_DETAILED, "SQL <%s>", pSQL);

	this->oraStat = OCIHandleAlloc(this->envhp, (dvoid**) &stmthp, OCI_HTYPE_STMT, (size_t) 0, (dvoid **) 0);

	if (this->oraStat == OCI_SUCCESS) {
		this->oraStat = OCIStmtPrepare(stmthp, this->errhp, (text *) pSQL, (ub4) strlen(pSQL), OCI_NTV_SYNTAX,
		OCI_DEFAULT);

		if (this->oraStat == OCI_SUCCESS) {
			if (pBindFn != NULL && (*pBindFn)(&bndhp1, stmthp) != 0) {
				iExitCode = 1;
				gl_error("Binding of input to select failed");
			}

			if (pDefineFn != NULL && (*pDefineFn)(&defhp1, stmthp) != 0) {
				iExitCode = 1;
				gl_error("Binding of output to select failed");
			}

			if (iExitCode == 0) {
				this->oraStat = OCIStmtExecute(this->svchp, stmthp, this->errhp, (ub4) lNumRowsToInsert, (ub4) 0, (OCISnapshot *) 0,
						(OCISnapshot *) 0, OCI_DEFAULT);
				if (this->oraStat != OCI_SUCCESS) {
					gl_error("Error during execute : %s", this->getError());
					iExitCode = 1;
				}
				else {
					this->oraStat = OCIAttrGet(stmthp, OCI_HTYPE_STMT, &rows, 0, OCI_ATTR_ROW_COUNT, this->errhp);

					*lRowsInserted = rows;
				}
			}
		}
		else {
			iExitCode = 1;
			gl_error("Statement Prepare Failed : %s", this->getError());
		}

		this->oraStat = OCIHandleFree((dvoid *) stmthp, OCI_HTYPE_STMT);
		if (this->oraStat != OCI_SUCCESS) {
			iExitCode = 1;
			gl_error("Handle Free Failed : %s", this->getError());
		}
	}

	return iExitCode;
}
