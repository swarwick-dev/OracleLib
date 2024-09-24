/*
 * OracleConnection.h
 *
 *  Created on: Mar 7, 2013
 *      Author: gby18020
 */

#ifndef ORACLECONNECTION_H_
#define ORACLECONNECTION_H_

#include <oci.h>
#include <string>
#include <vector>
#include "Delegate.h"
#include "OracleStatement.h"
using namespace std;

#define MAX_SIMPLE_STRING 2048

class OracleConnection {
	public:
		sword oraStat;
		OCIError *errhp;
		OCIEnv *envhp;
		OCISvcCtx *svchp;
		OCIAuthInfo *authp;

		vector<OracleStatement *> vStatements;
		int bConnected;

		OracleConnection();
		~OracleConnection();
		int Connect(string sUsername, string sPassword, string sDatabase, int bSQLTrace, string sInterface, int iThreadId);
		int Disconnect(int bSQLTrace);
		char *getError();
		int runSimpleStringSelect(char *pSQL, char **caSQLResult);
		int runSimpleStringSelect(char *pSQL);
		int createSavePoint(char *pSavePointName);
		int rollbackToSavePoint(char *pSavePointName);
		int runSimpleLongSelect(char *pSQL, long *piSQLResult);
		int createCursor(char *pSQL, Delegate<int, OCIBind **, OCIStmt *> *pBindFn, Delegate<int, OCIDefine **, OCIStmt *> *pDefineFn,
				OCIStmt **ppStmthp, OCIDefine **ppDefine, OCIBind **ppBind);
		int insert(char *pSQL, Delegate<int, OCIBind **, OCIStmt *> *pBindFn, Delegate<int, OCIDefine **, OCIStmt *> *pDefineFn,
				long *lRowsUpdated);
		int select(char *pSQL, Delegate<int, OCIBind **, OCIStmt *> *pBindFn, Delegate<int, OCIDefine **, OCIStmt *> *pDefineFn);
		int closeCursor(OCIStmt *pStmthp);
		int forceNoParallelism();
		int getOracleStatement(int *piStatementId, OracleStatement **ppOraStatement, string sSQLStatement, int iNumBinds, int iNumDefines);
		int bulkInsert(char *pSQL, Delegate<int, OCIBind **, OCIStmt *> *pBindFn, Delegate<int, OCIDefine **, OCIStmt *> *pDefineFn,
				long lNumRowsToInsert, long *lRowsInserted);
};

#endif /* ORACLECONNECTION_H_ */
