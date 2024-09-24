/*
 * OracleStatement.h
 *
 *  Created on: 27 Feb 2015
 *      Author: GBY18020
 */

#ifndef ORACLESTATEMENT_H_
#define ORACLESTATEMENT_H_

#include <oci.h>
#include <string>

using namespace std;

#define MAX_BINDS 11
#define MAX_DEFINES 11

class OracleStatement {
	public:
		OCIStmt *stmthp;
		OCIBind **pBinds;
		OCIDefine **pDefines;
		int iNumBinds;
		int iNumDefines;

		string sSQLStatement;

		OracleStatement(string sSQL, int iNumBinds, int iNumDefines);
		virtual ~OracleStatement();
};

#endif /* ORACLESTATEMENT_H_ */
