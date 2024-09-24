/*
 * OracleStatement.cpp
 *
 *  Created on: 27 Feb 2015
 *      Author: GBY18020
 */

#include "OracleStatement.h"

OracleStatement::OracleStatement(string sSQL, int iNumBinds, int iNumDefines)
{
	this->sSQLStatement = sSQL;
	this->stmthp = (OCIStmt *) 0;

	if ( iNumBinds > 0)
	{
		this->pBinds = (OCIBind **) malloc(iNumBinds * sizeof(OCIBind *));
		memset(this->pBinds, 0, iNumBinds * sizeof(OCIBind *));
	}

	this->iNumBinds = iNumBinds;

	if ( iNumDefines > 0 )
	{
		this->pDefines = (OCIDefine **) malloc(iNumDefines * sizeof(OCIDefine *));
		memset(this->pDefines, 0, iNumDefines * sizeof(OCIDefine *));
	}

	this->iNumDefines = iNumDefines;
}

OracleStatement::~OracleStatement()
{
	if ( iNumBinds > 0 )
		free(this->pBinds);

	if ( iNumDefines > 0 )
		free(this->pDefines);
}
