/*
 * OracleQueue.cpp
 *
 *  Created on: 22 May 2013
 *      Author: GBY18020
 */

#include "OracleQueue.h"
#include "Logger.h"

OracleQueue::OracleQueue(OracleConnection *oraConn, char *pcQueueName, char *pcPayload, char *pcInterface, BOOL bMultiConsumer) {
	this->pOraConn = oraConn;

	this->mesg_tdo = (OCIType *) 0;
	this->ind = 0;
	this->indptr = (dvoid *)&ind;
	this->dequeue_options = (OCIAQDeqOptions *)0;
	this->wait = OCI_DEQ_NO_WAIT;
	this->deqmode = OCI_DEQ_REMOVE;
	this->visibility = OCI_DEQ_ON_COMMIT;

	this->sQueueName = pcQueueName;
	this->sPayloadType = pcPayload;
	this->sInterface = pcInterface;
	this->bMultiConsumer = bMultiConsumer;

}

int OracleQueue::setupDequeue()
{
	int iExitCode = SUCCESS;

	/* allocate dequeue options descriptor to set the dequeue options */
	OCIDescriptorAlloc(this->pOraConn->envhp, (dvoid **)&this->dequeue_options, OCI_DTYPE_AQDEQ_OPTIONS, 0,
	                     (dvoid **)0);

	if ( this->bMultiConsumer == TRUE )
	{
		/* set the current interface as the consumer name */
		OCIAttrSet(dequeue_options, (ub4)OCI_DTYPE_AQDEQ_OPTIONS,
				   (dvoid *)this->sInterface.c_str(), (ub4)strlen(this->sInterface.c_str()),
				   OCI_ATTR_CONSUMER_NAME, this->pOraConn->errhp);
		 if (this->pOraConn->oraStat != OCI_SUCCESS)
		 {
			 gl_error(" Error setting consumer name : %s\n", this->pOraConn->getError());
			 iExitCode = FAILURE;
		 }
	}

	 /* set no wait so if the queue is empty we stop */
	 OCIAttrSet(dequeue_options, OCI_DTYPE_AQDEQ_OPTIONS, (dvoid *)&this->wait, 0,
				  OCI_ATTR_WAIT, this->pOraConn->errhp);
	 if (this->pOraConn->oraStat != OCI_SUCCESS)
	 {
		  gl_error(" Error setting queue wait time : %s\n", this->pOraConn->getError());
		  iExitCode = FAILURE;
	 }

	 OCIAttrSet(dequeue_options, OCI_DTYPE_AQDEQ_OPTIONS, (dvoid *)&this->deqmode, 0,
				  OCI_ATTR_DEQ_MODE, this->pOraConn->errhp);
	 if (this->pOraConn->oraStat != OCI_SUCCESS)
	 {
		 gl_error(" Error setting dequeue mode : %s\n", this->pOraConn->getError());
		 iExitCode = FAILURE;
	 }

	 OCIAttrSet(dequeue_options, OCI_DTYPE_AQDEQ_OPTIONS, (dvoid *)&this->visibility, 0,
			 OCI_ATTR_VISIBILITY, this->pOraConn->errhp);
	 if (this->pOraConn->oraStat != OCI_SUCCESS)
	 {
		 gl_error(" Error setting visibility : %s\n", this->pOraConn->getError());
		 iExitCode = FAILURE;
	 }

	/* Obtain the TDO of the data type - ASSUMES IT BELONGS TO SYS */
	this->pOraConn->oraStat = OCITypeByName(this->pOraConn->envhp, this->pOraConn->errhp, this->pOraConn->svchp,
			(CONST text *)"SYS", strlen("SYS"),
			(CONST text *) this->sPayloadType.c_str(), strlen(this->sPayloadType.c_str()),
			(text *)0, 0, OCI_DURATION_SESSION, OCI_TYPEGET_ALL, &this->mesg_tdo);
	if (this->pOraConn->oraStat != OCI_SUCCESS)
	{
		gl_error(" Error getting type : %s", this->pOraConn->getError());
		iExitCode = FAILURE;
	}

	return iExitCode;
}

int OracleQueue::setupEnqueue()
{
	int iExitCode = SUCCESS;

	/* Obtain the TDO of the data type - ASSUMES IT BELONGS TO SYS */
	this->pOraConn->oraStat = OCITypeByName(this->pOraConn->envhp, this->pOraConn->errhp, this->pOraConn->svchp,
			(CONST text *)"SYS", strlen("SYS"),
			(CONST text *) this->sPayloadType.c_str(), strlen(this->sPayloadType.c_str()),
			(text *)0, 0, OCI_DURATION_SESSION, OCI_TYPEGET_ALL, &this->mesg_tdo);
	if (this->pOraConn->oraStat != OCI_SUCCESS)
	{
		gl_error(" Error getting type : %s", this->pOraConn->getError());
		iExitCode = FAILURE;
	}

	return iExitCode;
}

OracleQueue::~OracleQueue() {
	OCIDescriptorFree((dvoid *)this->dequeue_options, OCI_DTYPE_AQDEQ_OPTIONS);
}


int OracleQueue::enqueueStringMessage(string sMessage)
{
	int iExitCode = SUCCESS;
	char         msg_text[MAX_MESSAGE_SIZE + 1] = {'\0'};
	OCIRaw       *mesg = (OCIRaw *)0;

	/* Prepare the message payload */
	memset(msg_text,0,MAX_MESSAGE_SIZE + 1);
	sprintf(msg_text, "%s", sMessage.c_str());

	mesg = (OCIRaw *) 0;
	this->pOraConn->oraStat = OCIRawAssignBytes(this->pOraConn->envhp, this->pOraConn->errhp,
			(unsigned char *)msg_text, strlen(msg_text), &mesg);
	if (this->pOraConn->oraStat != OCI_SUCCESS)
	{
		  gl_error(" Error assigning bytes : %s\n", this->pOraConn->getError());
		  iExitCode = FAILURE;
	}
	else
	{
		/* Enqueue the message into raw_msg_queue */
		this->pOraConn->oraStat = OCIAQEnq(this->pOraConn->svchp, this->pOraConn->errhp, (OraText *)this->sQueueName.c_str(), 0, 0,
			  mesg_tdo, (dvoid **)&mesg, (dvoid **)&indptr, 0, 0);
		if (this->pOraConn->oraStat != OCI_SUCCESS)
		{
			  gl_error(" Error enqueing : %s\n", this->pOraConn->getError());
			  iExitCode = FAILURE;
		}
	}

	return iExitCode;
}


int OracleQueue::dequeueMessage(void **ppvMessage)
{
	int iExitCode = SUCCESS;
	text errbuf[512] = {'\0'};
	sb4 errcode = 0;

	if ((this->pOraConn->oraStat = OCIAQDeq(this->pOraConn->svchp, this->pOraConn->errhp,
			(OraText *)this->sQueueName.c_str(), this->dequeue_options, 0,
			this->mesg_tdo, (dvoid **)ppvMessage, (dvoid **)&this->indptr, 0, 0)) != OCI_SUCCESS)
	{
		/* check if this is due to timeout or no messages otherwise return an error */
		OCIErrorGet ((dvoid *) this->pOraConn->errhp, (ub4) 1, (text *) NULL,
						&errcode, errbuf, (ub4) sizeof(errbuf),
						(ub4) OCI_HTYPE_ERROR);

		/* if no more messages to dequeue exit */
		if (errcode == 25228)
		{
			*ppvMessage = NULL;
			iExitCode = OCI_NO_DATA;
		}
		else
		{
			*ppvMessage = NULL;
			gl_error("Error whilst dequeuing message : %s", this->pOraConn->getError());
			iExitCode = FAILURE;
		}
	}

	return iExitCode;
}

int OracleQueue::dequeueArray(void **ppvMessage, void **ppvIndPtr, OCIRaw **ppvMsgids, ub4 *piIters)
{
	int iExitCode = SUCCESS;
	text errbuf[512] = {'\0'};
	sb4 errcode = 0;

	if ((this->pOraConn->oraStat = OCIAQDeqArray(this->pOraConn->svchp, this->pOraConn->errhp,
			(OraText *)this->sQueueName.c_str(), this->dequeue_options,	piIters, 0, mesg_tdo,
			(dvoid **)ppvMessage, (dvoid **)ppvIndPtr, ppvMsgids, 0, 0, 0)) != OCI_SUCCESS)
	{
		/* check if this is due to timeout or no messages otherwise return an error */
		OCIErrorGet ((dvoid *) this->pOraConn->errhp, (ub4) 1, (text *) NULL,
						&errcode, errbuf, (ub4) sizeof(errbuf),
						(ub4) OCI_HTYPE_ERROR);

		/* if no more messages to dequeue exit */
		if (errcode == 25228)
		{
			iExitCode = OCI_NO_DATA;
		}
		else
		{
			gl_error("Error whilst dequeuing message array : %s", this->pOraConn->getError());
			iExitCode = FAILURE;
		}
	}

	return iExitCode;
}
