/*
 * OracleQueue.h
 *
 *  Created on: 22 May 2013
 *      Author: GBY18020
 */

#ifndef ORACLEQUEUE_H_
#define ORACLEQUEUE_H_

#include <string>
#include "OracleConnection.h"
#include "common_constants.h"

using namespace std;

class OracleQueue {
private:
	OracleConnection *pOraConn;
	OCIType *mesg_tdo;
	OCIInd ind;
	dvoid *indptr;
	OCIAQDeqOptions *dequeue_options;
	ub4 wait; /* wait of 1 to prevent concurrency issues */
	ub4 deqmode;
	ub4 visibility;

	string sQueueName;
	string sPayloadType;
	string sInterface;
	BOOL bMultiConsumer;


public:
	OracleQueue(OracleConnection *oraConn, char *pcQueueName, char *pcPayload, char *pcInterface, BOOL bMultiConsumer);
	~OracleQueue();
	int setupDequeue();
	int setupEnqueue();
	int enqueueStringMessage(string sMessage);
	int dequeueMessage(void **ppvMessage);
	int dequeueArray(void **ppvMessage, void **ppvIndPtr, OCIRaw **ppvMsgids, ub4 *piIters);
};

#endif /* ORACLEQUEUE_H_ */
