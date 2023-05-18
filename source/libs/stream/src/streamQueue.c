/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "streamInc.h"

#define MAX_STREAM_EXEC_BATCH_NUM 128
#define MIN_STREAM_EXEC_BATCH_NUM 16

SStreamQueue* streamQueueOpen(int64_t cap) {
  SStreamQueue* pQueue = taosMemoryCalloc(1, sizeof(SStreamQueue));
  if (pQueue == NULL) return NULL;
  pQueue->queue = taosOpenQueue();
  pQueue->qall = taosAllocateQall();
  if (pQueue->queue == NULL || pQueue->qall == NULL) {
    goto FAIL;
  }
  pQueue->status = STREAM_QUEUE__SUCESS;
  taosSetQueueCapacity(pQueue->queue, cap);
  taosSetQueueMemoryCapacity(pQueue->queue, cap * 1024);
  return pQueue;

FAIL:
  if (pQueue->queue) taosCloseQueue(pQueue->queue);
  if (pQueue->qall) taosFreeQall(pQueue->qall);
  taosMemoryFree(pQueue);
  return NULL;
}

void streamQueueClose(SStreamQueue* queue) {
  while (1) {
    void* qItem = streamQueueNextItem(queue);
    if (qItem) {
      streamFreeQitem(qItem);
    } else {
      break;
    }
  }
  taosFreeQall(queue->qall);
  taosCloseQueue(queue->queue);
  taosMemoryFree(queue);
}

#if 0
bool streamQueueResEmpty(const SStreamQueueRes* pRes) {
  //
  return true;
}
int64_t           streamQueueResSize(const SStreamQueueRes* pRes) { return pRes->size; }
SStreamQueueNode* streamQueueResFront(SStreamQueueRes* pRes) { return pRes->head; }
SStreamQueueNode* streamQueueResPop(SStreamQueueRes* pRes) {
  SStreamQueueNode* pRet = pRes->head;
  pRes->head = pRes->head->next;
  return pRet;
}

void streamQueueResClear(SStreamQueueRes* pRes) {
  while (pRes->head) {
    SStreamQueueNode* pNode = pRes->head;
    streamFreeQitem(pRes->head->item);
    pRes->head = pNode;
  }
}

SStreamQueueRes streamQueueBuildRes(SStreamQueueNode* pTail) {
  int64_t           size = 0;
  SStreamQueueNode* head = NULL;

  while (pTail) {
    SStreamQueueNode* pTmp = pTail->next;
    pTail->next = head;
    head = pTail;
    pTail = pTmp;
    size++;
  }

  return (SStreamQueueRes){.head = head, .size = size};
}

bool    streamQueueHasTask(const SStreamQueue1* pQueue) { return atomic_load_ptr(pQueue->pHead); }
int32_t streamQueuePush(SStreamQueue1* pQueue, SStreamQueueItem* pItem) {
  SStreamQueueNode* pNode = taosMemoryMalloc(sizeof(SStreamQueueNode));
  pNode->item = pItem;
  SStreamQueueNode* pHead = atomic_load_ptr(pQueue->pHead);
  while (1) {
    pNode->next = pHead;
    SStreamQueueNode* pOld = atomic_val_compare_exchange_ptr(pQueue->pHead, pHead, pNode);
    if (pOld == pHead) {
      break;
    }
  }
  return 0;
}

SStreamQueueRes streamQueueGetRes(SStreamQueue1* pQueue) {
  SStreamQueueNode* pNode = atomic_exchange_ptr(pQueue->pHead, NULL);
  if (pNode) return streamQueueBuildRes(pNode);
  return (SStreamQueueRes){0};
}
#endif

struct SStreamQueueReader {
  SStreamTask* pTask;
  int32_t      minBlocks;
  int32_t      maxBlocks;
  int32_t      maxWaitDuration;
  const char*  id;
  SBlocksBatch *pCurrentBatch;
};

static int32_t doLoadMultiBlockFromQueue(SStreamQueueReader* pReader, SStreamQueueItem** pItems) {
  SStreamTask* pTask = pReader->pTask;

  int32_t sleepTimeDuration = 5; // 5ms
  int32_t num = 0;
  int32_t tryCount = 0;
  *pItems = NULL;

  int64_t st = taosGetTimestampUs();
  qDebug("s-task:%s extract data block from inputQ, max-blocks:%d, min-blocks:%d, waitDur:%d", pReader->id,
      pReader->maxBlocks, pReader->minBlocks, pReader->maxWaitDuration);

  int32_t maxRetryCount = pReader->maxWaitDuration / 5;

  while (1) {
    SStreamQueueItem* qItem = streamQueueNextItem(pTask->inputQueue);
    if (qItem == NULL) {

      // only handle data in batch at the source tasks
      if (pTask->taskLevel == TASK_LEVEL__SOURCE && num < pReader->minBlocks && tryCount < maxRetryCount) {
        tryCount++;
        taosMsleep(sleepTimeDuration);
        qDebug("s-task:%s ===stream===try again numOfBlocks:%d", pReader->id, num);
        continue;
      }

      qDebug("s-task:%s ===stream===break numOfBlocks:%d", pReader->id, num);
      break;
    }

    if (*pItems == NULL) {
      *pItems = qItem;
      streamQueueProcessSuccess(pTask->inputQueue);
      if (pTask->taskLevel == TASK_LEVEL__SINK) {
        break;
      }
    } else {
      // todo we need to sort the data block, instead of just appending into the array list.
      SStreamQueueItem* newRet = NULL;
      if ((newRet = streamMergeQueueItem(*pItems, qItem)) == NULL) {
        streamQueueProcessFail(pTask->inputQueue);
        break;
      } else {
        *pItems = newRet;
        streamQueueProcessSuccess(pTask->inputQueue);

        if (++num > pReader->maxBlocks) {
          qDebug("s-task:%s maximum batch limit:%d reached, processing, %s", pReader->id, pReader->maxBlocks,
                 pTask->id.idStr);
          break;
        }
      }
    }
  }

  double el = (taosGetTimestampUs() - st) / 1000.0;
  qDebug("s-task:%s extract data from inputQ, numOfBlocks:%d, elapsed time:%.2fms", pReader->id, num, el);
  return TSDB_CODE_SUCCESS;
}

struct SBlocksBatch {
  int32_t             index;
  int32_t             blockType;
  SArray*             pBlockList;
  int64_t             blockVer;
  SStreamQueueItem*   pRawItems;
  SStreamQueueReader* pReader;
};

static int32_t doConvertBlockList(SBlocksBatch* pBatch, const char* id) {
  qDebug("s-task:%s convert source blocks list from inputQ, type:%d", id, pBatch->pRawItems->type);

  int32_t type = pBatch->pRawItems->type;
  SStreamQueueItem* pItem = pBatch->pRawItems;

  if (type == STREAM_INPUT__GET_RES) {
    const SStreamTrigger* pTrigger = (const SStreamTrigger*)pItem;

    SPackedData tmp = {.pDataBlock = pTrigger->pBlock};
    taosArrayPush(pBatch->pBlockList, &tmp);
    pBatch->blockType = STREAM_INPUT__DATA_BLOCK;
  } else if (type == STREAM_INPUT__DATA_SUBMIT) {
    const SStreamDataSubmit* pSubmit = (const SStreamDataSubmit*)pItem;

    taosArrayPush(pBatch->pBlockList, &pSubmit->submit);
    pBatch->blockType = STREAM_INPUT__DATA_SUBMIT;
    qDebug("s-task:%s set 1 submit blocks as source block completed, %p %p len:%d ver:%" PRId64, id, pSubmit,
           pSubmit->submit.msgStr, pSubmit->submit.msgLen, pSubmit->submit.ver);
  } else if (type == STREAM_INPUT__DATA_BLOCK || pItem->type == STREAM_INPUT__DATA_RETRIEVE) {
    const SStreamDataBlock* pBlock = (const SStreamDataBlock*)pItem;

    SArray* pBlockList = pBlock->blocks;
    int32_t numOfBlocks = taosArrayGetSize(pBlockList);
    qDebug("s-task:%s set sdata blocks as input, num:%d, ver:%" PRId64, id, numOfBlocks, pBlock->sourceVer);

    for (int32_t i = 0; i < numOfBlocks; ++i) {
      SSDataBlock* pDataBlock = taosArrayGet(pBlockList, i);
      SPackedData  tmp = {.pDataBlock = pDataBlock};
      taosArrayPush(pBatch->pBlockList, &tmp);
    }

    pBatch->blockType = STREAM_INPUT__DATA_BLOCK;
  } else if (type == STREAM_INPUT__MERGED_SUBMIT) {  // multiple submit blocks
    const SStreamMergedSubmit* pMerged = (const SStreamMergedSubmit*)pItem;

    SArray* pBlockList = pMerged->submits;
    int32_t numOfBlocks = taosArrayGetSize(pBlockList);
    qDebug("s-task:%s get submit msg (merged), batch num:%d, vre:%" PRId64, id, numOfBlocks, pMerged->ver);

    for (int32_t i = 0; i < numOfBlocks; i++) {
      SPackedData* pReq = POINTER_SHIFT(pBlockList->pData, i * sizeof(SPackedData));
      taosArrayPush(pBatch->pBlockList, pReq);
    }
    pBatch->blockType = STREAM_INPUT__DATA_SUBMIT;
  } else if (type == STREAM_INPUT__REF_DATA_BLOCK) {  // delete msg block
    const SStreamRefDataBlock* pRefBlock = (const SStreamRefDataBlock*)pItem;

    SPackedData tmp = {.pDataBlock = pRefBlock->pBlock};
    taosArrayPush(pBatch->pBlockList, &tmp);
    pBatch->blockType = STREAM_INPUT__DATA_BLOCK;
  }

  return TSDB_CODE_SUCCESS;
}

static void resetBlockRes(SPackedData* pData, int32_t* type) {
  *type = 0;
  memset(pData, 0, sizeof(SPackedData));
}

SStreamQueueReader* createQueueReader(SStreamTask* pTask, int32_t maxDuration, int32_t minBlocks, int32_t maxBlocks) {
  SStreamQueueReader* p = taosMemoryCalloc(1, sizeof(SStreamQueueReader));
  if (p == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    qError("failed to allocated queue reader, size:%d", (int32_t) sizeof(SStreamQueueReader));
    return NULL;
  }

  if (maxBlocks < minBlocks) {
    TSWAP(maxBlocks, minBlocks);
  }

  if (maxBlocks > MAX_STREAM_EXEC_BATCH_NUM) {
    maxBlocks = MAX_STREAM_EXEC_BATCH_NUM;
  }

  if (minBlocks < MIN_STREAM_EXEC_BATCH_NUM) {
    minBlocks = MIN_STREAM_EXEC_BATCH_NUM;
  }

  p->pTask = pTask;
  p->id = pTask->id.idStr;
  p->minBlocks = minBlocks;
  p->maxBlocks = maxBlocks;
  p->maxWaitDuration = maxDuration;

  return p;
}

void* destroyBlockBatch(SBlocksBatch* pBatch) {
  if (pBatch == NULL) {
    return NULL;
  }

  if (pBatch->pBlockList != NULL) {
    pBatch->pBlockList = taosArrayDestroy(pBatch->pBlockList);
  }

  streamFreeQitem(pBatch->pRawItems);
  pBatch->pRawItems = NULL;
  return NULL;
}

void* destroyQueueReader(SStreamQueueReader* pReader) {
  taosMemoryFree(pReader);
  return NULL;
}

SBlocksBatch* queueReaderGetNewBlockBatch(SStreamQueueReader* pReader) {
  if (pReader->pCurrentBatch != NULL) {
    pReader->pCurrentBatch = destroyBlockBatch(pReader->pCurrentBatch);
  }

  SBlocksBatch* pBatch = taosMemoryCalloc(1, sizeof(SBlocksBatch));
  pBatch->pReader = pReader;
  pBatch->pBlockList = taosArrayInit(0, sizeof(SPackedData));

  int32_t code = doLoadMultiBlockFromQueue(pReader, &pBatch->pRawItems);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  if (pBatch->pRawItems == NULL) {
    destroyBlockBatch(pBatch);
    return NULL;
  }

  // for sink node, no convert needed
  if (pReader->pTask->taskLevel != TASK_LEVEL__SINK) {
    code = doConvertBlockList(pBatch, pReader->id);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return NULL;
    }
  }

  pReader->pCurrentBatch = pBatch;
  return pBatch;
}

SBlocksBatch* queueReaderGetCurrentBlockBatch(SStreamQueueReader* pReader) {
  return pReader->pCurrentBatch;
}

SPackedData* queueReaderNextBlocks(SBlocksBatch* pBatch) {
  if (pBatch->pBlockList == NULL || pBatch->pRawItems == NULL || pBatch->index >= taosArrayGetSize(pBatch->pBlockList)) {
    return NULL;
  }

  SPackedData* p = taosArrayGet(pBatch->pBlockList, pBatch->index++);
  return p;
}

int32_t queueReaderGetBlockType(SBlocksBatch* pBatch, int32_t* type, int64_t* ver) {
  if (pBatch != NULL) {
    *type = pBatch->blockType;
    *ver = pBatch->blockVer;
  }
  return pBatch->blockType;
}

SStreamQueueItem* queueReaderGetRawItems(SBlocksBatch* pBatch) {
  return pBatch->pRawItems;
}
