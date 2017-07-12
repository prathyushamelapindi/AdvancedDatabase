//
//  page_table.h
//  CS525_Assign#02
//
//  Created by raghuveer vemuri on 9/27/15.
//  Copyright © 2015 raghuveer vemuri. All rights reserved.
//

#ifndef page_table_h
#define page_table_h

#include "dt.h"
#include <pthread.h>


#include "buffer_mgr.h"
#include "storage_mgr.h"


typedef struct BP_PageFrame{
    
    BM_PageHandle page;
    int fix_Count;
    bool isDirty;
    
    struct BP_PageFrame *nextNode;
    
}BP_PageFrame;

typedef struct LRU_Helper{
    
    BP_PageFrame* pointing;
    struct LRU_Helper *nextNode;
    
}LRU_Helper;

typedef struct FIFO_Helper{
    
    BP_PageFrame* pointing;
    struct FIFO_Helper *nextNode;
    
}FIFO_Helper;

typedef struct BP_MgmtData{
    
    SM_FileHandle *fileHandle;
    BP_PageFrame *Head;
    BP_PageFrame *Tail;
    
    int readCount;
    int writeCount;
    
    LRU_Helper *LRU_Head;
    LRU_Helper *LRU_Tail;
    
    FIFO_Helper *FIFO_Head;
    FIFO_Helper *FIFO_Tail;
    
    pthread_mutex_t bm_Mutex;
    
}BP_MgmtData;

void init_PageFrame(BP_PageFrame *frame);

void init_BPMgmtData(BP_MgmtData *mgmtData);

RC insert_PageFrame(BP_PageFrame *frame, BM_BufferPool *const pool);

RC updateMRU(BP_PageFrame *frame, BM_BufferPool *const bm);

RC updateFIFO(BP_PageFrame* frame, BM_BufferPool *const bm);

BP_PageFrame *getFrame(BM_BufferPool *const bm, PageNumber pNum);

#endif /* page_table_h */
