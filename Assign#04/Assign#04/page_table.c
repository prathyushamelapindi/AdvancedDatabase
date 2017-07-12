//
//  page_table.c
//  CS525_Assign#02
//
//  Created by raghuveer vemuri on 9/27/15.
//  Copyright © 2015 raghuveer vemuri. All rights reserved.
//

#include <stdlib.h>
#include "page_table.h"
#include "dberror.h"


//Initialization function for BP_PageFrame
void init_PageFrame(BP_PageFrame *frame){
    
    (frame->page).pageNum = NO_PAGE;
    //(frame->page).data = (char*) calloc(PAGE_SIZE, sizeof(char));
    
    frame->fix_Count = 0;
    frame->isDirty = false;
    frame->nextNode = NULL;
    
}

//Initialization function for SM_FileHandle
void init_SMFileHandle(SM_FileHandle* fileHandle){
    
    //fileHandle = (SM_FileHandle*) malloc(sizeof(SM_FileHandle));
    
    fileHandle->curPagePos = 0;
    fileHandle->fileName = NULL;
    fileHandle->totalNumPages = 0;
    fileHandle->mgmtInfo = NULL;
    
}

//Initialization function for BP_MgmtData
void init_BPMgmtData(BP_MgmtData *mgmtData){
    
    mgmtData->readCount = 0;
    mgmtData->writeCount = 0;
    
    mgmtData->Head = NULL;
    mgmtData->Tail = NULL;
    
    mgmtData->LRU_Tail = NULL;
    mgmtData->LRU_Head = NULL;
    
    mgmtData->FIFO_Tail = NULL;
    mgmtData->FIFO_Head = NULL;
    
    //mgmtData->fileHandle = (SM_FileHandle*) malloc(sizeof(SM_FileHandle));
    
    init_SMFileHandle(mgmtData->fileHandle);
    
}

//Returns LRU an node, used for maintaining data for replacement strategy
LRU_Helper* makeLRUNode(){
    
    LRU_Helper* node = (LRU_Helper*)malloc(sizeof(LRU_Helper));
    node->nextNode = NULL;
    node->pointing = NULL;
    
    return node;
    
}

//Returns FIFO an node, used for maintaining data for replacement strategy
FIFO_Helper* makeFIFONode(){
    
    FIFO_Helper* node = (FIFO_Helper*)malloc(sizeof(FIFO_Helper));
    node->nextNode = NULL;
    node->pointing = NULL;
    
    return node;
    
}

//Inserts specified BP_PageFrame into specified BM_BufferPool
RC insert_PageFrame(BP_PageFrame *frame, BM_BufferPool *const pool){
    
    if (pool == NULL) {
        return RC_BUFFERPOOL_INIT_ERROR;
    }
    
    if (frame == NULL) {
        return RC_PAGEFRAME_ERROR;
    }
    
    BP_MgmtData* mgmtData = (BP_MgmtData*) pool->mgmtData;
    
    //If no element in linked list
    if(mgmtData->Head == NULL)
    {
        mgmtData->Head = frame;
        mgmtData->Tail = frame;
    }
    
    else{
        (mgmtData->Tail)->nextNode = frame;
        mgmtData->Tail = frame;
    }
    
    
    
    //--------LRU HELPER CODE FOLLOWS-----------------
    LRU_Helper *LRU_Node = makeLRUNode();
    LRU_Node->pointing = frame;
    
    
    if (mgmtData->LRU_Head == NULL) {
        mgmtData->LRU_Head = LRU_Node;
        mgmtData->LRU_Tail = LRU_Node;
    }
    
    else{
        mgmtData->LRU_Tail->nextNode = LRU_Node;
        mgmtData->LRU_Tail = LRU_Node;
    }
    
    //--------------LRU HELPER CODE ENDS---------------
    
    //--------FIFO HELPER CODE FOLLOWS-----------------
    FIFO_Helper *FIFO_Node = makeFIFONode();
    FIFO_Node->pointing = frame;
    
    
    if (mgmtData->FIFO_Head == NULL) {
        mgmtData->FIFO_Head = FIFO_Node;
        mgmtData->FIFO_Tail = FIFO_Node;
    }
    
    else{
        mgmtData->FIFO_Tail->nextNode = FIFO_Node;
        mgmtData->FIFO_Tail = FIFO_Node;
    }
    
    //--------------FIFO HELPER CODE ENDS---------------
    
    
    return RC_OK;
    
}

//sifts through the BM_BufferPool to find PageNumber if it exists.
BP_PageFrame *getFrame(BM_BufferPool *const bm, PageNumber pNum)
{
    BP_MgmtData *mgmtData = bm->mgmtData;
    BP_PageFrame *frame =  mgmtData->Head;
    
    int i=0;
    
    while (i<bm->numPages) {
        
        if(frame->page.pageNum == pNum){
            return frame;
        }
        
        frame = frame->nextNode;
        i++;
    }
    
    return NULL;
}

//Moves the frame to last (LRU)
RC moveToEndLRU(LRU_Helper* frame, LRU_Helper* frPrev, BP_MgmtData *const mgmtData){
    
    if(mgmtData->LRU_Head == NULL){
        return RC_LRU_MAINTENANCE_ERROR;
    }
    
    if ((mgmtData->LRU_Head == frame && mgmtData->LRU_Tail == frame) || (mgmtData->LRU_Tail == frame)) {
        return RC_OK;
    }
    
    if (mgmtData->LRU_Head == frame && frPrev == NULL) {
        mgmtData->LRU_Head = frame->nextNode;
        mgmtData->LRU_Tail->nextNode = frame;
        frame->nextNode = NULL;
        mgmtData->LRU_Tail = frame;
        
        return RC_OK;
    }
    
    frPrev->nextNode = frame->nextNode;
    mgmtData->LRU_Tail->nextNode = frame;
    frame->nextNode = NULL;
    mgmtData->LRU_Tail = frame;
    return RC_OK;
    
}

//Moves the frame to end (FIFO)
RC moveToEndFIFO(FIFO_Helper* frame, FIFO_Helper* frPrev, BP_MgmtData *const mgmtData){
    
    if(mgmtData->FIFO_Head == NULL){
        return RC_LRU_MAINTENANCE_ERROR;
    }
    
    if ((mgmtData->FIFO_Head == frame && mgmtData->FIFO_Tail == frame) || (mgmtData->FIFO_Tail == frame)) {
        return RC_OK;
    }
    
    if (mgmtData->FIFO_Head == frame && frPrev == NULL) {
        mgmtData->FIFO_Head = frame->nextNode;
        mgmtData->FIFO_Tail->nextNode = frame;
        frame->nextNode = NULL;
        mgmtData->FIFO_Tail = frame;
        
        return RC_OK;
    }
    
    frPrev->nextNode = frame->nextNode;
    mgmtData->FIFO_Tail->nextNode = frame;
    frame->nextNode = NULL;
    mgmtData->FIFO_Tail = frame;
    return RC_OK;
    
}

//Update the passed frame as Most recenlty used frame
RC updateMRU(BP_PageFrame* frame, BM_BufferPool *const bm){
    
    BP_MgmtData *mgmtData = bm->mgmtData;
    
    LRU_Helper *fr =mgmtData->LRU_Head;
    LRU_Helper *frPrev = NULL;
    
    int i=0;
    while (i < bm->numPages) {
        
        if (fr->pointing == frame) {
            RC status = moveToEndLRU(fr,frPrev,mgmtData);
            if (status != RC_OK) return RC_LRU_MAINTENANCE_ERROR;
            break;
        }
        
        frPrev = fr;
        fr = fr->nextNode;
        i++;
    }
    
    return RC_OK;
}

//Update the frame as Most recently fetched frame
RC updateFIFO(BP_PageFrame* frame, BM_BufferPool *const bm){
    
    BP_MgmtData *mgmtData = bm->mgmtData;
    
    FIFO_Helper *fr =mgmtData->FIFO_Head;
    FIFO_Helper *frPrev = NULL;
    
    int i=0;
    while (i < bm->numPages) {
        
        if (fr->pointing == frame) {
            RC status = moveToEndFIFO(fr,frPrev,mgmtData);
            if (status != RC_OK) return RC_FIFO_MAINTENANCE_ERROR;
            break;
        }
        
        frPrev = fr;
        fr = fr->nextNode;
        i++;
    }
    
    return RC_OK;
}


