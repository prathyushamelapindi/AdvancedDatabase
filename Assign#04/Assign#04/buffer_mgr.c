//
//  buffer_mgr.c
//  CS525_Assign#02
//
//  Created by raghuveer vemuri on 9/27/15.
//  Copyright © 2015 raghuveer vemuri. All rights reserved.
//

#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "page_table.h"
#include "assert.h"

//Thread-Saftey helpers
#define LOCK() pthread_mutex_lock(&(mgmtData->bm_Mutex));
#define UNLOCK() pthread_mutex_unlock(&(mgmtData->bm_Mutex));


//Initializes the buffer pool
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData)
{
    
    bm->pageFile = (char*) pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    
    BP_MgmtData* mgmtData = (BP_MgmtData*) malloc(sizeof(BP_MgmtData));
    bm->mgmtData = mgmtData;
    mgmtData->fileHandle = (SM_FileHandle*) malloc(sizeof(SM_FileHandle));
    
    init_BPMgmtData(mgmtData);
    
    if(openPageFile((char*)pageFileName, mgmtData->fileHandle) != RC_OK){
        return RC_BUFFER_INIT_FAIL_FILEOPEN;
    }
    
    int i = 0;
    BP_PageFrame *frame;
    
    //Initializes each frame and data
    while (i<numPages) {
        
        frame = (BP_PageFrame*) malloc(sizeof(BP_PageFrame));
        frame->page.data  = (char*) calloc(PAGE_SIZE, sizeof(char));
        init_PageFrame(frame);
        insert_PageFrame(frame, bm);
        i++;
    }
    
    return RC_OK;
}

//Shuts down the buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    if (bm==NULL) {
        return RC_BUFFERPOOL_INIT_ERROR;
    }
    
    BP_MgmtData *mgmtData = bm->mgmtData;
    LOCK();
    
    BP_PageFrame *prevFrame = NULL, *frame =  mgmtData->Head;
    
    int i =0;
    while (i<bm->numPages) {
        if(frame->fix_Count != 0){
            UNLOCK();
            return RC_BUFFER_SHUTDOWN_FAIL_FIXCOUNT;
        }
        frame = frame->nextNode;
        i++;
    }
    
    forceFlushPool(bm);
    
    i =0;
    frame =  mgmtData->Head;
    while (i<bm->numPages) {
        
        free(frame->page.data);
        prevFrame = frame;
        if (frame->nextNode!= NULL) frame = frame->nextNode;
        free(prevFrame);
        i++;
    }
    
    //Start Memory Freeing
    free(mgmtData->fileHandle);
    
    LRU_Helper *LRUNext = NULL, *LRUFrame = mgmtData->LRU_Head;
    FIFO_Helper *FIFONext = NULL, *FIFOFrame = mgmtData->FIFO_Head;
    i =0;
    while (i<bm->numPages) {
        
        LRUNext = LRUFrame->nextNode;
        FIFONext = FIFOFrame->nextNode;
        
        free(LRUFrame);
        free(FIFOFrame);
        
        LRUFrame = LRUNext;
        FIFOFrame = FIFONext;
        i++;
    }
    
    //End Memory Freeing

    
    UNLOCK();
    free(mgmtData);
    mgmtData=NULL;
    
    return RC_OK;
}

//Force flushes the pool
RC forceFlushPool(BM_BufferPool *const bm)
{
    if (bm==NULL) {
        return RC_BUFFERPOOL_INIT_ERROR;
    }
    
    BP_MgmtData *mgmtData = bm->mgmtData;
    LOCK();
    
    BP_PageFrame *frame =  mgmtData->Head;
    
    int i = 0;
    while (i<bm->numPages) {
        if(frame->isDirty & (frame->fix_Count == 0)){
            RC state = writeBlock(frame->page.pageNum, mgmtData->fileHandle, (SM_PageHandle) frame->page.data);
            
            if (state != RC_OK) return state;
            
            frame->isDirty = FALSE;
            mgmtData->writeCount++;
        }
        frame = frame->nextNode;
        i++;
    }
    
    UNLOCK();
    
    return RC_OK;
    
}


// Buffer Manager Interface Access Pages

//Marks the passed page as dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (bm==NULL) {
        return RC_BUFFERPOOL_INIT_ERROR;
    }
    
    if (page==NULL) {
        return RC_PAGEFRAME_ERROR;
    }
    
    BP_MgmtData *mgmtData = bm->mgmtData;
    LOCK();
    BP_PageFrame *frame = getFrame(bm,page->pageNum);
    
    //If found the frame in buffer
    if(frame != NULL)
    {
        frame->isDirty = true;
        UNLOCK();
        return RC_OK;
    }
    UNLOCK();
    return RC_PAGE_NOTPINNED;
    
}


//Unpins the page passed
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (bm==NULL) {
        return RC_BUFFERPOOL_INIT_ERROR;
    }
    
    if (page==NULL) {
        return RC_PAGEFRAME_ERROR;
    }
    
    BP_MgmtData *mgmtData = bm->mgmtData;
    LOCK();
    BP_PageFrame *frame = getFrame(bm,page->pageNum);

    //If found the frame in buffer
    if(frame != NULL)
    {
        assert(frame->fix_Count>0);
        
        frame->fix_Count--;
        UNLOCK();
        return RC_OK;
    }
    
    UNLOCK();
    return RC_PAGE_NOT_IN_BUFFERPOOL;
    
}

//forces the page to flush if fixcount is 0
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (bm==NULL) {
        return RC_BUFFERPOOL_INIT_ERROR;
    }
    
    if (page==NULL) {
        return RC_PAGEFRAME_ERROR;
    }
    
    BP_MgmtData *mgmtData = bm->mgmtData;
    LOCK();
    BP_PageFrame *frame = getFrame(bm,page->pageNum);
    
    //If found the frame in buffer
    if(frame != NULL)
    {
        if(frame->isDirty && frame->fix_Count==0){
            writeBlock(frame->page.pageNum, mgmtData->fileHandle, (SM_PageHandle) frame->page.data);
            frame->isDirty = FALSE;
            mgmtData->writeCount++;
            
        }

        UNLOCK();
        return RC_OK;
    }
    
    UNLOCK();
    return RC_PAGE_NOT_IN_BUFFERPOOL;

}

//Helper method to get the frame that can be replaced with replacement strategy in context
BP_PageFrame *getReplaceableFrame(BM_BufferPool* bm){
    
    BP_PageFrame* frame = NULL;
    BP_MgmtData* mgmtData = bm->mgmtData;
    
    if (bm->strategy == RS_LRU) {
        
        frame = mgmtData->LRU_Head->pointing;
        
        if (frame->fix_Count == 0) {
            updateMRU(frame, bm);
            updateFIFO(frame, bm);
            return frame;
        }
        
    }
    
    else if(bm->strategy == RS_FIFO){
        
        frame = mgmtData->FIFO_Head->pointing;
        
        int i = 0;
        while (i<bm->numPages) {
            
            if (frame->fix_Count == 0) {
                updateMRU(frame, bm);
                updateFIFO(frame, bm);
                return frame;
            }
            
            frame = frame->nextNode;
            i++;
        }
        
    }
    
    return NULL;
}

//pins the passed page
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
    if (bm==NULL) {
        return RC_BUFFERPOOL_INIT_ERROR;
    }
    
    if (page==NULL) {
        return RC_PAGEFRAME_ERROR;
    }
    
    BP_MgmtData *mgmtData = bm->mgmtData;
    LOCK();
    BP_PageFrame *frame = getFrame(bm,pageNum);
    
    //If found the frame in buffer
    if(frame != NULL)
    {
        page->pageNum = frame->page.pageNum;
        page->data = frame->page.data;
        frame->fix_Count++;
        updateMRU(frame, bm);
        
        UNLOCK();
        return RC_OK;
        
    }
    
    //else flush a flushable frame and fetch from disk
    
    frame = getReplaceableFrame(bm);
    
    if (frame==NULL){
        UNLOCK();
        return RC_BUFFERPOOL_PEAKED;
    }
    
    forcePage(bm, &frame->page);
    
    //Page Number doesn't exist (overflowed)in the file.
    if (pageNum >= mgmtData->fileHandle->totalNumPages) {
        frame->fix_Count++;
        frame->page.pageNum = pageNum;
        page->data = frame->page.data;
        page->pageNum = pageNum;
        
        UNLOCK();
        return RC_OK;
    }
    
    RC status = readBlock(pageNum, mgmtData->fileHandle, frame->page.data);
    
    if (status == RC_READ_NON_EXISTING_PAGE) {
        
    }
    
    else if (status!=RC_OK) {
        UNLOCK();
        return status;
    }
    
    mgmtData->readCount++;
    frame->fix_Count++;
    frame->page.pageNum = pageNum;
    page->data = frame->page.data;
    page->pageNum = pageNum;
    
    UNLOCK();
    return RC_OK;
    
    
}

// Statistics Interface

//gets the list of pages that are in buffer
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    if (bm==NULL) {
        return 0;
    }
    
    BP_MgmtData *mgmtData = bm->mgmtData;
    LOCK();
    BP_PageFrame *frame =  mgmtData->Head;
    
    PageNumber* pageNumArray = malloc(sizeof(int)*bm->numPages);
    
    int i =0;
    while (i<bm->numPages) {
        
        pageNumArray[i] = frame->page.pageNum;
        
        frame = frame->nextNode;
        i++;
    }
    
    UNLOCK();
    return pageNumArray;
}


//get the fixcounts on each frames
int *getFixCounts (BM_BufferPool *const bm)
{
    if (bm==NULL) {
        return 0;
    }
    
    BP_MgmtData *mgmtData = bm->mgmtData;
    LOCK();
    BP_PageFrame *frame =  mgmtData->Head;
    
    PageNumber* fixCountsArray = malloc(sizeof(int)*bm->numPages);
    
    int i =0;
    while (i<bm->numPages) {
        
        fixCountsArray[i] = frame->fix_Count;
        
        frame = frame->nextNode;
        i++;
    }
    
    UNLOCK();
    return fixCountsArray;

}

//returns the number of DiskReads till now
int getNumReadIO (BM_BufferPool *const bm)
{
    if (bm==NULL) {
        return 0;
    }
    
    BP_MgmtData* mgmtData = bm->mgmtData;
    
    return mgmtData->readCount;
    
}

//returns the number of DiskWrites till now
int getNumWriteIO (BM_BufferPool *const bm)
{
    if (bm==NULL) {
        return 0;
    }
    
    BP_MgmtData* mgmtData = bm->mgmtData;
    
    return mgmtData->writeCount;

    
}

//returns the list of flags representing if it's dirty for each frame in the buffer
bool *getDirtyFlags (BM_BufferPool *const bm)
{
    if (bm==NULL) {
        return false;
    }
    
    BP_MgmtData *mgmtData = bm->mgmtData;
    LOCK();
    BP_PageFrame *frame =  mgmtData->Head;

    bool* dirtyFlagsArray = malloc(sizeof(bool)*bm->numPages);

    int i =0;
    while (i<bm->numPages) {

        dirtyFlagsArray[i] = frame->isDirty;

        frame = frame->nextNode;
        i++;
    }

    UNLOCK();
    return dirtyFlagsArray;
}
