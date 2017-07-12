//
//  record_mgr.c
//  Assign#03
//
//  Created by raghuveer vemuri on 10/27/15.
//  Copyright © 2015 raghuveer vemuri. All rights reserved.
//

#include "buffer_mgr.h"
#include "storage_mgr.h"
# include "record_mgr.h"
#include "page_table.h"

#include <stdlib.h>
#include <string.h>
//#include <math.h>

#define I386_PGBYTES 4096 //remove later

//#include "bitmap.c"

#include <limits.h>
#include <stdint.h>   /* for uint32_t */


typedef uint8_t word_t;

enum { BITS_PER_WORD = sizeof(word_t) * CHAR_BIT };
#define WORD_OFFSET(b) ((b) / BITS_PER_WORD)
#define BIT_OFFSET(b)  ((b) % BITS_PER_WORD)

void set_bit(word_t *words, int n) {
    words[WORD_OFFSET(n)] |= (1 << BIT_OFFSET(n));
}

void clear_bit(word_t *words, int n) {
    words[WORD_OFFSET(n)] &= ~(1 << BIT_OFFSET(n));
}

int get_bit(word_t *words, int n) {
    word_t bit = words[WORD_OFFSET(n)] & (1 << BIT_OFFSET(n));
    return bit != 0;
}




typedef struct RM_PageData{
    
    int next;       //Stores the page number having empty slots
    int prev;       //Stores the page number having empty slots
    
    uint8_t *bitMap; //BitMap to identify Empty/Deleted Slots
    
    char *data;      //Record's begin here(i.e., Slot-0 of this page starts here)
    
}RM_PageData;


typedef struct RM_TableMgmtData{
    
    int recordCount;
    int freePage;
    
    RM_PageData pageInContext;
    BM_BufferPool bufferPool;
    BM_PageHandle pageHandle;
    
}RM_TableMgmtData;

typedef struct RM_ScanMgmtData{
    
    BM_PageHandle pageHandle;
    RM_PageData pageData;
    
    RID rid;
    int scanCount;
    Expr *cond;
    
}RM_ScanMgmtData;


#define MAX_FIELDNAME_LENGTH 64
#define SLOTS_PER_PAGE(schema) (int)((PAGE_SIZE - (2*sizeof(int)) - 1)/(getRecordSize(schema)+((double)1/(CHAR_BIT))))
#define TO_OFFSET(offset,size,src) memcpy(offset,src,size);offset+=size;
#define FROM_OFFSET(offset,size,dest) memcpy(dest,offset,size);offset+=size;


Schema* buildSchema(int numAttr, int keySize)
{
    Schema* schema = (Schema*) malloc(sizeof(Schema));
    
    schema->numAttr = numAttr;
    schema->attrNames = (char**)malloc(sizeof(char*) * numAttr);
    schema->dataTypes = (DataType*)malloc(sizeof(DataType)*numAttr);
    schema->typeLength = (int*) malloc(sizeof(int) * numAttr);
    schema->keySize = keySize;
    schema->keyAttrs = (int*) malloc(sizeof(int) * keySize);
    
    int i;
    for (i = 0; i<numAttr; i++) {
        schema->attrNames[i] = (char*)malloc(MAX_FIELDNAME_LENGTH);
    }
    
    return schema;
    
}

int getFreeSlot(RM_PageData* pageData, Schema *schema)
{
    int slotcount = SLOTS_PER_PAGE(schema);
    
    int i;
    for (i =0; i<slotcount; i++) {
        if (get_bit(pageData->bitMap, i) == 0) {
            return i;
        }
    }
    
    return -1;
    
}

void loadPageIntoContext(BM_PageHandle* pageHandle, RM_PageData* pageData, Schema* schema)
{
    
    char* offset = pageHandle->data;
    FROM_OFFSET(offset, sizeof(int), &pageData->next);
    FROM_OFFSET(offset, sizeof(int), &pageData->prev);
    
    pageData->bitMap = (uint8_t*)offset;
    offset+= ((SLOTS_PER_PAGE(schema)/CHAR_BIT) + 1);
    pageData->data = offset;
    
}

void updateFreePages(RM_TableData* tblData)
{
    RM_TableMgmtData *mgmtData =  tblData->mgmtData;
    RM_PageData *pageData = &mgmtData->pageInContext;
    char* offset;
    
    //Check if empty slots are available.
    int emptySlot = getFreeSlot(pageData, tblData->schema);
    
    //If not, kick out(if present in list)
    //This is not relevant to newly added block as they will(should) have free space.
    if (emptySlot == -1) {
        
        //Not the only page in freepage List
        if (mgmtData->pageInContext.next != mgmtData->pageHandle.pageNum) {
            mgmtData->freePage = mgmtData->pageInContext.next;
            //mgmtData->pageInContext.next = 0;
            
            int temp = 0;
            offset = mgmtData->pageHandle.data;
            TO_OFFSET(offset, sizeof(int), &temp);
            TO_OFFSET(offset, sizeof(int), &temp);
            markDirty(&mgmtData->bufferPool, &mgmtData->pageHandle);
            
            
            //setting prev of the next page to itself
            BM_PageHandle pageHandleTemp;
            
            pinPage(&mgmtData->bufferPool, &pageHandleTemp, mgmtData->freePage);
            
            offset = pageHandleTemp.data;
            offset+=4;
            TO_OFFSET(offset, sizeof(int), &pageHandleTemp.pageNum);
            markDirty(&mgmtData->bufferPool, &mgmtData->pageHandle);
            
            unpinPage(&mgmtData->bufferPool, &pageHandleTemp);
            
        }
        
        else{
            
            mgmtData->freePage = 0;
            
            int temp = 0;
            offset = mgmtData->pageHandle.data;
            TO_OFFSET(offset, sizeof(int), &temp);
            TO_OFFSET(offset, sizeof(int), &temp);
            markDirty(&mgmtData->bufferPool, &mgmtData->pageHandle);
            
        }
        
        return;
        
    }
    
    //Already in List not a new page
    if (mgmtData->pageInContext.next !=0) {
        return;
    }
    
    
    //This is relevant to newly appended block
    if (mgmtData->freePage == 0) {
        
        mgmtData->freePage = mgmtData->pageHandle.pageNum;
        pageData->next = pageData->prev = mgmtData->pageHandle.pageNum;
        
        //Manpulating current page
        offset = mgmtData->pageHandle.data;
        TO_OFFSET(offset, sizeof(int), &mgmtData->pageHandle.pageNum);
        TO_OFFSET(offset, sizeof(int), &mgmtData->pageHandle.pageNum);
        markDirty(&mgmtData->bufferPool, &mgmtData->pageHandle);
        
    }
    
    else{
        
        offset = mgmtData->pageHandle.data;
        TO_OFFSET(offset, sizeof(int), &mgmtData->freePage);
        TO_OFFSET(offset, sizeof(int), &mgmtData->pageHandle.pageNum);
        
        BM_PageHandle pageHandle;
        
        //load curent free page and manipulate
        pinPage(&mgmtData->bufferPool, &pageHandle, mgmtData->freePage);
        offset = pageHandle.data;
        offset+=4;
        TO_OFFSET(offset, sizeof(int), &mgmtData->pageHandle.pageNum);
        
        markDirty(&mgmtData->bufferPool, &pageHandle);
        
        mgmtData->freePage = mgmtData->pageHandle.pageNum;
        
        unpinPage(&mgmtData->bufferPool, &pageHandle);
        
    }
    
}

// table and manager

RC initRecordManager (void *mgmtData)
{
    return RC_OK;
}


RC shutdownRecordManager ()
{
    return RC_OK;
}


RC createTable (char *name, Schema *schema)
{
    SM_FileHandle *fileHandle;
    char data[PAGE_SIZE];
    char *offset = data;
    int grossSchemaSize = 0, grossRecordSize = 0;
    
    grossSchemaSize+= sizeof(int) * (4 + schema->keySize); //RecordCount+FreePage+NumAttr+KeySize
    grossSchemaSize+= schema->numAttr * (2*sizeof(int) + MAX_FIELDNAME_LENGTH); //DataType+TypeLength+FiledNameLength
    
    if (grossSchemaSize>PAGE_SIZE) {
        return RC_SCHEMA_TOO_BIG;
    }
    
    grossRecordSize+=getRecordSize(schema) + (sizeof(int) * 2) + 1; //Next + Prev + BitMap
    
    if (grossRecordSize>PAGE_SIZE) {
        return RC_RECORD_TOO_BIG;
    }
    
    
    memset(offset, 0, PAGE_SIZE);
    
    int a = 0; //Record count
    TO_OFFSET(offset,sizeof(int),&a);//0
    
    a = 0; //FreePage Number
    TO_OFFSET(offset, sizeof(int), &a);//4
    
    TO_OFFSET(offset, sizeof(int), &schema->numAttr);//8
    TO_OFFSET(offset, sizeof(int), &schema->keySize);//12
    
    int i;
    
    for (i = 0; i<schema->keySize; i++) {
        TO_OFFSET(offset, sizeof(int), &schema->keyAttrs[i]);//16
    }
    
    
    for (i = 0; i<schema->numAttr; i++) {
        
        TO_OFFSET(offset, sizeof(int), &schema->dataTypes[i]);          //Should I convert this into integer?
        TO_OFFSET(offset, sizeof(int), &schema->typeLength[i]);
        //char*temp = offset;
        TO_OFFSET(offset, MAX_FIELDNAME_LENGTH, schema->attrNames[i]);
        //printf("val: %s and address: %d\n",temp,temp);
    }
    
    fileHandle = (SM_FileHandle*)malloc(sizeof(SM_FileHandle));
    
    RC status = createPageFile(name);
    
    if (status!=RC_OK) {
        return status;
    }
    
    if ((status = openPageFile(name, fileHandle))!=RC_OK) {
        return status;
    }
    
    if ((status = writeBlock(0, fileHandle, data))!=RC_OK) {
        return RC_OK;
    }
    
    if ((status = closePageFile(fileHandle))!=RC_OK) {
        return status;
    }
    
    free(fileHandle);
    
    return RC_OK;
}

RC openTable(RM_TableData *rel, char *name)
{
    rel->name = strdup(name);
    RM_TableMgmtData *tblMgmtData = (RM_TableMgmtData*) malloc(sizeof(RM_TableMgmtData));
    rel->mgmtData = tblMgmtData;
    char* offset;
    
    RC status = initBufferPool(&tblMgmtData->bufferPool, rel->name, 1024, RS_FIFO, NULL);
    
    if (status!=RC_OK) {
        return status;
    }
    
    if ((status = pinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle, (PageNumber)0))!=RC_OK) {
        return status;
    }
    
    offset = (char*)tblMgmtData->pageHandle.data;
    
    FROM_OFFSET(offset, sizeof(int), &tblMgmtData->recordCount);
    FROM_OFFSET(offset, sizeof(int), &tblMgmtData->freePage);
    
    char* tempOffset = offset;
    int numAttrTemp, keySizeTemp;
    
    FROM_OFFSET(tempOffset, sizeof(int), &(numAttrTemp));
    FROM_OFFSET(tempOffset, sizeof(int), &(keySizeTemp));
    
    rel->schema = buildSchema(numAttrTemp,keySizeTemp);
    
    FROM_OFFSET(offset, sizeof(int), &(rel->schema->numAttr));
    FROM_OFFSET(offset, sizeof(int), &(rel->schema->keySize));
    
    int i;
    for (i = 0; i<rel->schema->keySize; i++) {
        FROM_OFFSET(offset, sizeof(int), &rel->schema->keyAttrs[i]);
    }
    
    for (i = 0; i<rel->schema->numAttr; i++) {
        
        FROM_OFFSET(offset, sizeof(int), &rel->schema->dataTypes[i]);
        FROM_OFFSET(offset, sizeof(int), &rel->schema->typeLength[i]);
        FROM_OFFSET(offset, MAX_FIELDNAME_LENGTH, rel->schema->attrNames[i]);
    }
    
    if ((status = unpinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle)) != RC_OK) {
        return status;
    }
    
    return RC_OK;
}

//void dissolveSchema(Schema* schema)
//{
//
//    for (int i = 0; i<schema->numAttr; i++) {
//        free(schema->attrNames[i]);
//    }
//
//    free(schema->attrNames);
//    free(schema->typeLength);
//    free(schema->keyAttrs);
//    free(schema->dataTypes);
//    free(schema);
//
//}

//TODO-write data into schemaPage
RC closeTable (RM_TableData *rel)
{
    
    RM_TableMgmtData *tblMgmtData = rel->mgmtData;
    RC status;
    char* offset;
    
    if ((status = pinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle, (PageNumber)0))!=RC_OK) {
        return status;
    }
    
    offset = tblMgmtData->pageHandle.data;
    markDirty(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle);
    
    TO_OFFSET(offset, sizeof(int), &tblMgmtData->recordCount);
    TO_OFFSET(offset, sizeof(int), &tblMgmtData->freePage);
    
    unpinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle);
    
    shutdownBufferPool(&tblMgmtData->bufferPool);
    
    //freeSchema(rel->schema);
    
    free(rel->mgmtData);
    free(rel->name);
    //free(rel);
    
    return RC_OK;
}


RC deleteTable (char *name)
{
    RC status;
    
    if ((status = destroyPageFile(name))!=RC_OK) {
        return status;
    }
    
    return RC_OK;
}


int getNumTuples (RM_TableData *rel)
{
    RM_TableMgmtData *mgmtData = (RM_TableMgmtData*)rel->mgmtData;
    return mgmtData->recordCount;
}


//int getFreeSlot(BM_PageHandle* pageHandle, Schema *schema)
//{
//    //int bitMaplength = ((SLOTS_PER_PAGE(schema)/CHAR_BIT) + 1);
//
//    int slotcount = SLOTS_PER_PAGE(schema);
//    char *offset = pageHandle->data;
//    offset +=sizeof(int)*2;
//    uint8_t *bitMap = (uint8_t*)offset;
//
//    for (int i =0; i<slotcount; i++) {
//        if (get_bit(bitMap, i) == 0) {
//            return i;
//        }
//    }
//
//    return -1;
//
//}



//RC writeRecordToSlot(Record *record, BM_PageHandle *pageHandle, Schema *schema)
//{
//    if (record->id.page < 1 || record->id.slot < 0) {
//        return RC_RID_INVALID;
//    }
//
//    char* offset = pageHandle->data;
//    int bitMaplength = ((SLOTS_PER_PAGE(schema)/CHAR_BIT) + 1);
//
//    offset+=2*sizeof(int); //Moving over next and prev
//    offset+=bitMaplength;  //Moving overbitmap
//    offset+=record->id.slot*getRecordSize(schema);
//
//    TO_OFFSET(offset, getRecordSize(schema), record->data);
//
//    return RC_OK;
//}


RC writeRecordToSlot(Record *record, RM_TableMgmtData *tblMgmtData, Schema *schema)
{
    if (record->id.page < 1 || record->id.slot < 0) {
        return RC_RID_INVALID;
    }
    
    markDirty(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle);
    
    char* offset = tblMgmtData->pageInContext.data;
    offset+=record->id.slot*getRecordSize(schema);
    
    TO_OFFSET(offset, getRecordSize(schema), record->data);
    
    set_bit(tblMgmtData->pageInContext.bitMap, record->id.slot);
    
    return RC_OK;
}



RC fetchRecordFromSlot(Record *record, RM_PageData *pageData, Schema *schema)
{
    if (record->id.page < 1 || record->id.slot < 0) {
        return RC_RID_INVALID;
    }
    
    if (get_bit(pageData->bitMap, record->id.slot)!=1) {
        return RC_NONEXISTENT_RECORD;
    }
    
    char* offset = pageData->data;
    offset+=record->id.slot*getRecordSize(schema);
    
    FROM_OFFSET(offset, getRecordSize(schema), record->data);
    
    return RC_OK;
}



// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record)
{
    RM_TableMgmtData *tblMgmtData = rel->mgmtData;
    BP_MgmtData *poolMgmtData = tblMgmtData->bufferPool.mgmtData;
    RC status;
    
    //NO FreePages
    if(tblMgmtData->freePage == 0)
    {
        if ((status = appendEmptyBlock(poolMgmtData->fileHandle))!=RC_OK) {
            return status;
        }
        
        record->id.page = poolMgmtData->fileHandle->totalNumPages - 1;
        
        record->id.slot = 0;
        
        if ((status = pinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle, record->id.page))!=RC_OK) {
            return status;
        }
        
        //NewCode-------------------------------------------------------------------------------------
        char* offsetTemp = tblMgmtData->pageHandle.data;
        
        int a = 0; //Record count
        TO_OFFSET(offsetTemp,sizeof(int),&a);//0
        
        a = 0; //FreePage Number
        TO_OFFSET(offsetTemp, sizeof(int), &a);//4
        
        offsetTemp = NULL;
        
        //markDirty(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle);
        //NewCode End--------------------------------------------------------------------------------------
        
        loadPageIntoContext( &tblMgmtData->pageHandle, &tblMgmtData->pageInContext, rel->schema);
        
    }
    
    else    {
        record->id.page = tblMgmtData->freePage;
        
        if ((status = pinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle, record->id.page))!=RC_OK) {
            return status;
        }
        
        loadPageIntoContext( &tblMgmtData->pageHandle, &tblMgmtData->pageInContext, rel->schema);
        
        record->id.slot = getFreeSlot(&tblMgmtData->pageInContext, rel->schema);
        
        //False Free Page
//        if (record->id.slot == -1) {
//            
//            printf("Shouldn't have come here in first place");
//            
//            if ((status = unpinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle))!=RC_OK) {
//                return status;
//            }
//            
//            if ((status = appendEmptyBlock(poolMgmtData->fileHandle))!=RC_OK) {
//                return status;
//            }
//            
//            record->id.page =  poolMgmtData->fileHandle->totalNumPages - 1;
//            
//            record->id.slot = 0;
//            
//            if ((status = pinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle, record->id.page))!=RC_OK) {
//                return status;
//            }
//            
//            loadPageIntoContext( &tblMgmtData->pageHandle, &tblMgmtData->pageInContext, rel->schema);
//            
//        }
        
    }
    
    
    writeRecordToSlot(record, tblMgmtData, rel->schema);
    
    tblMgmtData->recordCount++;
    
    updateFreePages(rel);
    
    if ((status = unpinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle))!=RC_OK) {
        return status;
    }
    
    return RC_OK;
}


RC deleteRecord (RM_TableData *rel, RID id)
{
    RM_TableMgmtData* tblMgmtData = rel->mgmtData;
    
    RC status = pinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle, id.page);
    
    if (status!=RC_OK) {
        return status;
    }
    
    loadPageIntoContext(&tblMgmtData->pageHandle, &tblMgmtData->pageInContext, rel->schema);
    
    markDirty(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle);
    
    clear_bit(tblMgmtData->pageInContext.bitMap, id.slot);
    
    if ((status = unpinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle))!=RC_OK) {
        return status;
    }
    
    tblMgmtData->recordCount--;
    
    updateFreePages(rel);
    
    return RC_OK;
    
}


RC updateRecord (RM_TableData *rel, Record *record)
{
    RM_TableMgmtData* tblMgmtData = rel->mgmtData;
    
    RC status = pinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle, record->id.page);
    
    if (status!=RC_OK) {
        return status;
    }
    
    loadPageIntoContext(&tblMgmtData->pageHandle, &tblMgmtData->pageInContext, rel->schema);
    
    writeRecordToSlot(record, tblMgmtData, rel->schema);
    
    if ((status = unpinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle))!=RC_OK) {
        return status;
    }
    
    return RC_OK;
}


RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    RM_TableMgmtData* tblMgmtData = rel->mgmtData;
    
    record->id.page = id.page;
    record->id.slot = id.slot;
    
    RC status = pinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle, record->id.page);
    
    if (status!=RC_OK) {
        return status;
    }
    
    loadPageIntoContext(&tblMgmtData->pageHandle, &tblMgmtData->pageInContext, rel->schema);
    
    fetchRecordFromSlot(record, &tblMgmtData->pageInContext, rel->schema);
    
    if ((status = unpinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle))!=RC_OK) {
        return status;
    }
    
    
    return RC_OK;
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    
    RM_ScanMgmtData *scanMgmtData = (RM_ScanMgmtData*)malloc(sizeof(RM_ScanMgmtData));
    
    scanMgmtData->cond = cond;
    scanMgmtData->scanCount = 0;
    scanMgmtData->rid.slot = -1;
    scanMgmtData->rid.page = -1;
    
    scan->mgmtData = scanMgmtData;
    scan->rel = rel;
    
    return RC_OK;
}

int getNextUsedSlot(RM_PageData* pageData,int from, int SlotsPerPage)
{
    uint8_t* bitmap = pageData->bitMap;
    int i = from;
    
    while (!get_bit(bitmap, i)) {
        
        i++;
        
        if (i>SlotsPerPage) return -1;
    }
    
    return i;
}

RC next (RM_ScanHandle *scan, Record *record)
{
    RM_ScanMgmtData *scanMgmtData = scan->mgmtData;
    RM_TableMgmtData *tblMgmtData = scan->rel->mgmtData;
    
    Value *val = (Value*)malloc(sizeof(Value));
    val->v.boolV = TRUE;
    
    int slotsPerPage = SLOTS_PER_PAGE(scan->rel->schema);
    
    if (tblMgmtData->recordCount < 1) {
        return RC_RM_NO_MORE_TUPLES;
    }
    
    do
    {
        if (scanMgmtData->scanCount==0) {
            
            scanMgmtData->rid.page = 1;
            scanMgmtData->rid.slot = 0;
            
            pinPage(&tblMgmtData->bufferPool, &scanMgmtData->pageHandle, (PageNumber)scanMgmtData->rid.page);
            
            loadPageIntoContext(&scanMgmtData->pageHandle, &scanMgmtData->pageData, scan->rel->schema);
            
        }
        
        else if (scanMgmtData->scanCount == tblMgmtData->recordCount){
            
            unpinPage(&tblMgmtData->bufferPool, &scanMgmtData->pageHandle);
            
            scanMgmtData->rid.page = -1;
            scanMgmtData->rid.slot = -1;
            scanMgmtData->scanCount = 0;
            
            return RC_RM_NO_MORE_TUPLES;
        }
        
        else{
            
            scanMgmtData->rid.slot+=1;
            
            if (scanMgmtData->rid.slot == slotsPerPage) {
                
                unpinPage(&tblMgmtData->bufferPool, &scanMgmtData->pageHandle);
                
                scanMgmtData->rid.page+=1;
                scanMgmtData->rid.slot = 0;
                
                pinPage(&tblMgmtData->bufferPool, &scanMgmtData->pageHandle, (PageNumber)scanMgmtData->rid.page);
                loadPageIntoContext(&scanMgmtData->pageHandle, &scanMgmtData->pageData, scan->rel->schema);
                
            }
            
        }
        
        record->id.slot = scanMgmtData->rid.slot;
        record->id.page = scanMgmtData->rid.page;
        
        fetchRecordFromSlot(record, &scanMgmtData->pageData, scan->rel->schema);
        
        scanMgmtData->scanCount+=1;
        
        if (scanMgmtData->cond !=NULL) {
            evalExpr(record, scan->rel->schema, scanMgmtData->cond, &val);
        }
        
    }while(!val->v.boolV && scanMgmtData->cond !=NULL);
    
    free(val);
    
    return RC_OK;
}


RC closeScan (RM_ScanHandle *scan)
{
    RM_ScanMgmtData *scanMgmtData = scan->mgmtData;
    RM_TableMgmtData *tblMgmtData = scan->rel->mgmtData;
    
    //Required????
    if (scanMgmtData->scanCount>0) {
        unpinPage(&tblMgmtData->bufferPool, &tblMgmtData->pageHandle);
    }
    
    free(scan->mgmtData);
    scan->mgmtData = NULL;
    return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema)
{
    int size = 0;
    
    int i;
    for (i = 0; i<schema->numAttr; i++) {
        size+=schema->typeLength[i];
    }
    
    return size;
}


Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema* schema;
    schema = buildSchema(numAttr, keySize);
    
    int i;
    for ( i = 0; i<numAttr; i++) {
        
        strncpy(schema->attrNames[i], attrNames[i],MAX_FIELDNAME_LENGTH);
        schema->dataTypes[i] = dataTypes[i];
        
        switch (dataTypes[i]) {
                
            case DT_INT:
                schema->typeLength[i] = sizeof(int);
                break;
            case DT_BOOL:
                schema->typeLength[i] = sizeof(bool);
                break;
            case DT_FLOAT:
                schema->typeLength[i] = sizeof(float);
                break;
            case DT_STRING:
                schema->typeLength[i] = typeLength[i];
                break;
            default: printf("Unknown attribute type during schema creation");
                break;
                
        }
        
        if (i<keySize) {
            schema->keyAttrs[i] = keys[i];
        }
        
    }
    
    return schema;
}


RC freeSchema (Schema *schema)
{
    int i;
    for (i = 0; i<schema->numAttr; i++) {
        free(schema->attrNames[i]);
    }
    
    free(schema->attrNames);
    free(schema->typeLength);
    free(schema->keyAttrs);
    free(schema->dataTypes);
    free(schema);
    
    return RC_OK;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema)
{
    int recSize = getRecordSize(schema);
    char* recData = (char*) malloc(recSize);
    memset(recData, 0, recSize);
    
    Record* rec = (Record*) malloc(sizeof(Record));
    rec->data = recData;
    rec->id.page = -1;
    rec->id.slot = -1;
    
    *record = rec;
    
    return RC_OK;
}

RC freeRecord (Record *record)
{
    free(record->data);
    free(record);
    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    void* offset = record->data;
    *value = (Value*) malloc(sizeof(Value));
    int jump = 0;
    
    int i;
    for (i=0; i<attrNum; i++) {
        jump+= schema->typeLength[i];
    }
    
    offset+=jump;
    ((*value)->dt) = schema->dataTypes[attrNum];
    
    switch (schema->dataTypes[attrNum]) {
            
        case DT_INT:
            
        case DT_BOOL:
            
        case DT_FLOAT:
            
            memcpy(&((*value)->v), offset, schema->typeLength[attrNum]);
            break;
            
        case DT_STRING:
            
            ((*value)->v.stringV) = (char*)malloc(schema->typeLength[attrNum]+1);
            FROM_OFFSET(offset, schema->typeLength[attrNum], (*value)->v.stringV);
            (*value)->v.stringV[schema->typeLength[attrNum]] = '\0';
            
            break;
            
        default: printf("Unknown attribute type during reading attribute");
            break;
            
    }
    
    return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    void* offset = record->data;
    int jump = 0;
    
    int i;
    for (i=0; i<attrNum; i++) {
        jump+= schema->typeLength[i];
    }
    
    offset+=jump;
    
    switch (value->dt) {
            
        case DT_INT:
            
        case DT_BOOL:
            
        case DT_FLOAT:
            
            TO_OFFSET(offset, schema->typeLength[attrNum], &value->v);
            break;
            
        case DT_STRING:
            
            TO_OFFSET(offset, schema->typeLength[attrNum], value->v.stringV);
            break;
            
        default: printf("Unknown attribute type during reading attribute");
            break;
            
    }
    
    
    return RC_OK;
}
