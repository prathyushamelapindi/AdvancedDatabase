//
//  storage_mgr.c
//  CS525
//
//  Created by raghuveer vemuri on 9/7/15.
//  Copyright (c) 2015 raghuveer vemuri. All rights reserved.
//

#include "storage_mgr.h"
#include <stdio.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "dberror.h"
#define I386_PGBYTES 4096

//Method to check if filehandle is properly initialized
RC authorizeHandle(SM_FileHandle *fileHandle)
{
    if (fileHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    if (fileHandle->mgmtInfo == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    return RC_OK;
}

/*Method to read data; will be invoked by several read methods
 @Params: page number, File-handle and page-handle(used as destination for read)
*/
RC readData(int pageNum, SM_FileHandle *fileHandle, SM_PageHandle memPage){
    
    //Should we reauthorize?
    if(authorizeHandle(fileHandle)!=RC_OK) return RC_FILE_HANDLE_NOT_INIT;
    
    FILE* filePtr = fileHandle->mgmtInfo;
    
    //checks for invalid pages
    if(pageNum >= fileHandle->totalNumPages || pageNum<0)
        return RC_READ_NON_EXISTING_PAGE;
    
    //seeks the start position of data
    if(fseek(filePtr, pageNum*PAGE_SIZE, SEEK_SET)!=0)
        return RC_SEEK_ERROR;
    
    fileHandle->curPagePos = pageNum;
    
    //reads the data and verifies if the read is successful
    if(fread(memPage, PAGE_SIZE, 1, filePtr) != 1)
        return RC_READ_FAILED;
    
    return RC_OK;
}

/*Method to write data; will be invoked by several write methods
 @Params: page number, File-handle and page-handle(used as source for write)
 */
RC writeData(int pageNum, SM_FileHandle *fileHandle, SM_PageHandle memPage){
    
    //Should we reauthorize?
    if(authorizeHandle(fileHandle)!=RC_OK) return RC_FILE_HANDLE_NOT_INIT;
    
    FILE* filePtr = fileHandle->mgmtInfo;
    
    //checks for invalid pages
    if(pageNum >= fileHandle->totalNumPages)
        ensureCapacity(pageNum, fileHandle);
    //return RC_WRITE_NON_EXISTING_PAGE;

    //seeks the start position of data
    if(fseek(filePtr, pageNum*PAGE_SIZE, SEEK_SET)!=0)
        return RC_SEEK_ERROR;
    
    fileHandle->curPagePos = pageNum;

    //writes the data and verifies if the write is successful
    if(fwrite(memPage, PAGE_SIZE, 1, filePtr) != 1)
        return RC_WRITE_FAILED;
    
    return RC_OK;

}

void initStorageManager (void){
    
}

/*
 @Description: Creates a new file with specified name
 @Args: address of fileName
 @Returns: RC_OK on success; RC_FILE_NOT_FOUND on failure
 */
RC createPageFile (char *fileName){
    
    FILE *newFilePtr = fopen(fileName, "w+");
    
    if(newFilePtr == NULL)
        return RC_FILE_CREATION_ERROR;
    
    //allocate memory of size PAGE_SIZE and fill it with NULL
    char *pg = (char*)calloc(PAGE_SIZE,sizeof(char));
    memset(pg,'\0', PAGE_SIZE);
    
    //writing NULL page into file
    int wrtChunk = (int)fwrite(pg, PAGE_SIZE, 1, newFilePtr);
    free(pg);
    
    //returning error as file write failed
    if(wrtChunk != 1)
    {
        fclose(newFilePtr);
        return RC_FILE_EMPTY_PAGE_ADDITION_ERROR;
    }
    
    return RC_OK;
    
}


/*
 Method opens the file specified by the file name and initilaizes the handle
*/
RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    
    FILE *filePtr = fopen(fileName, "r+");
    
    if(filePtr == NULL)
        return RC_FILE_NOT_FOUND;
    
    if(fseek(filePtr, 0, SEEK_END)!=0) return RC_SEEK_ERROR;
    
    long size = ftell(filePtr);
    rewind(filePtr);
    
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;
    fHandle->totalNumPages = (int)size/PAGE_SIZE;
    fHandle->mgmtInfo = filePtr;
    
    return RC_OK;
}


//closes the specified file referred by the fhandle
RC closePageFile (SM_FileHandle *fHandle){
    
    if(fclose(fHandle->mgmtInfo) == 0)
    {
//        free(fHandle->fileName);
//        fHandle->fileName = NULL;
//        free(fHandle->mgmtInfo);
//        fHandle->mgmtInfo = NULL;
//        free(fHandle);
        
        return RC_OK;
    }
    
    return RC_FILE_CLOSE_ERROR;
}


//Removes the file named by the fileName parameter
RC destroyPageFile(char *fileName){
    
    if(remove(fileName))
    {
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}


//Read the data from the file referred by fHandle and pageNum parameters into memPage
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    
    return readData(pageNum, fHandle, memPage);
    
}


//returns the current page position of the fileHandle
int getBlockPos (SM_FileHandle *fHandle){
    
    authorizeHandle(fHandle);
    
    return fHandle->curPagePos;
    
}


//Reads the first page/block from the file referred by the fHandle into memPage
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    
    authorizeHandle(fHandle);
    
    return readData(0, fHandle, memPage);
}


//reads the previous block of the file referred by the fHandle into memPage
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

    authorizeHandle(fHandle);
    
    return readData(fHandle->curPagePos - 1, fHandle, memPage);
}

//reads the next block of the file referred by the fHandle into memPage
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

    authorizeHandle(fHandle);
    
    return readData(fHandle->curPagePos+1, fHandle, memPage);
    
}

//reads the last block of the file referred by the fHandle into memPage
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

    authorizeHandle(fHandle);
    
    return readData(fHandle->totalNumPages-1, fHandle, memPage);
}


//writes the block of the file referred by the fHandle and pageNum into memPage
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    
    authorizeHandle(fHandle);
    
    return writeData(pageNum, fHandle, memPage);
}

//writes into current block of the file referred by the fHandle from memPage
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    
    authorizeHandle(fHandle);
    
    return writeData(fHandle->curPagePos, fHandle, memPage);
}

//appends a block to the file refered by the fHandle
RC appendEmptyBlock (SM_FileHandle *fHandle){
    
    authorizeHandle(fHandle);
    
    FILE* filePtr = fHandle->mgmtInfo;
    
    if(fseek(filePtr, 0, SEEK_END) !=0)
    {
        return RC_SEEK_ERROR;
    }
    
    //allocate memory of size PAGE_SIZE and fill it with NULL
    char *pg = (char*)calloc(PAGE_SIZE,sizeof(char));
    memset(pg,0, PAGE_SIZE);
        
    if (fwrite(pg, PAGE_SIZE, 1, filePtr)!=1) {
        free(pg);
        return RC_FILE_EMPTY_PAGE_ADDITION_ERROR;
    }
    
    free(pg);
    fHandle->totalNumPages+=1;
    fHandle->curPagePos = (int) ftell(filePtr)/PAGE_SIZE;
    
    return RC_OK;

}


//Ensures that available number of pages are available in file referred by fHandle.
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    
    authorizeHandle(fHandle);
    
    if (fHandle->totalNumPages<numberOfPages) {
        
        int pgsToBeAdded = numberOfPages - fHandle->totalNumPages;
        
        int i;
        for (i = 0; i<pgsToBeAdded; i++) {
            appendEmptyBlock(fHandle);
        }
    }
    
    return RC_OK;
}








