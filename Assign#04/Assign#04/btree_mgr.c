//
//  btree_mgr.c
//  Assign#04
//
//  Created by raghuveer vemuri on 11/28/15.
//  Copyright © 2015 raghuveer vemuri. All rights reserved.
//

#include <string.h>
#include <stdlib.h>
#include "btree_mgr.h"
#include "buffer_mgr.h"
#include "expr.h"
#include "page_table.h"

#include "storage_mgr.h"

#define I386_PGBYTES 4096 //remove later


#define TO_OFFSET(offset,size,src) memcpy(offset,src,size);offset+=size;
#define FROM_OFFSET(offset,size,dest) memcpy(dest,offset,size);offset+=size;


typedef struct BT_Node{
    
    bool isleaf;
    int* keys;
    int* pagePointers;
    int* slotPointers; //active only for LeafNodes
    
    int parent;
    int activeKeys;
    int next;          // Used for queue.(REMOVE/DELETE ONLY COMNT)
    
}BT_Node;


typedef struct BT_MgmtData {
    
    PageNumber rootPage;
    
    int activeNodes;
    int activeElements;
    
    DataType keyType;
    int order;
    
    BM_BufferPool bPool;
    BM_PageHandle pHandle;
    BT_Node nodeInContext;
    
} BT_MgmtData;


// init and shutdown index manager

//TO-DO
RC initIndexManager (void *mgmtData){
    return RC_OK;
}

//TO-DO
RC shutdownIndexManager (){
    return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n){
    
    char data[PAGE_SIZE];
    char* offset = data;
    
    //TO-DO add logic to limit overflow if node doesn't fit in one page.
    
    memset(offset, 0, PAGE_SIZE);
    
    int temp = 1;
    
    TO_OFFSET(offset, sizeof(int), &temp);//root Node
    temp = 0;
    TO_OFFSET(offset, sizeof(int), &temp);//active nodes
    TO_OFFSET(offset, sizeof(int), &temp);//active elements
    TO_OFFSET(offset, sizeof(DataType), &keyType);//DataType
    temp = n;
    TO_OFFSET(offset, sizeof(int), &temp);//order
    
    SM_FileHandle fHandle;
    RC status = createPageFile(idxId);
    
    if (status!=RC_OK) {
        return status;
    }
    
    if ((status=openPageFile(idxId, &fHandle))!=RC_OK) {
        return status;
    }
    
    if ((status = writeBlock(0, &fHandle, data))!=RC_OK) {
        return status;
    }
    
    if ((status = closePageFile(&fHandle))!=RC_OK) {
        return status;
    }
    
    return RC_OK;
}


RC openBtree (BTreeHandle **tree, char *idxId){
    
    
    BTreeHandle* treeHandle = (BTreeHandle*) malloc(sizeof(BTreeHandle));
    memset(treeHandle, 0, sizeof(BTreeHandle));
    
    
    treeHandle->idxId = strdup(idxId);
    
    BT_MgmtData* treeMgmtData = (BT_MgmtData*) malloc(sizeof(BT_MgmtData));
    memset(treeMgmtData, 0, sizeof(BT_MgmtData));
    
    RC status;
    
    if ((status=initBufferPool(&treeMgmtData->bPool, idxId, 1024, RS_FIFO, NULL))!=RC_OK) {
        return status;
    }
    
    if((status = pinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle, (PageNumber)0))!=RC_OK){
        return status;
    }
    
    char* offset = treeMgmtData->pHandle.data;
    
    FROM_OFFSET(offset, sizeof(int), &treeMgmtData->rootPage);
    FROM_OFFSET(offset, sizeof(int), &treeMgmtData->activeNodes);
    FROM_OFFSET(offset, sizeof(int), &treeMgmtData->activeElements);
    FROM_OFFSET(offset, sizeof(DataType), &treeMgmtData->keyType);
    FROM_OFFSET(offset, sizeof(int), &treeMgmtData->order);
    
    if((status = unpinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle))!=RC_OK){
        return status;
    }
    
    treeHandle->keyType = treeMgmtData->keyType;
    treeHandle->mgmtData = treeMgmtData;
    *tree = treeHandle;
    
    return RC_OK;
    
}


RC closeBtree (BTreeHandle *tree){
    
    RC status;
    BT_MgmtData* treeMgmtData = (BT_MgmtData*)tree->mgmtData;
    
    
    if((status = pinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle, (PageNumber)0))!=RC_OK){
        return status;
    }
    
    char* offset = treeMgmtData->pHandle.data;
    
    
    TO_OFFSET(offset, sizeof(int), &treeMgmtData->rootPage);
    TO_OFFSET(offset, sizeof(int), &treeMgmtData->activeNodes);
    TO_OFFSET(offset, sizeof(int), &treeMgmtData->activeElements);
    //TO_OFFSET(offset, sizeof(DataType), &treeMgmtData->keyType);
    //TO_OFFSET(offset, sizeof(int), &treeMgmtData->order);
    
    if((status = unpinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle))!=RC_OK){
        return status;
    }
    
    if((status = shutdownBufferPool(&treeMgmtData->bPool))!=RC_OK){
        return status;
    }
    
    free(treeMgmtData);
    free(tree->idxId);
    free(tree);
    
    return RC_OK;
}


RC deleteBtree (char *idxId){
    
    RC status;
    
    if ((status = destroyPageFile(idxId))!=RC_OK) {
        return status;
    }
    
    return RC_OK;
}


// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result){
    
    BT_MgmtData* treeMgmtData = ( BT_MgmtData*)tree->mgmtData;
    
    *result = treeMgmtData->activeNodes;
    
    return RC_OK;
    
}


RC getNumEntries (BTreeHandle *tree, int *result){
    
    BT_MgmtData* treeMgmtData = ( BT_MgmtData*)tree->mgmtData;
    
    *result = treeMgmtData->activeElements;
    
    return RC_OK;
    
}


RC getKeyType (BTreeHandle *tree, DataType *result){
    
    BT_MgmtData* treeMgmtData = ( BT_MgmtData*)tree->mgmtData;
    
    *result = treeMgmtData->keyType;
    
    return RC_OK;
    
}

static RC flushParent(BT_MgmtData* treeMgmtData, BT_Node* node){
    
    char* offset;
    
    if (node->isleaf) {
        offset = (char*)node->slotPointers;
        offset+= sizeof(int)*treeMgmtData->order;
    }
    else{
        offset = (char*)node->pagePointers;
        offset+= sizeof(int)*(treeMgmtData->order - 1);
    }
    
    TO_OFFSET(offset, sizeof(int), &node->parent);
    return RC_OK;
}

static RC flushActiveKeys(BT_MgmtData* treeMgmtData, BT_Node* node){
    
    char* offset;
    
    if (node->isleaf) {
        offset = (char*)node->slotPointers;
        offset+= sizeof(int)*treeMgmtData->order;
    }
    else{
        offset = (char*)node->pagePointers;
        offset+= sizeof(int)*(treeMgmtData->order - 1);
    }
    
    offset+=sizeof(node->parent);
    
    TO_OFFSET(offset, sizeof(int), &node->activeKeys);
    return RC_OK;
}


//static RC loadNodeIntoContext(BT_MgmtData* treeMgmtData){
//    
//    char* offset = treeMgmtData->pHandle.data;
//    
//    BT_Node* node = &treeMgmtData->nodeInContext;
//    
//    FROM_OFFSET(offset, sizeof(bool), &node->isleaf);
//    
//    node->keys = (int*)offset;
//    offset+=(treeMgmtData->order-1)*(sizeof(int));
//    
//    node->Pagepointers = (int*)offset;
//    offset+=(treeMgmtData->order)*(sizeof(int));
//    
//    if (node->isleaf) {
//        node->slotPointers = (int*)offset;
//        offset+=(treeMgmtData->order)*(sizeof(int));
//    }
//    else
//        node->slotPointers = NULL;
//    
//    
//    FROM_OFFSET(offset, sizeof(int), &node->parent);
//    FROM_OFFSET(offset, sizeof(int), &node->activeKeys);
//    
//    return RC_OK;
//
//}

static RC loadNodeIntoContext(BT_MgmtData* treeMgmtData, BT_Node* node, BM_PageHandle* pHandle){
    
    char* offset = pHandle->data;
    
    FROM_OFFSET(offset, sizeof(bool), &node->isleaf);
    
    node->keys = (int*)offset;
    offset+=(treeMgmtData->order-1)*(sizeof(int));
    
    node->pagePointers = (int*)offset;
    offset+=(treeMgmtData->order)*(sizeof(int));
    
    if (node->isleaf) {
        node->slotPointers = (int*)offset;
        offset+=(treeMgmtData->order)*(sizeof(int));
    }
    else
        node->slotPointers = NULL;
    
    
    FROM_OFFSET(offset, sizeof(int), &node->parent);
    FROM_OFFSET(offset, sizeof(int), &node->activeKeys);
    
    return RC_OK;
    
}


//static RC pinAndLoadNodeIntoContext(BT_MgmtData* treeMgmtData, PageNumber pNum, bool shouldUnpin){
//    
//    RC status;
//    
//    if (shouldUnpin) {
//        if ((status = unpinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle)) != RC_OK) {
//            return status;
//        }
//    }
//    
//    if ((status = pinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle, (PageNumber)(pNum))) != RC_OK) {
//        return status;
//    }
//    
//    return loadNodeIntoContext(treeMgmtData);
//    
//}

static RC pinAndLoadNodeIntoContext(BT_MgmtData* treeMgmtData, BT_Node* node, BM_PageHandle* pHandle,  PageNumber pNum, bool shouldUnpin){
    
    RC status;
    
    if (shouldUnpin) {
        if ((status = unpinPage(&treeMgmtData->bPool, pHandle)) != RC_OK) {
            return status;
        }
    }
    
    if ((status = pinPage(&treeMgmtData->bPool, pHandle, (PageNumber)(pNum))) != RC_OK) {
        return status;
    }
    
    return loadNodeIntoContext(treeMgmtData, node, pHandle);
    
}

int searchInNode (BT_Node* node, Value *key) {
    
    int i = 0;
    Value smRes, tempHolder;
    tempHolder.dt = key->dt;
    //tempHolder.v.intV = key->v.intV;
    
    while (i<node->activeKeys) {
        tempHolder.v.intV = node->keys[i];
        valueSmaller(key, &tempHolder, &smRes);
        //valueEquals(key, &tempHolder, &eqRes);
        
        if (!smRes.v.boolV) i++;
        else break;
        
    }
    
    return node->pagePointers[i];
}

bool searchInLeafNode (BT_Node* node, Value *key, int* pos) {
    
    int i = 0;
    Value eqRes, smRes, tempHolder;
    tempHolder.dt = key->dt;
    //tempHolder.v.intV = key->v.intV;
    
    while (i<node->activeKeys) {
        tempHolder.v.intV = node->keys[i];
        valueSmaller(key, &tempHolder, &smRes);
        valueEquals(key, &tempHolder, &eqRes);
        
        if (!eqRes.v.boolV && !smRes.v.boolV) i++;
        
        else {
            *pos = i;
            
            if (eqRes.v.boolV) return true;
            else return false;
        }
        
    }
    
    return false;
}

// index access

PageNumber find_leaf(BT_MgmtData* treeMgmtData, Value *key){
    
    
    RC status;
    int nextNodePage = -1, i = 0;
    Value smRes, eqRes, tempHolder;
    tempHolder.dt = key->dt;
    BT_Node node;
    BM_PageHandle pHandle;
    
    if ((status = pinAndLoadNodeIntoContext(treeMgmtData, &node, &pHandle, (PageNumber)(treeMgmtData->rootPage), false)) != RC_OK) {
        printf("error in pinning, findleaf %d\n",(int)status);
    }
    
    
    if (!node.activeKeys>0) {
        printf("Empty tree\n");
        
        if ((status = unpinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle)) != RC_OK) {
            printf("error in unpinning, findleaf %d\n",(int)status);
        }
        
        return -1;
    }
    
    while (!node.isleaf) {
        
        nextNodePage = searchInNode(&node, key);
        
        if ((status = pinAndLoadNodeIntoContext(treeMgmtData, &node, &pHandle, (PageNumber)(nextNodePage), true)) != RC_OK) {
            printf("error in pinning, findleaf %d\n",(int)status);
        }
    }
    
    
    if ((status = unpinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle)) != RC_OK) {
        printf("error in unpinning, findleaf %d\n",(int)status);
    }

    return nextNodePage;
}


RC findKey (BTreeHandle *tree, Value *key, RID *result){
    
    Value tempHolder;
    tempHolder.dt = key->dt;
    Value eqRes;
    RC status;
    int tempPageHolder = -1;
    BT_Node node;
    BM_PageHandle pHandle;
    
    BT_MgmtData* treeMgmtData = tree->mgmtData;
    
    PageNumber leafPage = find_leaf(treeMgmtData, key);
    
    if (leafPage<1) return RC_LEAF_SEARCH_FAILED;
    
    pinAndLoadNodeIntoContext(treeMgmtData, &node, &pHandle, leafPage, false);
    
    
    if (!node.isleaf){
        
        if ((status = unpinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle)) != RC_OK) {
            return status;
        }
        
        return RC_LEAF_SEARCH_FAILED;}
    
    if(searchInLeafNode(&node, key, &tempPageHolder)){
        
        if ((status = unpinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle)) != RC_OK) {
            return status;
        }
        
        return RC_IM_KEY_NOT_FOUND;
    }
    
//    int i =0;
//    for (i = 0; i<node->activeKeys; i++) {
//        tempHolder.v.intV = node->keys[i];
//        valueEquals(key, &tempHolder, &eqRes);
//        
//        if (eqRes.v.boolV) {
//            break;
//        }
//    }
//    
//    if (i>=node->activeKeys) {
//        return RC_IM_KEY_NOT_FOUND;
//    }
    
    result->page = node.pagePointers[tempPageHolder];
    result->slot = node.slotPointers[tempPageHolder];
    
    if ((status = unpinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle)) != RC_OK) {
                return status;
            }
    
    return status;
    
}

static int createNode(BTreeHandle* treeHandle){
    
    BT_MgmtData* treeMgmtData = treeHandle->mgmtData;
    BP_MgmtData* poolMgmtData = treeMgmtData->bPool.mgmtData;
    
    RC status;
    
    
    if ((status = appendEmptyBlock(poolMgmtData->fileHandle))!=RC_OK) {
        printf("error in appending emptyblock, create Node %d\n",(int)status);
    }
    
    treeMgmtData->activeNodes++;
    return (poolMgmtData->fileHandle->totalNumPages - 1);
    
}

static int createLeafNode(BTreeHandle* treeHandle){
    
    BT_MgmtData* treeMgmtData = treeHandle->mgmtData;
    int ret =  createNode(treeHandle);
    RC status;
    
    if ((status = pinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle, (PageNumber)ret)) != RC_OK) {
        printf("error in pinning create LeafNode %d\n",(int)status);
    }
    
    
    char* offset = treeMgmtData->pHandle.data;
    
    bool temp = true;
    TO_OFFSET(offset, sizeof(bool), &temp);
    
    if ((status = unpinPage(&treeMgmtData->bPool, &treeMgmtData->pHandle)) != RC_OK) {
        printf("error in unpinning create LeafNode %d\n",(int)status);
    }
    
    return ret;
    
}


RC insetIntoLeaf(BTreeHandle *tree, BT_Node* node, RID rid, Value* key){
    
    BT_MgmtData *treeMgmtData = tree->mgmtData;
    int i = 0, pos = 0;
    
    if (searchInLeafNode(node, key, &pos)) {
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    
    for (i = node->activeKeys; i > pos; i--) {
        node->keys[i] = node->keys[i-1];
        node->pagePointers[i] = node->pagePointers[i-1];
        node->slotPointers[i] = node->slotPointers[i-1];
    }
    
    node->keys[pos] = key->v.intV;
    node->pagePointers[pos] = rid.page;
    node->slotPointers[pos] = rid.slot;
    
    node->activeKeys++;
    treeMgmtData->activeElements++;
    
    return RC_OK;
}

int findWhereItGoes(BT_Node* node, Value* key)
{
    int i = 0;
    Value eqRes, smRes, tempHolder;
    tempHolder.dt = key->dt;
    //tempHolder.v.intV = key->v.intV;
    
    while (i<node->activeKeys) {
        tempHolder.v.intV = node->keys[i];
        valueSmaller(key, &tempHolder, &smRes);
        //valueEquals(key, &tempHolder, &eqRes);
        
        if (!smRes.v.boolV) i++;
        
        else {
            return i;
        }
    }
    
    return i;
}

RC insertIntoNewRoot(BTreeHandle* tree, BT_Node* node1, PageNumber pgNum1, BT_Node* node2, PageNumber pgNum2, Value* val){
    
    BT_MgmtData* treeMgmtData = tree->mgmtData;
    BT_Node root;
    BM_PageHandle pHandle;
    RC status;
    
    PageNumber rootPage = createNode(tree);
    
    if ((status = pinAndLoadNodeIntoContext(treeMgmtData, &root, &pHandle, rootPage, false)) != RC_OK) {
        return status;
    }
    
    root.parent = -1;
    root.keys[0] = val->v.intV;
    root.pagePointers[0] = pgNum1;
    root.pagePointers[1] = pgNum2;
    root.activeKeys = 1;
    
    node1->parent = rootPage;
    node2->parent = rootPage;
    
    flushParent(tree->mgmtData, &root);
    flushActiveKeys(tree->mgmtData, &root);
    
    if ((status = unpinPage(&treeMgmtData->bPool, &pHandle)) != RC_OK) {
        return status;
    }
    
    return RC_OK;
}

int getNodeIndex(BT_Node* parent, int pg){
    
    int i = 0;
    
    while (i<=parent->activeKeys) {
        
        if (parent->pagePointers[i] == pg) {
            break;
        }
        
        i++;
    }
    
    return i;
}


RC insertIntoNode(BTreeHandle* tree, BT_Node* parent, int left, BT_Node* childNode1, PageNumber pgNum1, BT_Node* childNode2, PageNumber pgNum2, Value* key){
    
    int i;
    
    for (i = parent->activeKeys; i>left; i--) {
        parent->keys[i] = parent->keys[i-1];
        parent->pagePointers[i+1] = parent->pagePointers[i];
    }
    
    parent->keys[left] = key->v.intV;
    parent->pagePointers[left+1] = pgNum2;
    parent->activeKeys++;
    
    return RC_OK;
}

RC updateParents(BTreeHandle* tree, BT_Node* newNode, int pageNum){
    
    BT_MgmtData* treeMgmtData = tree->mgmtData;
    RC status;
    int i = 0;
    BT_Node node;
    BM_PageHandle pHandle;
    int eachPage;
    
    while (i<=newNode->activeKeys) {
        
        eachPage = newNode->pagePointers[i];
        
        if((status = pinAndLoadNodeIntoContext(tree->mgmtData, &node, &pHandle, eachPage, TRUE))!=RC_OK){
            return status;
        }
        
        node.parent = pageNum;
        flushParent(tree->mgmtData, &node);
        
        if ((status = unpinPage(&treeMgmtData->bPool, &pHandle))) {
            return status;
        }
        
        i++;
    }
    
    return RC_OK;
}



RC insertIntoNodeAfterSplitting(BTreeHandle* tree, BT_Node* parent, int left, BT_Node* childNode1, PageNumber pgNum1, BT_Node* childNode2, PageNumber pgNum2, Value* key){
    
    BT_MgmtData *treeMgmtData = tree->mgmtData;
    PageNumber pageNum;
    BT_Node newNode;
    BM_PageHandle newPHandle;
    Value newKey;
    RC status;
    int splitPoint;
    
    newKey.dt = key->dt;
    
    int* temp_keys = (int*)malloc(treeMgmtData->order*sizeof(int));
    int* temp_pagePointers = (int*)malloc((treeMgmtData->order+1)*sizeof(int));
    
    splitPoint = (treeMgmtData->order + 1)/2;
    
    int i,j;
    
    for (i = 0, j = 0; i<parent->activeKeys+1; i++,j++) {
        if (j==left+1) {
            j++;
        }
        
        temp_pagePointers[j] = parent->pagePointers[i];
    }
    
    
    for (i = 0, j = 0; i<parent->activeKeys; i++,j++) {
        if (j==left) {
            j++;
        }
        
        temp_keys[j] = parent->keys[i];
    }
    
    temp_pagePointers[left+1] = pgNum2;
    temp_keys[left] = key->v.intV;
    
    pageNum = createNode(tree);
    if ((status = pinAndLoadNodeIntoContext(treeMgmtData, &newNode, &newPHandle, pageNum, false)) != RC_OK) {
        return status;
    }
    
    parent->activeKeys = 0;
    
    for (i = 0; i<splitPoint-1; i++) {
        
        parent->pagePointers[i] = temp_pagePointers[i];
        parent->keys[i] = temp_keys[i];
        parent->activeKeys++;
        
    }
    
    parent->pagePointers[i] = temp_pagePointers[i];
    newKey.v.intV = temp_keys[splitPoint-1];
    
    for (++i, j=0; i<treeMgmtData->order; i++,j++) {
        newNode.pagePointers[j] = temp_pagePointers[i];
        newNode.keys[j] = temp_keys[i];
        newNode.keys++;
    }
    
    newNode.pagePointers[j] = temp_pagePointers[i];
    free(temp_pagePointers);
    free(temp_keys);
    
    newNode.parent = parent->parent;
    
    if((status = updateParents(tree, &newNode, pageNum))!= RC_OK){
        return status;
    }
    
    flushActiveKeys(treeMgmtData, &newNode);
    flushParent(treeMgmtData, &newNode);
    
    
    if ((status = insertIntoParent(tree, <#BT_Node *childNode1#>, <#PageNumber pgNum1#>, <#BT_Node *childNode2#>, <#PageNumber pgNum2#>, newKey)) != RC_OK) {
        return status;
    }

    
    if ((status = unpinPage(&treeMgmtData->bPool, &newPHandle)) != RC_OK) {
        return status;
    }
    
    return RC_OK;
}


RC insertIntoParent(BTreeHandle* tree, BT_Node* childNode1, PageNumber pgNum1, BT_Node* childNode2, PageNumber pgNum2, Value* key){
    
    BT_MgmtData* treeMgmtData = tree->mgmtData;
    RC status;
    PageNumber pageNum;
    BT_Node node;
    BM_PageHandle pHandle;
    
    int left;
    
    if (childNode1->parent<1) {
        
        if ((status = insertIntoNewRoot(tree, childNode1, pgNum1, childNode2, pgNum2, key))!=RC_OK) {
            return status;
        }
        
        return RC_OK;
    }
    
    pageNum = (PageNumber)childNode1->parent;
    
    if ((status = pinAndLoadNodeIntoContext(treeMgmtData, &node, &pHandle, pageNum, false)) != RC_OK) {
        return status;
    }
    
    
    left = getNodeIndex(&node,(int)pgNum1);
    
    
    if (node.activeKeys<treeMgmtData->order - 1) {
        
        insertIntoNode(tree, &node, left, childNode1, pgNum1, childNode2, pgNum2, key);
        
        
        flushActiveKeys(treeMgmtData, &node);
        flushParent(treeMgmtData, &node);
        
        if ((status = unpinPage(&treeMgmtData->bPool, &pHandle)) != RC_OK) {
            return status;
        }
        
        return status;
    }
    
    
    insertIntoNodeAfterSplitting(tree, &node, left, childNode1, pgNum1, childNode2, pgNum2, key);
    
    flushActiveKeys(treeMgmtData, &node);
    flushParent(treeMgmtData, &node);
    
    if ((status = unpinPage(&treeMgmtData->bPool, &pHandle)) != RC_OK) {
        return status;
    }
    
    return status;
}


RC splitAndInsertIntoLeaf(BTreeHandle *tree, BT_Node* node, PageNumber nodePageNum, RID rid, Value* key){
    
    BT_MgmtData *treeMgmtData = tree->mgmtData;
    PageNumber pageNum;
    BT_Node newNode;
    BM_PageHandle newPHandle;
    Value newKey;
    RC status;
    
    int* temp_keys = (int*)malloc(treeMgmtData->order*sizeof(int));
    int* temp_pagePointers = (int*)malloc(treeMgmtData->order*sizeof(int));
    int* temp_slotPointers = (int*)malloc(treeMgmtData->order*sizeof(int));
    
    int splitPoint, whereItGoes;
    
    splitPoint = (treeMgmtData->order + 1)/2;
    pageNum = createLeafNode(tree);
    whereItGoes = findWhereItGoes(node,key);
    
    int i = 0, j = 0;
    while (j < treeMgmtData->order) {
        
        if (j==whereItGoes) {
            temp_keys[j] = key->v.intV;
            temp_pagePointers[j] = rid.page;
            temp_slotPointers[j] = rid.slot;
            j++;
            continue;
        }
        
        temp_keys[j] = node->keys[i];
        temp_pagePointers[j] = node->pagePointers[i];
        temp_slotPointers[j] = node->slotPointers[i];
        i++;
        j++;
    }
    
    node->activeKeys = 0;
    
    for (i = 0; i<splitPoint; i++) {
        node->keys[i] = temp_keys[i];
        node->pagePointers[i] = temp_pagePointers[i];
        node->slotPointers[i] = temp_slotPointers[i];
        node->activeKeys++;
    }
    
    if ((status = pinAndLoadNodeIntoContext(treeMgmtData, &newNode, &newPHandle, pageNum, false)) != RC_OK) {
        return status;
    }
    
    for (i = splitPoint, j = 0; i<treeMgmtData->order; i++,j++) {
        newNode.keys[j] = temp_keys[i];
        newNode.pagePointers[j] = temp_pagePointers[i];
        newNode.slotPointers[j] = temp_slotPointers[i];
        newNode.activeKeys++;
        
    }
    
    free(temp_keys);
    free(temp_pagePointers);
    free(temp_slotPointers);
    
    newNode.pagePointers[treeMgmtData->order - 1] = node->pagePointers[treeMgmtData->order - 1];
    node->pagePointers[treeMgmtData->order - 1] = pageNum;
    newNode.parent = node->parent;
    
    flushActiveKeys(treeMgmtData, &newNode);
    
    newKey.dt = key->dt;
    newKey.v.intV = newNode.keys[0];
    
    insertIntoParent(tree,node,nodePageNum,&newNode,pageNum,&newKey);
    
    flushParent(treeMgmtData, &newNode);
    
    if ((status = unpinPage(&treeMgmtData->bPool, &newPHandle)) != RC_OK) {
        return status;
    }
    
    return RC_OK;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid){
    
    BT_MgmtData* treeMgmtData = tree->mgmtData;
    //BP_MgmtData* poolMgmtData = treeMgmtData->bPool.mgmtData;
    RC status;
    PageNumber pageNum;
    BT_Node node;
    BM_PageHandle pHandle;
    
    
    if (!treeMgmtData->activeElements) {
        
        pageNum = createLeafNode(tree);
        treeMgmtData->rootPage = pageNum;
        
        pinAndLoadNodeIntoContext(treeMgmtData, &node, &pHandle, pageNum, false);
        node.parent = -1;
        flushParent(treeMgmtData, &node);
        
        
        if ((status = insetIntoLeaf(tree, &node, rid, key)) != RC_OK) {
            return status;
        }
        
        flushActiveKeys(treeMgmtData, &node);
        
        if ((status = unpinPage(&treeMgmtData->bPool, &pHandle)) != RC_OK) {
            return status;
        }
        
        return status;
        
    }
    
    pageNum = find_leaf(treeMgmtData, key);
    pinAndLoadNodeIntoContext(treeMgmtData, &node, &pHandle, pageNum, false);
    
    if (node.activeKeys<(treeMgmtData->order - 1)) {
        
        if ((status = insetIntoLeaf(tree, &node, rid, key)) != RC_OK) {
            return status;
        }
        
        flushActiveKeys(treeMgmtData, &node);
        
        if ((status = unpinPage(&treeMgmtData->bPool, &pHandle)) != RC_OK) {
            return status;
        }
        
        return status;
        
    }
    
    else{
        
        if ((status = splitAndInsertIntoLeaf(tree, &node, pageNum, rid, key))!=RC_OK) {
            return status;
        }

        flushActiveKeys(treeMgmtData, &node);
        flushParent(treeMgmtData, &node);
        
        if ((status = unpinPage(&treeMgmtData->bPool, &pHandle)) != RC_OK) {
            return status;
        }
        
        return status;
        
    }
    
    
    
    return RC_OK;
}

extern RC deleteKey (BTreeHandle *tree, Value *key);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// debug and test functions
extern char *printTree (BTreeHandle *tree);





