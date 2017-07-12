////
////  joinImplementation.c
////  Assign#03
////
////  Created by Shravani Mynampally on 12/10/15.
////  Copyright © 2015 raghuveer vemuri. All rights reserved.
////
//
//#include <stdio.h>
//#include "buffer_mgr.h"
//#include "storage_mgr.h"
//# include "record_mgr.h"
//#include "string.h"
//#include "expr.h"
//#include "test_assign3_1.c"
//
//#define TO_OFFSET(offset,size,src) memcpy(offset,src,size);offset+=size;
//#define FROM_OFFSET(offset,size,dest) memcpy(dest,offset,size);offset+=size;
//
//
//
//Schema* buildSchema(int numAttr, int keySize)
//{
//    Schema* schema = (Schema*) malloc(sizeof(Schema));
//    
//    schema->numAttr = numAttr;
//    schema->attrNames = (char**)malloc(sizeof(char*) * numAttr);
//    schema->dataTypes = (DataType*)malloc(sizeof(DataType)*numAttr);
//    schema->typeLength = (int*) malloc(sizeof(int) * numAttr);
//    schema->keySize = keySize;
//    schema->keyAttrs = (int*) malloc(sizeof(int) * keySize);
//    
//    int i;
//    for (i = 0; i<numAttr; i++) {
//        schema->attrNames[i] = (char*)malloc(MAX_FIELDNAME_LENGTH);
//    }
//    
//    return schema;
//    
//}
//
//
//Schema* createSchemaOther (int numAttr, int numAttr1, char **attrNames, char **attrNames1, DataType *dataTypes, DataType *dataTypes1, int *typeLength, int *typeLength1, int keySize, int keySize1, int *keys, int *keys1)
//{
//    Schema* schema;
//    schema = buildSchema(numAttr+numAttr1, keySize+keySize1); //TODO- remove keysize1
//    
//    int i,j;
//    for (i = 0; i<numAttr+numAttr1; i++) {
//        
//        strncpy(schema->attrNames[i], attrNames[i],MAX_FIELDNAME_LENGTH);
//        schema->dataTypes[i] = dataTypes[i];
//        
//        switch (dataTypes[i]) {
//                
//            case DT_INT:
//                schema->typeLength[i] = sizeof(int);
//                break;
//            case DT_BOOL:
//                schema->typeLength[i] = sizeof(bool);
//                break;
//            case DT_FLOAT:
//                schema->typeLength[i] = sizeof(float);
//                break;
//            case DT_STRING:
//                schema->typeLength[i] = typeLength[i];
//                break;
//            default: printf("Unknown attribute type during schema creation");
//                break;
//                
//        }
//        
//        if (i<keySize) {
//            schema->keyAttrs[i] = keys[i];
//        }
//        
//    }
//    
//    for (; i<numAttr+numAttr1; i++) {
//        
//        j = i - numAttr;
//        strncpy(schema->attrNames[i], attrNames1[j],MAX_FIELDNAME_LENGTH);
//        schema->dataTypes[i] = dataTypes1[j];
//        
//        switch (dataTypes1[j]) {
//                
//            case DT_INT:
//                schema->typeLength[i] = sizeof(int);
//                break;
//            case DT_BOOL:
//                schema->typeLength[i] = sizeof(bool);
//                break;
//            case DT_FLOAT:
//                schema->typeLength[i] = sizeof(float);
//                break;
//            case DT_STRING:
//                schema->typeLength[i] = typeLength1[j];
//                break;
//            default: printf("Unknown attribute type during schema creation");
//                break;
//                
//        }
//        
//    }
//    
//    return schema;
//}
//
//
//bool match(Schema* schema1, Record* rec1, Schema* schema2, Record* rec2, int a, int b){
//    
//    Value* val1;
//    Value* val2;
//    
//    getAttr(rec1, schema1, a, &val1);
//    getAttr(rec2, schema2, b, &val2);
//    
//    if (val1->dt!=val2->dt) {
//        return false;
//    }
//    Value* result = (Value*) malloc(sizeof(Value));
//    valueEquals(val1, val2, result);
//    
//    if (result->v.boolV) {
//        free(result);
//        return true;
//    }
//    else
//    {
//        free(result);
//        return false;
//    }
//    
//}
//
//RC join(char* rel1, char* rel2, int rel1Attr, int rel2Attr, char** joinRel){
//    
//    RC status;
//    
//    char* joinResult = "result";
//    
//    RM_TableData *rel1TableData = (RM_TableData *) malloc(sizeof(RM_TableData));
//    RM_TableData *rel2TableData = (RM_TableData *) malloc(sizeof(RM_TableData));
//    RM_TableData *resultTableData = (RM_TableData *) malloc(sizeof(RM_TableData));
//    
//    RM_TableMgmtData* rel1TblMgmtData = rel1TableData->mgmtData;
//    RM_TableMgmtData* rel2TblMgmtData = rel1TableData->mgmtData;
//    
//    RM_ScanHandle *sc1 = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
//    RM_ScanHandle *sc2 = (RM_ScanHandle *) malloc(sizeof(RM_ScanHandle));
//    
//    
//    if ((status=  openTable(rel1TableData, rel1))) {
//        return RC_OK;
//    }
//    
//    if ((status=  openTable(rel2TableData, rel2))) {
//        return RC_OK;
//    }
//    
//    //int numAttr = (rel1TableData->schema->numAttr + rel2TableData->schema->numAttr);
//    
//    Schema* schema = createSchemaOther(rel1TableData->schema->numAttr, rel2TableData->schema->numAttr, rel1TableData->schema->attrNames,  rel2TableData->schema->attrNames, rel1TableData->schema->dataTypes, rel2TableData->schema->dataTypes, rel1TableData->schema->typeLength, rel2TableData->schema->typeLength, rel2TableData->schema->numAttr, rel2TableData->schema->numAttr, rel1TableData->schema->keyAttrs,   rel2TableData->schema->keyAttrs);
//    
//    if ((status=  createTable (joinResult, schema)) != RC_OK) {
//        return RC_OK;
//    }
//    
//    
//    if ((status=  openTable(resultTableData, joinResult)) != RC_OK) {
//        return RC_OK;
//    }
//
//    if ((status= startScan(rel1TableData, sc1, NULL) ) != RC_OK) {
//        return RC_OK;
//    }
//    
//    if ((status= startScan(rel2TableData, sc2, NULL) ) != RC_OK) {
//        return RC_OK;
//    }
//    
//    int i = 0, j = 0;
//    
//    Record* record1;
//    Record* record2;
//    Record* resultRecord;
//    
//    createRecord(&record1, rel1TableData->schema);
//    createRecord(&record2, rel2TableData->schema);
//    createRecord(&resultRecord, schema);
//    
//    int rec1Size = 0, rec2Size = 0;
//    char* offset;
//    
//    for (i = 0; i<rel1TblMgmtData->recordCount; i++) {
//        
//        next(sc1, record1);
//        
//        for (j = 0; j<rel2TblMgmtData->recordCount; j++) {
//            
//            next(sc2, record2);
//            
//            if (match(rel1TableData->schema, record1, rel2TableData->schema, record2, rel1Attr, rel2Attr)) {
//                
//                rec1Size = getRecordSize((rel1TableData->schema));
//                rec2Size = getRecordSize((rel2TableData->schema));
//                
//                offset = resultRecord->data;
//                TO_OFFSET(offset, rec1Size, record1->data);
//                TO_OFFSET(offset, rec2Size, record2->data);
//                
//                if ((status = insertRecord(resultTableData, resultRecord)) != RC_OK) {
//                    return status;
//                }
//                
//            }
//            
//        }
//        
//    }
//    
//    *joinRel = joinResult;
//    
//    freeRecord(record1);
//    freeRecord(record2);
//    freeRecord(resultRecord);
//    
//    closeTable(rel1TableData);
//    closeTable(rel2TableData);
//    closeTable(resultTableData);
//    
//    free(rel1TableData);
//    free(rel2TableData);
//    free(resultTableData);
//    
//    free(sc1);
//    free(sc2);
//    
//    return RC_OK;
//}
//
//
