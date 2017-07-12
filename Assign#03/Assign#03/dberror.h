#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4

//My Custom error
#define RC_FILE_CREATION_ERROR 5
#define RC_FILE_EMPTY_PAGE_ADDITION_ERROR 6
#define RC_SEEK_ERROR 7
#define RC_FILE_CLOSE_ERROR 8
#define RC_READ_FAILED 9
#define RC_WRITE_NON_EXISTING_PAGE 10
#define RC_BUFFERPOOL_INIT_ERROR 11
#define RC_PAGEFRAME_ERROR 12
#define RC_BUFFER_INIT_FAIL_FILEOPEN 13
#define RC_BUFFER_SHUTDOWN_FAIL_FIXCOUNT 14
#define RC_PAGE_NOTPINNED 15
#define RC_PAGE_NOT_IN_BUFFERPOOL 16;
#define RC_LRU_MAINTENANCE_ERROR 17;
#define RC_BUFFERPOOL_PEAKED 18;
#define RC_FIFO_MAINTENANCE_ERROR 19;
#define RC_SCHEMA_TOO_BIG 20;
#define RC_RECORD_TOO_BIG 21;
#define RC_RID_INVALID 22;
#define RC_NONEXISTENT_RECORD 23;


#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
do {			  \
RC_message=message;	  \
return rc;		  \
} while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
do {									\
int rc_internal = (code);						\
if (rc_internal != RC_OK)						\
{									\
char *message = errorMessage(rc_internal);			\
printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
free(message);							\
exit(1);							\
}									\
} while(0);


#endif
