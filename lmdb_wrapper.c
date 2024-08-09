#include <stdio.h>                                                              
#include <stdlib.h>                                                             
#include <string.h>                                                             
#include <time.h>                                                               
#include "lmdb.h"                                                               
                                                                                
typedef struct evdb_s {                                                      
    MDB_env         *env;                                                       
    MDB_dbi         dbi;                                                        
    MDB_txn         *txn;                                                       
    MDB_stat        mst;                                                        
    char            envname[1024];                                              
    char            dbname[1024];                                               
    uint32_t        mapsz;                                                      
                                                                                
} evdb_t;

int ev_db_init(evdb_t *db, char *envname, char *dbname, uint32_t mapsz)   
{                                                                               
    int     rc;                                                                 
                                                                                
    if (db == NULL) {                                                           
        return -1;                                                              
    }                                                                           
    if ((rc = mdb_env_create(&(db->env))) != MDB_SUCCESS) {                     
        printf("Error creation of mdb env : %s\n", mdb_strerror(rc));           
        return -1;                                                              
    }                                                                           
    if ((rc = mdb_env_set_maxreaders(db->env, 1)) != MDB_SUCCESS) {             
        printf("Error in set of mdb env : %s\n", mdb_strerror(rc));             
        return -1;                                                              
    }                                                                           
    if ((rc = mdb_env_set_mapsize(db->env, mapsz)) != MDB_SUCCESS) {            
        printf("Error in set mapzise of mdb env : %s\n", mdb_strerror(rc));        
        return -1;                                                              
    }                                                                           
    if ((rc = mdb_env_set_maxdbs(db->env, 1)) != MDB_SUCCESS) {                 
        printf("Error in env set maxdb : %s\n", mdb_strerror(rc));              
        return -1;                                                              
    }                                                                           
    if ((rc = mdb_env_open(db->env, envname, MDB_FIXEDMAP|MDB_NOSYNC,           
                    0664)) != MDB_SUCCESS) {                                    
        printf("Error in open db : %s\n", mdb_strerror(rc));                    
        return -1;                                                              
    }                                                                           
                                                                                
    if ((rc = mdb_txn_begin(db->env, NULL, 0, &db->txn)) != MDB_SUCCESS) {      
        printf("Error in txn begin : %s\n", mdb_strerror(rc));                  
        return -1;                                                              
    }                                                                           
                                                                                
    if ((rc = mdb_dbi_open(db->txn, dbname, MDB_CREATE, &db->dbi)) != MDB_SUCCESS) {
        printf("ErrorInit in dbi open : %s\n", mdb_strerror(rc));               
        return -1;                                                              
    }                                                                           
    if ((rc = (mdb_txn_commit(db->txn))) != MDB_SUCCESS) {                      
        printf("Error in txn commit : %s\n", mdb_strerror(rc));                 
        return -1;                                                              
    }                                                                           
                                                                                
    return 0;
}

void ev_insert_db(evdb_t *db, char *rkey, char *rdata)                    
{                                                                               
    char        kval[32] = "";                                                  
    char        dval[32] = "";                                                  
    MDB_val     key, data;                                                      
    int         rc;                                                             
    MDB_txn     *txn;                                                           
    MDB_dbi     dbi;                                                            
    MDB_envinfo stat;                                                           
    MDB_stat    mst;                                                            
                                                                                
    if (NULL == db) {                                                           
        printf("db is NULL");                                                   
        return;                                                                 
    }                                                                           
    sprintf(kval, "%s", rkey);                                                  
    key.mv_size = sizeof(kval);                                                 
    key.mv_data = kval;                                                         
                                                                                
    sprintf(dval, "%s", rdata);                                                 
    data.mv_size = sizeof(dval);                                                
    data.mv_data = dval;       
    if (mdb_env_info(db->env, &stat) == 0) {                                    
        printf("last txnid=%lu\n", stat.me_last_txnid);                         
    }                                                                           
                                                                                
    if ((rc = mdb_txn_begin(db->env, NULL, 0, &txn)) != MDB_SUCCESS) {          
        printf("Error in txn begin : %s\n", mdb_strerror(rc));                  
        return;                                                                 
    }                                                                           
    if ((rc = mdb_dbi_open(txn, db->dbname, 0, &dbi)) != MDB_SUCCESS) {         
        printf("Error in dbi open failed ..: %s\n", mdb_strerror(rc));          
        return;                                                                 
    }                                                                           
                                                                                
    rc = mdb_put(db->txn, db->dbi, &key, &data, MDB_NOOVERWRITE); //MDB_CURRENT 
    if (rc == 0) {                                                              
        printf("key : [%s] inserted\n", rkey);                                  
        mdb_txn_commit(txn);                                                    
    } else {                                                                    
        mdb_txn_abort(txn);                                                     
    }                                                                           
                                                                                
    if ((rc = mdb_env_stat(db->env, &mst)) != MDB_SUCCESS) {                    
        printf("Error in env stat : %s\n", mdb_strerror(rc));                   
        return;                                                                 
    }                                                                           
}       

void ev_del_key_db(evdb_t *db, char *rkey)                                
{                                                                               
    char        kval[32] = "";                                                  
    MDB_val     key;                                                            
    int         rc;                                                             
    MDB_txn     *txn;                                                           
    MDB_dbi     dbi;                                                            
    MDB_envinfo stat;                                                           
    MDB_stat    mst;                                                            
                                                                                
    if (NULL == db) {                                                           
        printf("db is NULL");                                                   
        return;                                                                 
    }                                                                           
    sprintf(kval, "%s", rkey);                                                  
    key.mv_size = sizeof(kval);                                                 
    key.mv_data = kval;                                                         
                                                                                
    if (mdb_env_info(db->env, &stat) == 0) {                                    
        printf("last txnid=%lu\n", stat.me_last_txnid);                         
    }                                                                           
                                                                                
    if ((rc = mdb_txn_begin(db->env, NULL, 0, &txn)) != MDB_SUCCESS) {          
        printf("Error in txn begin : %s\n", mdb_strerror(rc));                  
        return;                                                                 
    }                                                                           
    if ((rc = mdb_dbi_open(txn, db->dbname, 0, &dbi)) != MDB_SUCCESS) {         
        printf("Error-del in dbi open failed : %s\n", mdb_strerror(rc));        
        return;                                                                 
    }                                                                           
                                                                                
    rc = mdb_del(db->txn, db->dbi, &key, NULL);                                 
    if (rc == 0) {                                                              
        printf("key : [%s] deleted\n", rkey);                                   
        mdb_txn_commit(txn);                                                    
    } else {                                                                    
        mdb_txn_abort(txn);                                                     
    }                                                        
    if ((rc = mdb_env_stat(db->env, &mst)) != MDB_SUCCESS) {                    
        printf("Error in env stat : %s\n", mdb_strerror(rc));                   
        return;                                                                 
    }                                                                           
                                                                                
}                                                                               
                                                                                
void ev_update_key_db(evdb_t *db, char *key, char *data)                  
{                                                                               
    if (NULL == db) {                                                           
        printf("db is NULL");                                                   
        return;                                                                 
    }                                                                           
                                                                                
    ev_del_key_db(db, key);                                                  
    ev_insert_db(db, key, data);                                             
}                

void ev_get_db(evdb_t *db, char *rkey, char *rdata)                       
{                                                                               
    int             rc;                                                         
    char            kval[32] = "";                                              
    MDB_val         key, data;                                                  
    MDB_txn     *txn;                                                           
    MDB_dbi     dbi;                                                            
    MDB_envinfo stat;                                                           
    MDB_stat    mst;                                                            
                                                                                
    if (NULL == db) {                                                           
        printf("db is NULL");                                                   
        return;                                                                 
    }                                                                           
    sprintf(kval, "%s", rkey);                                                  
    key.mv_size = sizeof(kval);                                                 
    key.mv_data = kval;                                                         
                                                                                
    if (mdb_env_info(db->env, &stat) == 0) {                                    
        printf("last txnid=%lu\n", stat.me_last_txnid);                         
    }                                                                           
                                                                                
    if ((rc = mdb_txn_begin(db->env, NULL, 0, &txn)) != MDB_SUCCESS) {          
        printf("Error in txn begin : %s\n", mdb_strerror(rc));                  
        return;                                                                 
    }                                                                           
    if ((rc = mdb_dbi_open(txn, db->dbname, 0, &dbi)) != MDB_SUCCESS) {         
        printf("Error-del in dbi open failed : %s\n", mdb_strerror(rc));        
        return;                                                                 
    }                                                                           
                                                                                
    rc = mdb_get(txn, db->dbi, &key, &data);                                    
    if (rc == 0) {                                                              
        char *valuen = (char *) malloc(data.mv_size + 1);                       
        memcpy(valuen, data.mv_data, data.mv_size);                             
        valuen[data.mv_size] = 0;                                               
                                                                                
        printf("key : [%s]  data : [%s]\n", (char *) key.mv_data, valuen);      
                                                                                
        free(valuen);                                                           
        mdb_txn_commit(txn);               
    } else {                                                                    
        printf("No such key : %d\n", rc);                                       
        mdb_txn_abort(txn);                                                     
    }                                                                           
                                                                                
    if ((rc = mdb_env_stat(db->env, &mst)) != MDB_SUCCESS) {                    
        printf("Error in env stat : %s\n", mdb_strerror(rc));                   
        return;                                                                 
    }                                                                                                                           
}                                                                               
                                                                                
void ev_close_mdb(evdb_t *db)                                             
{                                                                               
    if (NULL == db) {                                                           
        return;                                                                 
    }                                                                           
    //if (db->txn) mdb_txn_abort(db->txn);                                      
    if (db->env && db->dbi) mdb_dbi_close(db->env, db->dbi);                    
    if (db->env) mdb_env_close(db->env);                                        
}                                     

int main(int argc,char * argv[])                                                
{                                                                               
    evdb_t         *db = NULL;                                                 
    int             rc;                                                         
    db = (evdb_t *) calloc(1, sizeof(evdb_t));                            
    if (NULL == db) {                                                           
        return -1;                                                              
    }                                                                           
    sprintf(db->envname, "%s", "./testdb");                                     
    sprintf(db->dbname, "%s", "event");                                           
    db->mapsz = 10485760;                                                       
                                                                                
    if ((rc = ev_db_init(db, db->envname, db->dbname, db->mapsz)) < 0) {     
        printf("Error in ev db init\n");                                     
        free(db);                                                               
        return -1;                                                              
    }                                                                           
                                                                                                                            
    ev_insert_db(db, "hello", "world");                                      
    ev_get_db(db, "hello", "");

    ev_insert_db(db, "hello1", "world");                                     
    ev_get_db(db, "hello1", "");                                             

    ev_insert_db(db, "swadhin", "cooldude");                                 
    ev_get_db(db, "swadhin", "");                                            
    ev_del_key_db(db, "swadhin");                                            

    ev_close_mdb(db);                                                        
    if (db) free(db);                                                           
    return 0;                                                                   
}             