/*
 * db.c - O banco: consultas, transferências e snapshots.
 *
 * Implemente as funções de sincronização (db_acquire_*, db_release_*,
 * db_snapshot_begin/end). As operações db_get, db_transfer e db_snapshot, no
 * final do arquivo, já estão prontas e NÃO devem ser alteradas.
 *
 * VOCÊ DEVE ALTERAR ESTE ARQUIVO.
 */

#include <stdlib.h>
#include "db.h"


void db_init(db_t *self, wal_t *wal)
{
    /* Cria as contas (NÃO ALTERE!). */
    char key[KEY_LEN];
    self->wal = wal;
    table_init(&self->table, config.accounts);
    for (unsigned int i = 0; i < config.accounts; i++) {
        account_key(i, key);
        table_insert(&self->table, key, INITIAL_BALANCE);
    }

    /* TODO: Adicionar código aqui se necessário! */
    self->mutex_leitores = malloc(config.partitions * sizeof(pthread_mutex_t *));
    self->mutex_escrita = malloc(config.partitions * sizeof(pthread_mutex_t *));
    self->catraca = malloc(config.partitions * sizeof(pthread_mutex_t *));
    self->readers = malloc(config.partitions * sizeof(int *));
    for(int i = 0; i < config.partitions; i++){
        self->mutex_leitores[i] = malloc(sizeof(pthread_mutex_t));
        self->mutex_escrita[i] = malloc(sizeof(pthread_mutex_t));
        self->catraca[i] = malloc(sizeof(pthread_mutex_t));
        self->readers[i] = malloc(sizeof(int));
        *self->readers[i] = 0;
        pthread_mutex_init(self->mutex_leitores[i], NULL);
        pthread_mutex_init(self->mutex_escrita[i], NULL);
        pthread_mutex_init(self->catraca[i], NULL);
    }
    pthread_mutex_init(&self->snapshot_pendente, NULL);

    plog("[db] Initialized\n");
}

void db_finalize(db_t *self)
{
    /* TODO: Adicionar código aqui se necessário! */
    for(int i = 0; i < config.partitions; i++){
        pthread_mutex_destroy(self->mutex_leitores[i]);
        pthread_mutex_destroy(self->mutex_escrita[i]);
        pthread_mutex_destroy(self->catraca[i]);
        free(self->mutex_leitores[i]);
        free(self->mutex_escrita[i]);
        free(self->catraca[i]);
        free(self->readers[i]);
    }
    pthread_mutex_destroy(&self->snapshot_pendente);
    free(self->mutex_escrita);
    free(self->mutex_leitores);
    free(self->catraca);
    free(self->readers);

    table_finalize(&self->table);
    plog("[db] Finalized\n");
}

void db_acquire_read(db_t *self, const char *key)
{
    unsigned int particao = db_partition_of(self, key);
    pthread_mutex_lock(self->catraca[particao]);
    pthread_mutex_unlock(self->catraca[particao]);

    pthread_mutex_lock(self->mutex_leitores[particao]);
    (*self->readers[particao])++;
    if((*self->readers[particao]) == 1){
        pthread_mutex_lock(self->mutex_escrita[particao]);
    }
    pthread_mutex_unlock(self->mutex_leitores[particao]);
}

void db_release_read(db_t *self, const char *key)
{
    unsigned int particao = db_partition_of(self, key);
    pthread_mutex_lock(self->mutex_leitores[particao]);
    (*self->readers[particao])--;
    if((*self->readers[particao]) == 0){
        pthread_mutex_unlock(self->mutex_escrita[particao]);
    }
    pthread_mutex_unlock(self->mutex_leitores[particao]);

}

void db_acquire_pair(db_t *self, const char *from, const char *to)
{
    unsigned int particao_from = db_partition_of(self, from);
    unsigned int particao_to = db_partition_of(self, to);
    
    pthread_mutex_lock(&self->snapshot_pendente);
    pthread_mutex_unlock(&self->snapshot_pendente);

    if(particao_from < particao_to){
        pthread_mutex_lock(self->catraca[particao_from]);
        pthread_mutex_lock(self->mutex_escrita[particao_from]);
        pthread_mutex_unlock(self->catraca[particao_from]);

        pthread_mutex_lock(self->catraca[particao_to]);
        pthread_mutex_lock(self->mutex_escrita[particao_to]);
        pthread_mutex_unlock(self->catraca[particao_to]);
    }else if(particao_from > particao_to){
        pthread_mutex_lock(self->catraca[particao_to]);
        pthread_mutex_lock(self->mutex_escrita[particao_to]);
        pthread_mutex_unlock(self->catraca[particao_to]);
        
        pthread_mutex_lock(self->catraca[particao_from]);
        pthread_mutex_lock(self->mutex_escrita[particao_from]);
        pthread_mutex_unlock(self->catraca[particao_from]);
    }else{
        pthread_mutex_lock(self->catraca[particao_from]);
        pthread_mutex_lock(self->mutex_escrita[particao_from]);
        pthread_mutex_unlock(self->catraca[particao_from]);
    }
    
}

void db_release_pair(db_t *self, const char *from, const char *to)
{
    unsigned int particao_from = db_partition_of(self, from);
    unsigned int particao_to = db_partition_of(self, to);
     if(particao_from < particao_to){
        pthread_mutex_unlock(self->mutex_escrita[particao_from]); 
        pthread_mutex_unlock(self->mutex_escrita[particao_to]);          
    }else if(particao_from > particao_to){
        pthread_mutex_unlock(self->mutex_escrita[particao_to]);       
        pthread_mutex_unlock(self->mutex_escrita[particao_from]); 
    }else{
        pthread_mutex_unlock(self->mutex_escrita[particao_to]);
    }

}

void db_snapshot_begin(db_t *self)
{
    pthread_mutex_lock(&self->snapshot_pendente);

    for(int i = 0; i < config.partitions; i++){
        pthread_mutex_lock(self->mutex_leitores[i]);
        (*self->readers[i])++;
        if(*self->readers[i] == 1){
            pthread_mutex_lock(self->mutex_escrita[i]);
        }
        pthread_mutex_unlock(self->mutex_leitores[i]);

        
    }
}

void db_snapshot_end(db_t *self)
{
     for(int i = 0; i < config.partitions; i++){
        pthread_mutex_lock(self->mutex_leitores[i]);
        (*self->readers[i])--;
        if(*self->readers[i] == 0){
            pthread_mutex_unlock(self->mutex_escrita[i]);
        }
        pthread_mutex_unlock(self->mutex_leitores[i]);


    }

     pthread_mutex_unlock(&self->snapshot_pendente);
}

/* --------------------------------------------------------- */
/* ATENÇÃO: Não será necessário modificar as funções abaixo! */
/* --------------------------------------------------------- */

unsigned int db_partition_of(db_t *self, const char *key)
{
    return table_hash(key) % config.partitions;
}

int db_get(db_t *self, const char *key)
{
    db_acquire_read(self, key);
    int balance = table_get(&self->table, key);
    db_release_read(self, key);
    return balance;
}

long db_transfer(db_t *self, const char *from, const char *to, int amount)
{
    long lsn = -1;

    /* Trava as duas contas e, se houver saldo, transfere e registra no log. */
    db_acquire_pair(self, from, to);
    if (table_get(&self->table, from) >= amount) {
        table_add(&self->table, from, -amount);
        table_add(&self->table, to, amount);
        lsn = wal_append(self->wal, from, to, amount);
    }
    db_release_pair(self, from, to);

    /* Só confirma ao cliente depois que o registro estiver em disco. */
    if (lsn >= 0)
        wal_wait_durable(self->wal, lsn);

    return lsn;
}

double db_snapshot(db_t *self, int *balances)
{
    double start = now_ms();
    db_snapshot_begin(self);
    double waited = now_ms() - start;

    for (unsigned int i = 0; i < table_size(&self->table); i++)
        balances[i] = table_value_at(&self->table, i);

    db_snapshot_end(self);
    return waited;
}
