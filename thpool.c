/* ********************************
 * 
 * Author:  Johan Hanssen Seferidis
 * Date:    12/08/2011
 * Update:  01/11/2011
 * License: LGPL
 * 
 * 
 *//** @file thpool.h *//*
 ********************************/

/* Library providing a threading pool where you can add work. For an example on 
 * usage you refer to the main file found in the same package */

/* 
 * Fast reminders:
 * 
 * tp           = threadpool 
 * thpool       = threadpool
 * thpool_t     = threadpool type
 * tp_p         = threadpool pointer
 * sem          = semaphore
 * xN           = x can be any string. N stands for amount
 * 
 * */

#include "thpool.h"      /* here you can also find the interface to each function */

static int thpool_keepalive=1;

/* Initialise thread pool */
thpool_t* thpool_init(int threadsN)
{
    thpool_t* tp_p;

    if (!threadsN || threadsN<1) threadsN=1;

    /* Make new thread pool */
    tp_p=(thpool_t*)malloc(sizeof(thpool_t));                              /* MALLOC thread pool */
    if (tp_p==NULL){
        fprintf(stderr, "thpool_init(): Could not allocate memory for thread pool\n");
        return NULL;
    }
    tp_p->threads=(pthread_t*)malloc(threadsN*sizeof(pthread_t));          /* MALLOC thread IDs */
    if (tp_p->threads==NULL){
        fprintf(stderr, "thpool_init(): Could not allocate memory for thread IDs\n");
		free(tp_p);
        return NULL;
    }
    tp_p->threadsN=threadsN;

    /* Initialise the job queue */
    if (thpool_jobqueue_init(tp_p)==-1){
        fprintf(stderr, "thpool_init(): Could not allocate memory for job queue\n");
		free(tp_p->threads);
		free(tp_p);
        return NULL;
    }

    /* Initialise semaphore*/
    sem_init(&tp_p->mutex, 0, 1);
    sem_init(&tp_p->nEmpty, 0, threadsN);
    sem_init(&tp_p->nStored, 0, 0);

    /* Make threads in pool */
    int t;
    for (t=0; t<threadsN; t++){
        if (pthread_create(&(tp_p->threads[t]), NULL, (void *)thpool_thread_do, (void *)tp_p) != 0) /* MALLOCS INSIDE PTHREAD HERE */
        {
            fprintf(stderr, "created thread pool failed %s\n", strerror(errno));
        }
    }

    return tp_p;
}


/* What each individual thread is doing 
 * There are two scenarios here. One is everything works as it should and second if
 * the thpool is to be killed. In that manner we try to BYPASS sem_wait and end each thread. 
 * -------Consumer-------  */
void thpool_thread_do(thpool_t* tp_p)
{
    while(thpool_keepalive){
        if (sem_wait(&tp_p->nStored)) {/* WAITING until there is work in the queue */
            fprintf(stderr, "thpool_thread_do(): Waiting for semaphore");
            exit(1);
        }

        if (thpool_keepalive){
            /* Read job from queue and execute it */
            void*(*func_buff)(void* arg);
            void*  arg_buff;
            thpool_job_t* job_p;

            sem_wait(&tp_p->mutex);                  /* LOCK */

            job_p = thpool_jobqueue_peek(tp_p);
            func_buff=job_p->function;
            arg_buff =job_p->arg;
            thpool_jobqueue_removelast(tp_p);

            sem_post(&tp_p->mutex);                /* UNLOCK */
            sem_post(&tp_p->nEmpty);

            func_buff(arg_buff);                     /* run function */
            free(job_p);                          /* DEALLOC job */
        }
    }
    return;
}


/* Add work to the thread pool 
 * block if queue is full 
 * Producer */
int thpool_add_work(thpool_t* tp_p, void *(*function_p)(void*), void* arg_p)
{
    thpool_job_t* newJob;

    newJob=(thpool_job_t*)malloc(sizeof(thpool_job_t));                        /* MALLOC job */
    if (newJob==NULL) {
        fprintf(stderr, "thpool_add_work(): Could not allocate memory for new job\n");
        exit(1);
    }

    /* add function and argument */
    newJob->function=function_p;
    newJob->arg=arg_p;

    sem_wait(&tp_p->nEmpty);
    sem_wait(&tp_p->mutex);
    /* add job to queue */
    thpool_jobqueue_add(tp_p, newJob);

    sem_post(&tp_p->mutex);
    sem_post(&tp_p->nStored);
    return 0;
}

void thpool_destroy(thpool_t* tp_p)
{
    int t;

    /* End each thread's infinite loop */
    thpool_keepalive=0; 

    /* Awake idle threads waiting at semaphore */
    for (t=0; t<(tp_p->threadsN); t++) {
        if (sem_post(&tp_p->nStored)){
            fprintf(stderr, "thpool_destroy(): Could not bypass sem_wait()\n");
        }
    }

    /* Kill semaphore */
    if (sem_destroy(&tp_p->mutex)!=0){
        fprintf(stderr, "thpool_destroy(): Could not destroy semaphore\n");
    }
    if (sem_destroy(&tp_p->nEmpty)!=0){
        fprintf(stderr, "thpool_destroy(): Could not destroy semaphore\n");
    }
    if (sem_destroy(&tp_p->nStored)!=0){
        fprintf(stderr, "thpool_destroy(): Could not destroy semaphore\n");
    }
    /* Wait for threads to finish */
    for (t=0; t<(tp_p->threadsN); t++){
        pthread_join(tp_p->threads[t], NULL);
    }

    thpool_jobqueue_empty(tp_p);

    /* Dealloc */
    free(tp_p->threads);                                                   /* DEALLOC threads             */
    free(tp_p->jobqueue);                                                  /* DEALLOC job queue           */
    free(tp_p);                                                            /* DEALLOC thread pool         */
}

/* =================== JOB QUEUE OPERATIONS ===================== */



/* Initialise queue */
int thpool_jobqueue_init(thpool_t* tp_p)
{
    tp_p->jobqueue=(thpool_jobqueue*)malloc(sizeof(thpool_jobqueue));      /* MALLOC job queue */
    if (tp_p->jobqueue==NULL) return -1;
    tp_p->jobqueue->tail=NULL;
    tp_p->jobqueue->head=NULL;
    tp_p->jobqueue->jobsN=0;
    return 0;
}


/* Add job to queue */
void thpool_jobqueue_add(thpool_t* tp_p, thpool_job_t* newjob_p)
{
/* remember that job prev and next point to NULL */
    newjob_p->next=NULL;
    newjob_p->prev=NULL;

    thpool_job_t *oldFirstJob;
    oldFirstJob = tp_p->jobqueue->head;
    /* fix jobs' pointers */
    switch(tp_p->jobqueue->jobsN){
        case 0:     /* if there are no jobs in queue */
            tp_p->jobqueue->tail=newjob_p;
            tp_p->jobqueue->head=newjob_p;
            break;

        default:     /* if there are already jobs in queue */
            oldFirstJob->prev=newjob_p;
            newjob_p->next=oldFirstJob;
            tp_p->jobqueue->head=newjob_p;
    }

    (tp_p->jobqueue->jobsN)++;     /* increment amount of jobs in queue */
}


/* Remove job from queue */
int thpool_jobqueue_removelast(thpool_t* tp_p)
{
    thpool_job_t *oldLastJob;
    oldLastJob = tp_p->jobqueue->tail;

    /* fix jobs' pointers */
    switch(tp_p->jobqueue->jobsN){

        case 0:     /* if there are no jobs in queue */
            return -1;
            break;

        case 1:     /* if there is only one job in queue */
            tp_p->jobqueue->tail=NULL;
            tp_p->jobqueue->head=NULL;
            break;

        default:     /* if there are more than one jobs in queue */
            oldLastJob->prev->next=NULL;               /* the almost last item */
            tp_p->jobqueue->tail=oldLastJob->prev;

    }

    (tp_p->jobqueue->jobsN)--;

    return 0;
}


/* Get first element from queue */
thpool_job_t* thpool_jobqueue_peek(thpool_t* tp_p)
{
    return tp_p->jobqueue->tail;
}

/* Remove and deallocate all jobs in queue */
void thpool_jobqueue_empty(thpool_t* tp_p)
{
    thpool_job_t* curjob;
    curjob=tp_p->jobqueue->tail;

    while(tp_p->jobqueue->jobsN){
        tp_p->jobqueue->tail=curjob->prev;
        free(curjob);
        curjob=tp_p->jobqueue->tail;
        tp_p->jobqueue->jobsN--;
    }

    /* Fix head and tail */
    tp_p->jobqueue->tail=NULL;
    tp_p->jobqueue->head=NULL;
}
