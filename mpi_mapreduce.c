#include <stdio.h>
#include <mpi.h>
#include <omp.h>
#include <string.h>
#include <unistd.h>


//Run times:
//Did following tests:
//with 16 nodes, times are:
//Read 8, Write 8, Map 2, Reduce 10 - 0.7s - Fastest
//Read 6, Write 6, Map 2, Reduce 8 - 1.4s
//Read 4, Write 4, Map 4, Reduce 8 - 2s
//Read 9, Write 9, Map 1, Reduce 10 - 2+ s 
///User config
#define NUM_PROCESS 16
#define NUM_FILE_CHUNKS 130
#define KEYS_PER_REDUCER 5000
#define READ_THREADS 8
#define WRITE_THREADS 8
#define MAP_THREADS 2
#define REDUCE_THREADS 10
#define READER_Q_SIZE 200000


///Do not change
#define LINE_LENGTH 256
#define WORD_LENGTH 70 
#define OUTPUT_WRITE 10001

typedef struct Q {
    int size;
    int pos;
    char** QHead;
} Q;


typedef struct scratchlist {
    int file;
    int map;
    int thread;
    struct scratchlist* nextptr;
} scratchlist;

typedef struct LLitem {
    int cnt;
    char* word;
    struct LLitem* nextptr;
} LLitem;


struct Q* InitQ (int n) {
    struct Q* newQ = (struct Q*) malloc (sizeof(Q));
    newQ->size=n;
    newQ->pos=-1;
    newQ->QHead = (char**) malloc(sizeof(char*)*n);
    int i;
    for (i=0;i<newQ->size;i++) {
        newQ->QHead[i]=(char*) malloc(LINE_LENGTH*sizeof(char));
    }
    return newQ;
} 

int getWorkWQ (struct Q* W, char* buf) {
    if (W->pos>-1) {
        memcpy(buf,W->QHead[W->pos],(strlen(W->QHead[W->pos]))*sizeof(char));
        buf[strlen(W->QHead[W->pos])]=0;
        W->pos--;
        return 1;
    } else {
        return 0;
    }
}

int putWorkWQ (struct Q* W, char* buf) {
    if (W->pos < W->size-1) {
        W->pos++;
        memcpy(W->QHead[W->pos],buf,(strlen(buf)+1)*sizeof(char));
        return 1;
    } else {
        return 0;
    }
}

void reader(struct Q* W, char* fileName, int pid, omp_lock_t* lck) {
    int lines = 0;
    int returnVal=1;
    FILE* file = fopen(fileName,"r");
    char* buf = (char*) malloc(LINE_LENGTH*sizeof(char));
    while (fgets(buf,LINE_LENGTH,file) != NULL) {
        if (buf[0]!='\n') {
            lines++;
            do {
            omp_set_lock(lck);
            returnVal=putWorkWQ(W,buf);
            omp_unset_lock(lck);
            if (returnVal==0) {
                usleep(5);
                //printf("PID %d Q currently full, reader waiting\n",pid);
            }
        } while(returnVal==0);
        }
    }
    fclose(file);
    return;
}

int hashFunc(char* a) {
    int i,hash;
    hash=0;
    int tmp=0;
    int n = strlen(a);
    for (i=0;i<n;i++) {
        hash=(hash<<4 ^ hash) ^ a[i];
    }
    return hash&255;
}

int getWord(char** l, char* buf) {
    int n = strlen(*l);
    int i;
    for(i=0;i<n;i++) {
        if ((*(*l+i)==' ') || (*(*l+i)==0) || (*(*l+i)=='\n')) {break;}
    }
    memcpy(buf,*l,i*sizeof(char));
    buf[i]=(int) 0;
    int flag=(int) *(*l+i);
    if (*(*l+i)=='\n') {flag=0;}
    *l+=i+1;
    return flag;
}

void normalizeWord(char* w) {
    int n = strlen(w);
    int flag;
    int i,j=0;
    for (i=0;i<n;i++) {
        if (w[i]<=90 && w[i]>=65) {w[i]+=32;}
        if (!((w[i]>=65 && w[i]<=90) || (w[i]>=97 && w[i]<=122))) {
            j++;
        }
    }
    if (j!=n) {
        while(1) {
            flag=0;
            if (!((w[0]>=65 && w[0]<=90) || (w[0]>=97 && w[0]<=122))) {
                flag=1;
                for (i=0;i<n;i++) {
                    w[i] = w[i+1];
                }
                n=n-1;
            }
            if (!((w[n-1]>=65 && w[n-1]<=90) || (w[n-1]>=97 && w[n-1]<=122))) {
                flag=1;
                w[n-1]=0;
                n=n-1;
            }
            if (!flag) {break;}
        }
    }
}

void printTable(struct LLitem** h, int n) {
    int i;
    struct LLitem *p;
    for (i=0;i<n;i++) {
        printf("H[%d]",i);
        if ( *(h+i) != NULL) {
            p = *(h+i);
            do {
                printf(" {%s,%d} ",p->word,p->cnt);
                p = p->nextptr;
            } while(p!=NULL);
        }
        else {printf(" NULL ");}
        printf("\n");
    }
} 

void printScratchTable(struct scratchlist** h, int n) {
    int i;
    struct scratchlist *p;
    for (i=0;i<n;i++) {
        printf("R[%d]",i);
        if ( *(h+i) != NULL) {
            p = *(h+i);
            do {
                printf(" -> {%d,%d,%d} ",p->map,p->file,p->thread);
                p = p->nextptr;
            } while(p!=NULL);
        }
        else {printf(" NULL ");}
        printf("\n");
    }
} 

void insertScratchFile (struct scratchlist** h, int file, int map, int tid) {
    struct scratchlist* p = *h;
    struct scratchlist* elem = (struct scratchlist*) malloc(sizeof(struct scratchlist));
    *h = elem;
    elem->file=file;
    elem->map=map;
    elem->thread=tid;
    elem->nextptr=p;
    return;
}

void insert (struct LLitem** h, char* w, int c) {
    if (strlen(w)>WORD_LENGTH) {
        return;
    }
    if (*h==NULL) {
        struct LLitem* elem = (struct LLitem*) malloc(sizeof(struct LLitem));
        *h = elem;
        elem->cnt=c;
        elem->word=(char*) malloc(WORD_LENGTH*sizeof(char));
        memcpy(elem->word,w,sizeof(char)*(strlen(w)+1));
        elem->nextptr=NULL;
        return;
    }
    struct LLitem* p;
    p = *h;
    int flag=0;
    do {
        if(strcmp(p->word,w)==0) {
            p->cnt+=c;
            flag=1;
            break;
        }
        p=p->nextptr;
    }while (p!=NULL);
    if (!flag) {
        struct LLitem* elem = (struct LLitem*) malloc(sizeof(struct LLitem));
        p=*h;
        *h = elem;
        elem->cnt=c;
        elem->word=(char*) malloc(WORD_LENGTH*sizeof(char));
        memcpy(elem->word,w,sizeof(char)*(strlen(w)+1));
        elem->nextptr=p;
        return;
    }
    return;
}

int readingDone(int* flag, int num_readers, int num_read_threads) {
    int i=0;
    int f=1;
    while(i<num_readers) {
        if ((*(flag+i))!=num_read_threads) {
            f=0;
            break;
        }
        i++;
    }
    return f;
}

int writer(struct LLitem** h, int index, int pid, int tid) {
    struct LLitem* p;
    int file_count = 0;
    char filename[30];
    if (index != OUTPUT_WRITE) {
        while (*h!=NULL) {
            sprintf(filename,"%d_%d_reducerFile_%d_%d",pid,tid,index,file_count);
            FILE* f = fopen(filename,"w");
            int count = 0;
            while ((*h!=NULL) && (count<KEYS_PER_REDUCER)) {
                p = *h;
                fprintf(f,"%s %d\n",p->word,p->cnt);
                *h = p->nextptr;
                count++;
            }
            fclose(f);
            file_count++;
        }
        return file_count;
    } else {
        sprintf(filename,"output_%d_%d",pid,tid);
        FILE* f = fopen(filename,"w");
        while (*h!=NULL) {
            p = *h;
            fprintf(f,"%s %d\n",p->word,p->cnt);
            *h = p->nextptr;
        }
        fclose(f);
        return 1;
    }
}

void printFlag(int* flag, int n, char* name) {
    int i =0;
    printf("%s: ",name);
    for (i=0;i<n;i++) {
        printf("%d ",*(flag+i));
    }
    printf("\n");
}

void mapper(struct Q* W, int* done, int num_read_threads, int num_write_threads, int pid, int* num_scratch_files, omp_lock_t* lck, int tid) {
    struct LLitem* hTable[NUM_PROCESS];
    int i;
    for (i=0;i<NUM_PROCESS;i++) {
        hTable[i]=NULL;
    }
    int hIndex;
    int returnVal;
    int workReturnVal=0;
    int lines=0;
    char* buf = (char*) malloc(LINE_LENGTH*sizeof(char));
    char* startptr = buf;
    char* word = (char*) malloc(WORD_LENGTH*sizeof(char));
    while(1) {
        omp_set_lock(lck);
        buf = startptr;
        workReturnVal=getWorkWQ(W, buf);
        omp_unset_lock(lck);
        if (workReturnVal) {
            lines++;
            do {
                returnVal=getWord(&buf,word);
                normalizeWord(word);
                if (!strlen(word)) {continue;}
                hIndex=hashFunc(word)%NUM_PROCESS;
                insert(&hTable[hIndex],word,1);
            } while(returnVal);
        } else {
            if (*done==num_read_threads) {
                break;
            }
            else {
                usleep(50);
                //printf("PID %d Empty Q but reading not done\n",pid);
                continue;
            }
        }
    } 
    #pragma omp parallel for num_threads(num_write_threads)
        for (i=0;i<NUM_PROCESS;i++) {
            num_scratch_files[i]=writer(&hTable[i],i,pid,tid); // Need to parallelize this
        }
    return;
}

void printScratchInfo (int n, int* p) {
    int i,j;
    printf("   ");
    for (i=0;i<n;i++) {
        printf("M%d ",i);
    }
    printf("\n");
    for (i=0;i<NUM_PROCESS;i++) {
        printf("R%d ",i);
        for (j=0;j<n;j++) {
            printf(" %d ",*p);
            p = p+1;
        }
        printf("\n");
    }
}

void get_scratch_file(scratchlist** h, int* retVal) {
    *retVal=999;
    *(retVal+1)=999;
    *(retVal+2)=999;
    while(1) {
        if (*h == NULL) {break;}
        if ((*h)->file>0) {
            (*h)->file--;
            *retVal = (*h)->map;
            *(retVal+1) = (*h)->file;
            *(retVal+2) = (*h)->thread;
            break;
        } else {
            *h = (*h)->nextptr;
        }
    }
    return;
} 

char* getReaderFileName(int n) {
    char* buf = (char*) malloc(10);
    sprintf(buf,"%d",n);
    strcat(buf,".txt");
    return buf;
}

int min(int a, int b) {
    return a ? a<b : b;
}


void send_to_reducer(struct LLitem** h, int index, struct LLitem** reducerQ_head) {
    printf("thread %d inside send to reducer\n",omp_get_thread_num());
    struct LLitem* p;
    int file_count = 0;
    while (*h!=NULL) {
        struct LLitem* reducerQ_item = (struct LLitem*) malloc(sizeof(LLitem));
        p = *h;
        reducerQ_item->cnt = p->cnt;
        reducerQ_item->word = p->word;
        reducerQ_item->nextptr = *(reducerQ_head+index);
        *(reducerQ_head+index) = reducerQ_item;
        *h = p->nextptr;
    }
    return;
}

void openmp_mapper(struct Q* W, int* done, int num_read_threads, struct LLitem** reducerQ, omp_lock_t* lck, omp_lock_t* reducer_q_lck) {
    struct LLitem* hTable[REDUCE_THREADS];
    int i;
    for (i=0;i<REDUCE_THREADS;i++) {
        hTable[i]=NULL;
    }
    int hIndex;
    int returnVal;
    int workReturnVal=0;
    int lines=0;
    char* buf = (char*) malloc(LINE_LENGTH*sizeof(char));
    char* startptr = buf;
    char* word = (char*) malloc(WORD_LENGTH*sizeof(char));
    while(1) {
        buf = startptr;
        omp_set_lock(lck);
        workReturnVal=getWorkWQ(W, buf);
        omp_unset_lock(lck);
        if (workReturnVal) {
            lines++;
            do {
                returnVal=getWord(&buf,word);
                normalizeWord(word);
                if (!strlen(word)) {continue;}
                hIndex=hashFunc(word)%REDUCE_THREADS;
                insert(&hTable[hIndex],word,1);
            } while(returnVal);
        } else {
            if (*done==num_read_threads) {
                break;
            }
            else {
                usleep(1500);
                continue;
            }
        }
    } 
    //printf("thread %d Mapping Done, sending files to reducer\n",omp_get_thread_num());
    #pragma omp parallel for num_threads(WRITE_THREADS)
    for (i=0;i<REDUCE_THREADS;i++) {
        omp_set_lock(&reducer_q_lck[i]);
        send_to_reducer(&hTable[i],i,reducerQ); // Need to parallelize this
        omp_unset_lock(&reducer_q_lck[i]);
    }
    //printf("thread %d Done sending files to reducer\n",omp_get_thread_num());
    return;
}

int openmp_writer(struct LLitem** h, int pid) {
    struct LLitem* p;
    int file_count = 0;
    char filename[30];
    sprintf(filename,"output_%d",pid);
    FILE* f = fopen(filename,"w");
    while (*h!=NULL) {
        p = *h;
        fprintf(f,"%s %d\n",p->word,p->cnt);
        *h = p->nextptr;
    }
    fclose(f);
    return 1;
}


int reducer(int rid, struct LLitem** rQ, omp_lock_t* lck, int* map_done) {
    printf("thread %d inside reducer %d\n",omp_get_thread_num(),rid);
    struct LLitem* internal_reducerQ = (struct LLitem*) malloc(sizeof(LLitem));
    struct LLitem* p;
    internal_reducerQ=NULL;
    while (1) {
        omp_set_lock(lck);
        p = *rQ;
        if ((*rQ)!=NULL) {(*rQ) = (*rQ)->nextptr;}
        omp_unset_lock(lck);
        if ((p==NULL) && (*map_done!=MAP_THREADS)) {
            usleep(1000);
            continue;
        } else if ((p==NULL) && (*map_done==MAP_THREADS)) {
            break;
        } else {
            //printf("thread %d Putting %s,%d into the internal q\n",omp_get_thread_num(),p->word,p->cnt);
            insert(&internal_reducerQ,p->word,p->cnt);
        }
    }
    printf("thread %d done reducing\n",omp_get_thread_num());
    openmp_writer(&internal_reducerQ,rid);
}


int main (int argc, char *argv[]) {
    if (NUM_PROCESS>1) {
        const int read_done = 1234;
        const int reducer_done = 9999;
        const int reducer_q_empty = 999;
        int num_read_threads;
        int num_write_threads;
        int pid;
        int map_done = 0;
        int numP,provided;
        MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&provided);
        if (provided != MPI_THREAD_MULTIPLE)
        {
            printf("Sorry, this MPI implementation does not support multiple threads\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        MPI_Comm_size(MPI_COMM_WORLD,&numP);
        MPI_Comm_rank(MPI_COMM_WORLD,&pid);
        if (numP != NUM_PROCESS) {
            printf("NUM_PROCESS is not equal to number of processes. Please correctly define it in the header\n");
            MPI_Abort(MPI_COMM_WORLD,1);
        }
        num_read_threads = READ_THREADS;
        num_write_threads = WRITE_THREADS;
        int num_read_threads_master = omp_get_max_threads()-4;
        int num_write_threads_master = omp_get_max_threads()-4;
        if ((num_read_threads<1) || (num_write_threads<1)) {
            printf("Insufficient threads per task\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        if (NUM_FILE_CHUNKS<READ_THREADS*NUM_PROCESS) {
            printf("Uneven work distribution - reduce NUM_PROCESS or increase file chunks\n");
            MPI_Abort(MPI_COMM_WORLD,1);
        }
        MPI_Barrier(MPI_COMM_WORLD);
        double elapsedTime = -MPI_Wtime();
        int i,j;
        if (!pid) {
            struct scratchlist* scratch_table[NUM_PROCESS];
            omp_lock_t scratch_locks[NUM_PROCESS];
            for (i=0;i<NUM_PROCESS;i++) {
                scratch_table[i] = NULL;
                omp_init_lock(&scratch_locks[i]);
            }
            int reader_file_ptr = 0;
            int reader_msg = 0;
            int reducer_recv_msg = 0;
            int reducer_send_msg[3] = {0,0,0};
            int reader_pid;
            MPI_Request reader_req_for_work;
            MPI_Status reader_req_status;
            int flag[NUM_PROCESS]; //For keeping track of what mappers are done
            int got_scratch_info[NUM_PROCESS]; //For keeping track of what mappers sent scratch info
            int scratch_buf[NUM_PROCESS+1]; //For collecting inbound scratchfile data
            int reduce_finish_ptr[NUM_PROCESS]; //For keeping track of what reducers are done- 
            char reducerFileName[25];
            FILE *reducerFile;
            int done_read;
            struct Q* reader_Q = InitQ(READER_Q_SIZE);
            MPI_Request scratch_info;
            
            omp_lock_t lck, done_lck; //For mapper and readers to synchronize work q
            omp_init_lock(&lck);
            omp_init_lock(&done_lck);
            int done=0; 
            for (i=0;i<NUM_PROCESS;i++) {
                flag[i] = 0;
            }
            flag[0] = num_read_threads-num_read_threads_master;
            for (i=0;i<NUM_PROCESS;i++) {
                got_scratch_info[i] = 0;
            }
            for (i=0;i<NUM_PROCESS;i++) {
                reduce_finish_ptr[i]=0;
            }
            MPI_Request scratch_msg_done, reduce_msg_done, node_init_done;
            //Assuming numP is < num file chunks
            for (i=1;i<numP;i++) {
                for (j=0;j<num_read_threads;j++) {
                    MPI_Isend(&reader_file_ptr,1,MPI_INT,i,0,MPI_COMM_WORLD,&node_init_done);
                    printf("PID %d sending file %d.txt to destination %d\n",pid,reader_file_ptr,i);
                    MPI_Wait(&node_init_done,MPI_STATUS_IGNORE);
                    reader_file_ptr++;
                }
            }
            #pragma omp parallel
            {
                #pragma omp single
                {
                    #pragma omp task shared(read_done,flag,reader_file_ptr)
                    {
                        while (reader_file_ptr<NUM_FILE_CHUNKS) {
                            MPI_Irecv(&reader_msg,1,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&reader_req_for_work);
                            MPI_Wait(&reader_req_for_work,&reader_req_status);
                                MPI_Send(&reader_file_ptr,1,MPI_INT,reader_msg,0,MPI_COMM_WORLD);
                                reader_file_ptr++;
                        }
                        while (!readingDone(flag,NUM_PROCESS,num_read_threads)) {
                            MPI_Irecv(&reader_msg,1,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&reader_req_for_work);
                            MPI_Wait(&reader_req_for_work,&reader_req_status);
                                MPI_Send(&read_done,1,MPI_INT,reader_msg,0,MPI_COMM_WORLD);
                                flag[reader_msg]++;
                        }
                    }

                    #pragma omp task shared(scratch_table,map_done)
                    {
                        got_scratch_info[0]=MAP_THREADS-1;
                        while (!readingDone(got_scratch_info,NUM_PROCESS,MAP_THREADS)) {
                            MPI_Irecv(&scratch_buf,NUM_PROCESS+2,MPI_INT,MPI_ANY_SOURCE,1,MPI_COMM_WORLD,&scratch_msg_done);
                            MPI_Wait(&scratch_msg_done,MPI_STATUS_IGNORE);
                            for (i=0;i<NUM_PROCESS;i++) {
                                if(scratch_buf[i]>0) {
                                    omp_set_lock(&scratch_locks[i]);
                                    insertScratchFile(&scratch_table[i],scratch_buf[i],scratch_buf[NUM_PROCESS],scratch_buf[NUM_PROCESS+1]);
                                    // printScratchTable(scratch_table,NUM_PROCESS);
                                    omp_unset_lock(&scratch_locks[i]);
                                }
                            }
                            got_scratch_info[scratch_buf[NUM_PROCESS]]++;
                        }
                        map_done=1;
                    }
                
                    #pragma omp task shared(map_done,scratch_table)
                    {
                        char name[20]="reduce_finish_ptr: ";
                        while (!readingDone(reduce_finish_ptr,NUM_PROCESS,1)) {
                            MPI_Irecv(&reducer_recv_msg,1,MPI_INT,MPI_ANY_SOURCE,2,MPI_COMM_WORLD,&reduce_msg_done);
                            MPI_Wait(&reduce_msg_done,MPI_STATUS_IGNORE);
                            while (1) { 
                                omp_set_lock(&scratch_locks[reducer_recv_msg]);//**
                                get_scratch_file(&scratch_table[reducer_recv_msg],reducer_send_msg);//**
                                omp_unset_lock(&scratch_locks[reducer_recv_msg]);//**
                                if ((reducer_send_msg[0]==reducer_q_empty) && map_done) {
                                    MPI_Send(&reducer_send_msg,3,MPI_INT,reducer_recv_msg,2,MPI_COMM_WORLD);
                                    reduce_finish_ptr[reducer_recv_msg]=1; //**
                                    break;
                                } else if ((reducer_send_msg[0]==reducer_q_empty) && !map_done) {
                                    usleep(100);
                                    continue;
                                } else {
                                    MPI_Send(&reducer_send_msg,3,MPI_INT,reducer_recv_msg,2,MPI_COMM_WORLD);
                                    break;
                                }
                            }
                        }
                    }
                    
                    for (i=0;i<num_read_threads_master;i++) { 
                        #pragma omp task shared(done) 
                        {
                            usleep(0.5);
                            int file_to_read;
                            char* filename;
                            int send_msg = pid;
                            MPI_Status status;
                            while (1) {
                                //sleep(2);
                                MPI_Send(&send_msg,1,MPI_INT,0,0,MPI_COMM_WORLD);
                                MPI_Recv(&file_to_read,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                                if (file_to_read==0) {continue;}
                                if (file_to_read==read_done) {
                                    omp_set_lock(&done_lck);
                                    done++;
                                    omp_unset_lock(&done_lck);
                                    break;              
                                } else {
                                    filename = getReaderFileName(file_to_read);
                                    reader(reader_Q,filename,pid,&lck); 
                                }
                            }
                        }
                    }

                    #pragma omp task shared(done,scratch_table)
                    {
                        int num_scratch[NUM_PROCESS+2];
                        mapper(reader_Q,&done,num_read_threads_master,num_write_threads_master,pid,&num_scratch[0],&lck,0);
                        MPI_Isend(&num_scratch[0],NUM_PROCESS+2,MPI_INT,0,1,MPI_COMM_WORLD,&scratch_info);
                        struct LLitem* rQ=NULL;
                        int send_msg = pid;
                        int recv_msg[2] = {0,0};
                        FILE* reducerFile;
                        char buf[WORD_LENGTH];
                        char reducerFileName[25];
                        int count;
                        MPI_Request reducer_work_req;
                        while(1) {
                            MPI_Send(&send_msg,1,MPI_INT,0,2,MPI_COMM_WORLD); // Explore using diff comm for mapper and reducer
                            MPI_Recv(&recv_msg,3,MPI_INT,0,2,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                            if (recv_msg[0] == reducer_q_empty) {break;}
                            sprintf(reducerFileName,"%d_%d_reducerFile_%d_%d",recv_msg[0],recv_msg[2],pid,recv_msg[1]);
                            reducerFile = fopen(reducerFileName,"r");
                            while (fscanf(reducerFile,"%s%d",buf,&count)!=EOF) {
                                insert(&rQ,buf,count);
                            }
                            fclose(reducerFile);
                            remove(reducerFileName);
                        }
                        writer(&rQ,OUTPUT_WRITE,pid,0);
                    }
                }
            }
            omp_destroy_lock(&lck); 
            for (i=0;i<NUM_PROCESS;i++) {
                omp_destroy_lock(&scratch_locks[i]);
            }
            printf("Master process: All tasks have finished reading and reducing\n");
        } else {
            struct Q* reader_Q = InitQ(READER_Q_SIZE);
            MPI_Request scratch_info;
            omp_lock_t lck, done_lck; //For mapper and readers to synchronize work q
            omp_init_lock(&lck);
            omp_init_lock(&done_lck);
            int done=0; 
            #pragma omp parallel shared(done)
            {
                #pragma omp single 
                {
                    for (int k=0;k<num_read_threads;k++) {
                        #pragma omp task shared(done)
                        {
                            int file_to_read;
                            char* filename;
                            int send_msg = pid;
                            MPI_Recv(&file_to_read,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                            filename=getReaderFileName(file_to_read);
                            while (1) {
                                reader(reader_Q,filename,pid,&lck); 
                                MPI_Send(&send_msg,1,MPI_INT,0,0,MPI_COMM_WORLD);
                                MPI_Recv(&file_to_read,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                                if (file_to_read==read_done) {
                                    omp_set_lock(&done_lck);
                                    done++;
                                    omp_unset_lock(&done_lck);
                                    break;              
                                } else {
                                    filename = getReaderFileName(file_to_read);
                                }
                            } 
                        }
                    }
                    for (i=0;i<MAP_THREADS;i++) {
                        #pragma omp task firstprivate(i)
                        {
                            int num_scratch[NUM_PROCESS+2];
                            mapper(reader_Q,&done,num_read_threads,num_write_threads,pid,&num_scratch[0],&lck,i);
                            num_scratch[NUM_PROCESS]=pid;
                            num_scratch[NUM_PROCESS+1]=i;
                            MPI_Isend(&num_scratch[0],NUM_PROCESS+2,MPI_INT,0,1,MPI_COMM_WORLD,&scratch_info);
                        }
                    }
                }
            }
            omp_destroy_lock(&lck);
            struct LLitem* rQ[REDUCE_THREADS];
            for (i=0;i<REDUCE_THREADS;i++) {
                rQ[i]=NULL;
            }
            int send_msg = pid;
            int recv_msg[3] = {0,0,0};
            FILE *reducerFile;
            char buf[WORD_LENGTH];
            int count, rid;
            char reducerFileName[25];
            MPI_Request reducer_work_req;
            while(1) {
                MPI_Send(&send_msg,1,MPI_INT,0,2,MPI_COMM_WORLD); // Explore using diff comm for mapper and reducer
                MPI_Recv(&recv_msg,3,MPI_INT,0,2,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                if (recv_msg[0] == reducer_q_empty) {break;}
                sprintf(reducerFileName,"%d_%d_reducerFile_%d_%d",recv_msg[0],recv_msg[2],pid,recv_msg[1]);
                reducerFile = fopen(reducerFileName,"r");
                while (fscanf(reducerFile,"%s%d",buf,&count)!=EOF) {
                    rid = ((int) (hashFunc(buf)/NUM_PROCESS)) % REDUCE_THREADS;
                    insert(&rQ[rid],buf,count);
                }
                fclose(reducerFile);
                remove(reducerFileName);
            }
            #pragma omp parallel for num_threads(REDUCE_THREADS)
            for (i=0;i<REDUCE_THREADS;i++)
            {
                writer(&rQ[i],OUTPUT_WRITE,pid,i);
            }
        }
        elapsedTime+=MPI_Wtime();
        if (!pid) {printf("Time taken to process all files is %.2lfs\n",elapsedTime);}
        MPI_Finalize();
    } else {
        int read_file_ptr=0;
        int reducer_id_track=0;
        int num_read_threads = READ_THREADS;
        int num_write_threads = WRITE_THREADS;
        int i,j;
        int done=0;
        int map_done=0;
        struct Q* reader_Q = InitQ(READER_Q_SIZE);
        omp_lock_t read_file_lck,reader_q_lck,reducer_id_lck;
        omp_lock_t reducer_q_lck[REDUCE_THREADS];
        omp_init_lock(&read_file_lck);
        omp_init_lock(&reader_q_lck);
        omp_init_lock(&reducer_id_lck);
        struct LLitem* hTable[REDUCE_THREADS];
        struct LLitem* rQ[REDUCE_THREADS];
        for (i=0;i<REDUCE_THREADS;i++) {
            omp_init_lock(&reducer_q_lck[i]);
            hTable[i]=NULL;
            rQ[i]=NULL;
        }
        printf("Doing Mapreduce in openmp using %d readers, %d mappers and %d reducers\n",READ_THREADS,MAP_THREADS,REDUCE_THREADS);
        double elapsed_time = -omp_get_wtime();
        #pragma omp parallel
        {
            #pragma omp single
            {
                for(i=0;i<READ_THREADS;i++) {
                    #pragma omp task shared(read_file_ptr,done) // reader thread
                    {
                        printf("thread %d in reader\n",omp_get_thread_num());
                        int file_to_read;
                        char* filename;
                        while (1) {
                            omp_set_lock(&read_file_lck);
                            file_to_read=read_file_ptr++;
                            omp_unset_lock(&read_file_lck);
                            if (file_to_read>=NUM_FILE_CHUNKS) {break;}
                            filename = getReaderFileName(file_to_read);
                            printf("reader thread %d got %s file to read\n",omp_get_thread_num(),filename);
                            reader(reader_Q, filename, 0, &reader_q_lck);
                        }
                        printf("thread %d done reading\n",omp_get_thread_num());
                        done++;
                    }
                }


                for (i=0;i<MAP_THREADS;i++) {
                    #pragma omp task shared(map_done,hTable,rQ,done)
                    {
                        printf("thread %d in mapper\n",omp_get_thread_num());
                        openmp_mapper(reader_Q,&done,num_read_threads,rQ,&reader_q_lck,reducer_q_lck);
                        map_done++;
                        printf("thread %d is done mapping, map_done=%d\n",omp_get_thread_num(),map_done);
                    }
                }

                for(i=0;i<REDUCE_THREADS;i++) {
                    #pragma omp task shared(map_done,rQ)
                    {
                        omp_set_lock(&reducer_id_lck);
                        int rid = reducer_id_track++;
                        omp_unset_lock(&reducer_id_lck);
                        reducer(rid,&rQ[rid],&reducer_q_lck[rid],&map_done);
                    }
                }
            }
        }
        elapsed_time+=omp_get_wtime();
        printf("Time taken to process all files is %.2lfs\n",elapsed_time);
    }
}
