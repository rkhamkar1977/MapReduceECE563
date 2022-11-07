#include <stdio.h>
#include <mpi.h>
#include <omp.h>
#include <string.h>
#include <unistd.h>

#define LINE_LENGTH 256
#define WORD_LENGTH 70 //Making it 32 because some word strings are > 20
#define NUM_PROCESS 2
#define NUM_FILE_CHUNKS 100
#define READER_Q_SIZE 100000
#define KEYS_PER_REDUCER 150
#define OUTPUT_WRITE 10001
#define READ_THREADS 3
#define WRITE_THREADS 3


typedef struct Q {
    int size;
    int pos;
    char** QHead;
} Q;


typedef struct scratchlist {
    int file;
    int map;
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
    //char* str = (char*) malloc(sizeof(char)*LINE_LENGTH);
    //printf("In getWorkQ\n");
    if (W->pos>-1) {
        //printf("DEBUG: %s length is %d\n",W->QHead[W->pos],strlen(W->QHead[W->pos])); 
        memcpy(buf,W->QHead[W->pos],(strlen(W->QHead[W->pos]))*sizeof(char));
        //printf("Done Memcpy\n");
        //str=W->QHead[W->pos];
        W->pos--;
        return 1;
    } else {
        //printf("ERROR:Trying to get work from empty Work Q\n");
        return 0;
    }
}

int putWorkWQ (struct Q* W, char* buf) {
    if (W->pos < W->size-1) {
        W->pos++;
        //printf("Inside putWorkWQ, workpos is %d\n",W->pos);
        memcpy(W->QHead[W->pos],buf,(strlen(buf)+1)*sizeof(char));
        return 1;
    } else {
        //printf("Trying to push into full Q\n");
        return 0;
    }
}

void reader(struct Q* W, char* fileName, int pid, omp_lock_t* lck) {
    //printf("PID %d In reader, File %s\n",pid,fileName);
    //char* fileName="2.txt";
    int lines = 0;
    int returnVal=1;
    FILE* file = fopen(fileName,"r");
    char* buf = (char*) malloc(LINE_LENGTH*sizeof(char));
    while (fgets(buf,LINE_LENGTH,file) != NULL) {
        if (buf[0]!='\n') {
            lines++;
            //printf("Putting %s into Work queue\n",buf);
            do {
            omp_set_lock(lck);
            returnVal=putWorkWQ(W,buf);
            omp_unset_lock(lck);
            if (returnVal==0) {
                usleep(5);
                printf("PID %d Q currently full, reader waiting\n",pid);
            }
        } while(returnVal==0);
        //printf("Put %s into Work queue\n",buf);
        }
    }
    //printf("PID %d, Processed file %s, num lines: %d\n",pid,fileName,lines);
    //printf("PID %d, Processed file %s, work q position %d\n",pid,fileName,W->pos);    
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
            //printf("Printing word at %p\n",&((*(h+i))->word));
            // adding comment to test commit
            do {
                printf(" {%s,%d} ",p->word,p->cnt);
                p = p->nextptr;
                //printf("%d next pointer\n",(p==NULL));
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
            //printf("Printing word at %p\n",&((*(h+i))->word));
            do {
                printf(" -> {%d,%d} ",p->map,p->file);
                p = p->nextptr;
                //printf("%d next pointer\n",(p==NULL));
            } while(p!=NULL);
        }
        else {printf(" NULL ");}
        printf("\n");
    }
} 

void insertScratchFile (struct scratchlist** h, int file, int map) {
    struct scratchlist* p = *h;
    struct scratchlist* elem = (struct scratchlist*) malloc(sizeof(struct scratchlist));
    *h = elem;
    elem->file=file;
    elem->map=map;
    //printf("Writing word %s to %p\n",elem->word,&(elem->word));
    elem->nextptr=p;
    return;
}

void insert (struct LLitem** h, char* w, int c) {
    if (strlen(w)>WORD_LENGTH) {
        //printf("Word %s is bigger than word length, might lead to Malloc issues",w);
        return;
    }
    if (*h==NULL) {
        struct LLitem* elem = (struct LLitem*) malloc(sizeof(struct LLitem));
        *h = elem;
        elem->cnt=c;
        elem->word=(char*) malloc(WORD_LENGTH*sizeof(char));
        memcpy(elem->word,w,sizeof(char)*(strlen(w)+1));
        //printf("Writing word %s to %p\n",elem->word,&(elem->word));
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
        //printf("Writing word %s to %p\n",elem->word,&(elem->word));
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

int writer(struct LLitem** h, int index, int pid) {
    struct LLitem* p;
    int file_count = 0;
    char filename[30];
    if (index != OUTPUT_WRITE) {
        while (*h!=NULL) {
            sprintf(filename,"%d_reducerFile_%d_%d",pid,index,file_count);
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
        //printf("PID %d Index %d word count %d\n",pid,index,file_count);
        return file_count;
    } else {
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
}

void printFlag(int* flag, int n, char* name) {
    int i =0;
    printf("%s: ",name);
    for (i=0;i<n;i++) {
        printf("%d ",*(flag+i));
    }
    printf("\n");
}

void mapper(struct Q* W, int* done, int num_read_threads, int num_write_threads, int pid, int* num_scratch_files, omp_lock_t* lck) {
    //printf("PID %d in Mapper\n",pid);
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
        //printf("PID %d going into getWorkQ\n",pid);
        buf = startptr;
        workReturnVal=getWorkWQ(W, buf);
        //printf("DEBUG 1: %s length is %d\n",buf,strlen(buf)); 
        omp_unset_lock(lck);
        if (workReturnVal) {
            lines++;
            //printf("PID %d Pulled string %s from buffer\n",pid,buf);
            do {
                returnVal=getWord(&buf,word);
                //printf("DEBUG 2: %s length is %d\n",buf,strlen(buf)); 
                normalizeWord(word);
                if (!strlen(word)) {continue;}
                //printf("in mapper loop %s\n",word);
                //printf("%s\n",word);
                hIndex=hashFunc(word)%NUM_PROCESS;
                //printf("Pushing %s into table (Hash value %d)\n",word, hIndex);
                insert(&hTable[hIndex],word,1);
            } while(returnVal);
            //printf("out of mapper loop\n");
        } else {
            if (*done==num_read_threads) {
                //printf("PID %d done mapping\n",pid);
                break;
            }
            else {
                usleep(1500);
                //printf("PID %d Empty Q but reading not done\n",pid);
                continue;
            }
        }
    } 
    //printf("Reached scratch mapping\n");
    //printTable(hTable,NUM_PROCESS);
    //int num_scratch_files[NUM_PROCESS];
    //#pragma omp parallel for num_threads(num_write_threads)
        for (i=0;i<NUM_PROCESS;i++) {
            num_scratch_files[i]=writer(&hTable[i],i,pid); // Need to parallelize this
        }
    //printf("PID %d, Total lines processed %d\n",pid,lines);
    if (pid == 1) {
        //printTable(hTable,NUM_PROCESS);
        //char name[18]="Num Scratch Files";
        //printFlag(num_scratch_files,NUM_PROCESS,name);
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
    //int* retVal = (int*) malloc(sizeof(int)*2);
    *retVal=999;
    *(retVal+1)=999;
    while(1) {
        if (*h == NULL) {break;}
        if ((*h)->file>0) {
            (*h)->file--;
            *retVal = (*h)->map;
            *(retVal+1) = (*h)->file;
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

int main (int argc, char *argv[]) {
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
    MPI_Barrier(MPI_COMM_WORLD);
    double elapsedTime = -MPI_Wtime();
    int i,j;

    if (numP>1) {
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
            int reducer_send_msg[2] = {0,0};
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
            ///New declarations here:
            struct Q* reader_Q = InitQ(READER_Q_SIZE);
            MPI_Request scratch_info;
            int num_scratch[NUM_PROCESS+1];
            omp_lock_t lck, done_lck; //For mapper and readers to synchronize work q
            omp_init_lock(&lck);
            omp_init_lock(&done_lck);
            int done=0; //When making multi-threaded reader need to make this an array

            //No reducer should finish if mapping isn't finished
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
            //MPI Master process messaging bit : 0 - reader asking for work, 2 -reducer asking for file, 1 - Mapper giving scratch info
            //MPI Master messaging ettiquette - send 2 integers - 1st is your pid and second is messaging bit - 0/1/2
            //MPI Process tag for getting and asking for work - 0
            //MPI_Request node_init_done;
            MPI_Request scratch_msg_done, reduce_msg_done, node_init_done;
            //Assuming numP is < num file chunks
            //Need to create different tasks for each loop
            for (i=1;i<numP;i++) {
                for (j=0;j<num_read_threads;j++) {
                    MPI_Isend(&reader_file_ptr,1,MPI_INT,i,0,MPI_COMM_WORLD,&node_init_done);
                    //printf("PID %d sending file %d.txt to destination %d\n",pid,reader_file_ptr,i);
                    MPI_Wait(&node_init_done,MPI_STATUS_IGNORE);
                    reader_file_ptr++;
                }
            }
            //Need to add if condition for case when initial distribution exhausts files
            //MPI_Waitall(numP-1,node_init_done,MPI_STATUSES_IGNORE); Don't need a waitall - what if one node finishes file given to it initially> shouldnt have to wait
            // while (reader_file_ptr<NUM_FILE_CHUNKS) {
            //     MPI_Irecv(&reader_msg,1,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&reader_req_for_work);
            //     MPI_Wait(&reader_req_for_work,&reader_req_status);
            //     //if (reader_msg[1] == 0) {
            //         MPI_Send(&reader_file_ptr,1,MPI_INT,reader_msg,0,MPI_COMM_WORLD);
            //         //printf("PID %d sending file %d.txt to destination %d\n",pid,reader_file_ptr,reader_msg[0]);
            //         omp_set_lock(&reader_file_ptr_lck);
            //         reader_file_ptr++;
            //         omp_unset_lock(&reader_file_ptr_lck);
            //     //}
            // }
            #pragma omp parallel
            {
                #pragma omp single
                {
                    #pragma omp task shared(read_done,flag,reader_file_ptr)
                    {
                        while (reader_file_ptr<NUM_FILE_CHUNKS) {
                            MPI_Irecv(&reader_msg,1,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&reader_req_for_work);
                            MPI_Wait(&reader_req_for_work,&reader_req_status);
                            //if (reader_msg[1] == 0) {
                                MPI_Send(&reader_file_ptr,1,MPI_INT,reader_msg,0,MPI_COMM_WORLD);
                                //printf("PID %d sending file %d.txt to destination %d\n",pid,reader_file_ptr,reader_msg[0]);
                                reader_file_ptr++;
                            //}
                        }
                        while (!readingDone(flag,NUM_PROCESS,num_read_threads)) {
                            MPI_Irecv(&reader_msg,1,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&reader_req_for_work);
                            MPI_Wait(&reader_req_for_work,&reader_req_status);
                            //if (reader_msg[1]==0) {
                                MPI_Send(&read_done,1,MPI_INT,reader_msg,0,MPI_COMM_WORLD);
                                //printf("PID %d sending %d to destination %d\n",pid,read_done,reader_msg);
                                flag[reader_msg]++;
                                char name[20] = "MPI_READ_FLAG";
                                //printFlag(flag,NUM_PROCESS,name);
                            //} else if (reader_msg[1]==1) {
                                //Reading not done for all tasks but some tasks have already started reducer work - need to add here
                            //}
                        }
                    }

                    #pragma omp task shared(scratch_table,map_done)
                    {
                        while (!readingDone(got_scratch_info,NUM_PROCESS,1)) {
                            MPI_Irecv(&scratch_buf,NUM_PROCESS+1,MPI_INT,MPI_ANY_SOURCE,1,MPI_COMM_WORLD,&scratch_msg_done);
                            MPI_Wait(&scratch_msg_done,MPI_STATUS_IGNORE);
                            //printf("Got scratch info from pid %d\n",scratch_buf[NUM_PROCESS]);
                            // char name[20]="Got_scratch: ";
                            // printFlag(got_scratch_info,NUM_PROCESS,name);
                            for (i=0;i<NUM_PROCESS;i++) {
                                //scratch_ptr[i][scratch_buf[NUM_PROCESS]-1]=scratch_buf[i];
                                if(scratch_buf[i]>0) {
                                    omp_set_lock(&scratch_locks[i]);
                                    insertScratchFile(&scratch_table[i],scratch_buf[i],scratch_buf[NUM_PROCESS]);
                                    omp_unset_lock(&scratch_locks[i]);
                                }
                            }
                            got_scratch_info[scratch_buf[NUM_PROCESS]]=1;
                        }
                        //printScratchTable(scratch_table,NUM_PROCESS);
                        map_done=1;
                    }
                
                    #pragma omp task shared(map_done,scratch_table)
                    {
                        char name[20]="reduce_finish_ptr: ";
                        while (!readingDone(reduce_finish_ptr,NUM_PROCESS,1)) {
                            MPI_Irecv(&reducer_recv_msg,1,MPI_INT,MPI_ANY_SOURCE,2,MPI_COMM_WORLD,&reduce_msg_done);
                            MPI_Wait(&reduce_msg_done,MPI_STATUS_IGNORE);
                            //printFlag(reduce_finish_ptr,NUM_PROCESS,name);
                            //printf("PID %d asking for reducerfile\n",reducer_recv_msg);
                            while (1) { 
                                omp_set_lock(&scratch_locks[reducer_recv_msg]);//**
                                get_scratch_file(&scratch_table[reducer_recv_msg],reducer_send_msg);//**
                                omp_unset_lock(&scratch_locks[reducer_recv_msg]);//**
                                if ((reducer_send_msg[0]==reducer_q_empty) && map_done) {
                                    MPI_Send(&reducer_send_msg,2,MPI_INT,reducer_recv_msg,2,MPI_COMM_WORLD);
                                    reduce_finish_ptr[reducer_recv_msg]=1; //**
                                    break;
                                } else if ((reducer_send_msg[0]==reducer_q_empty) && !map_done) {
                                    //printf("Map not done, reducer sleeping\n");
                                    usleep(100);
                                    continue;
                                } else {
                                    MPI_Send(&reducer_send_msg,2,MPI_INT,reducer_recv_msg,2,MPI_COMM_WORLD);
                                    break;
                                }
                            }
                        }
                    }

                    for (i=0;i<num_read_threads_master;i++) { 
                        #pragma omp task shared(done) //Need a lock for reader_file_ptr
                        {
                            int file_to_read;
                            char* filename;
                            int send_msg = pid;
                            // MPI_Recv(&file_to_read,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                            // filename=getReaderFileName(file_to_read);
                            //printf("PID: %d, Thread %d Got file %s to read from Master process\n",pid,omp_get_thread_num(),filename);
                            while (1) {
                                //sleep(2);
                                MPI_Send(&send_msg,1,MPI_INT,0,0,MPI_COMM_WORLD);
                                MPI_Recv(&file_to_read,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                                if (file_to_read==read_done) {
                                    //printf("PID: %d, Thread %d got read done signal\n",pid,omp_get_thread_num());      
                                    omp_set_lock(&done_lck);
                                    done++;
                                    omp_unset_lock(&done_lck);
                                    //printf("PID: %d, Thread %d Done reading\n",pid,omp_get_thread_num());  
                                    break;              
                                } else {
                                    filename = getReaderFileName(file_to_read);
                                    //printf("PID: %d, Thread %d Got file %s to read from Master process\n",pid,omp_get_thread_num(),filename);
                                    reader(reader_Q,filename,pid,&lck); //What happens if Q is full but files are not finished yet? Should wait - need to update 
                                }
                            }
                        }
                    }

                    #pragma omp task shared(done,scratch_table)
                    {
                        mapper(reader_Q,&done,num_read_threads_master,num_write_threads_master,pid,&num_scratch[0],&lck);
                        num_scratch[NUM_PROCESS]=pid;
                        //printf("PID %d All tasks finished, sending scratch info\n",pid);
                        MPI_Isend(&num_scratch[0],NUM_PROCESS+1,MPI_INT,0,1,MPI_COMM_WORLD,&scratch_info);
                        omp_destroy_lock(&lck); 
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
                            MPI_Recv(&recv_msg,2,MPI_INT,0,2,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                            // omp_set_lock(&scratch_locks[0]);//**
                            // get_scratch_file(&scratch_table[0],recv_msg);//**
                            // omp_unset_lock(&scratch_locks[0]);//**
                            if (recv_msg[0] == reducer_q_empty) {break;}
                            sprintf(reducerFileName,"%d_reducerFile_%d_%d",recv_msg[0],pid,recv_msg[1]);
                            //printf("PID %d Reducer %d got file %s from Master\n",pid,pid,reducerFileName);
                            reducerFile = fopen(reducerFileName,"r");
                            //sleep(2);
                            while (fscanf(reducerFile,"%s%d",buf,&count)!=EOF) {
                                insert(&rQ,buf,count);
                            }
                            fclose(reducerFile);
                            remove(reducerFileName);
                        }
                        writer(&rQ,OUTPUT_WRITE,pid);
                    }
                }
            }
            for (i=0;i<NUM_PROCESS;i++) {
                omp_destroy_lock(&scratch_locks[i]);
            }
            printf("Master process: All tasks have finished reading and reducing\n");
            //All tasks have finished reading and now all are running reducer work - need to add here - update - added
        } else {
            struct Q* reader_Q = InitQ(READER_Q_SIZE);
            MPI_Request scratch_info;
            int num_scratch[NUM_PROCESS+1];
            omp_lock_t lck, done_lck; //For mapper and readers to synchronize work q
            omp_init_lock(&lck);
            omp_init_lock(&done_lck);
            int done=0; //When making multi-threaded reader need to make this an array
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
                            //printf("PID: %d, Thread %d Got file %s to read from Master process\n",pid,omp_get_thread_num(),filename);
                            while (1) {
                                reader(reader_Q,filename,pid,&lck); //What happens if Q is full but files are not finished yet? Should wait - need to update
                                //sleep(2);
                                MPI_Send(&send_msg,1,MPI_INT,0,0,MPI_COMM_WORLD);
                                MPI_Recv(&file_to_read,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                                if (file_to_read==read_done) {
                                    //printf("PID: %d, Thread %d got read done signal\n",pid,omp_get_thread_num());      
                                    omp_set_lock(&done_lck);
                                    done++;
                                    omp_unset_lock(&done_lck);
                                    //printf("PID: %d, Thread %d Done reading\n",pid,omp_get_thread_num());  
                                    break;              
                                } else {
                                    filename = getReaderFileName(file_to_read);
                                    //printf("PID: %d, Thread %d Got file %s to read from Master process\n",pid,omp_get_thread_num(),filename);
                                }
                            } 
                        }
                    }
                    #pragma omp task
                        mapper(reader_Q,&done,num_read_threads,num_write_threads,pid,&num_scratch[0],&lck);
                }
            }
            num_scratch[NUM_PROCESS]=pid;
            //printf("PID %d All tasks finished, sending scratch info\n",pid);
            MPI_Isend(&num_scratch[0],NUM_PROCESS+1,MPI_INT,0,1,MPI_COMM_WORLD,&scratch_info);
            omp_destroy_lock(&lck);
            //char name[18] = "Num Scratch";
            //printFlag(num_scratch,NUM_PROCESS,name);
            //Reducer code
            struct LLitem* rQ=NULL;
            int send_msg = pid;
            int recv_msg[2] = {0,0};
            FILE *reducerFile;
            char buf[WORD_LENGTH];
            int count;
            char reducerFileName[25];
            MPI_Request reducer_work_req;
            while(1) {
                MPI_Send(&send_msg,1,MPI_INT,0,2,MPI_COMM_WORLD); // Explore using diff comm for mapper and reducer
                MPI_Recv(&recv_msg,2,MPI_INT,0,2,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                if (recv_msg[0] == reducer_q_empty) {break;}
                sprintf(reducerFileName,"%d_reducerFile_%d_%d",recv_msg[0],pid,recv_msg[1]);
                //printf("PID %d Reducer %d got file %s from Master\n",pid,pid,reducerFileName);
                reducerFile = fopen(reducerFileName,"r");
                //sleep(2);
                while (fscanf(reducerFile,"%s%d",buf,&count)!=EOF) {
                    insert(&rQ,buf,count);
                }
                fclose(reducerFile);
                remove(reducerFileName);
            }
            writer(&rQ,OUTPUT_WRITE,pid);
        }
    }
    elapsedTime+=MPI_Wtime();
    if (!pid) {printf("Time taken to process all files is %.2lfs\n",elapsedTime);}
    MPI_Finalize();
}
