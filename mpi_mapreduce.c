#include <stdio.h>
#include <mpi.h>
#include <omp.h>
#include <string.h>
#include <unistd.h>

#define LINE_LENGTH 256
#define WORD_LENGTH 33 //Making it 32 because some word strings are > 20
#define NUM_REDUCERS 8
#define NUM_FILE_CHUNKS 20
#define READER_Q_SIZE 5000
#define KEYS_PER_REDUCER 150

typedef struct Q {
    int size;
    int pos;
    char** QHead;
} Q;


typedef struct keyData {
    int cnt;
    char* word;
} keyData;

typedef struct reducerQ {
    int size;
    int pos;
    keyData** k;
} reducerQ;

typedef struct LLitem {
    int cnt;
    char* word;
    struct LLitem* nextptr;
} LLitem;

struct reducerQ* InitReducerQ (int size) {
    struct reducerQ* r = (struct reducerQ*) malloc(sizeof(reducerQ));
    r->size = size;
    r->pos = -1;
    int i;
    for (i=0;i<size;i++) {
        *(r->k + i) = (keyData*) malloc(sizeof(keyData));
        (*(r->k + i))->word = (char*) malloc(sizeof(char)*WORD_LENGTH);
        (*(r->k + i))->cnt = 0;
    }
    return r;
}


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

char* getWorkWQ (struct Q* W) {
    char* str;
    if (W->pos>-1) {
        str=W->QHead[W->pos];
        W->pos--;
        return str;
    } else {
        printf("ERROR:Trying to get work from empty Work Q\n");
        return NULL;
    }
}

int putWorkWQ (struct Q* W, char* buf) {
    if (W->pos < W->size-1) {
        W->pos++;
        //printf("Inside putWorkWQ, workpos is %d\n",W->pos);
        memcpy(W->QHead[W->pos],buf,(strlen(buf)+1)*sizeof(char));
        return 1;
    } else {
        printf("Trying to push into full Q\n");
        return 0;
    }
}

void reader(struct Q* W, char* fileName, int pid, omp_lock_t* lck) {
    if (pid == 1) {printf("In reader, File %s\n",fileName);}
    //char* fileName="2.txt";
    int returnVal=1;
    FILE* file = fopen(fileName,"r");
    char* buf = (char*) malloc(LINE_LENGTH*sizeof(char));
    while (fgets(buf,LINE_LENGTH,file) && returnVal) {
        if (buf[0]!='\n') {
        //if (pid==1) {printf("Putting %s into the Work queue\n",buf);}
        omp_set_lock(lck);
        returnVal=putWorkWQ(W,buf);
        omp_unset_lock(lck);
        //if (pid==1) {printf("Put %s into the Work queue\n",buf);}
        }
    }
    printf("PID %d, Processed file %s, Work Q pos %d of %d full\n",pid,fileName,W->pos,W->size);
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
    int flag[n];
    int i,j=0;
    for (i=0;i<n;i++) {
        flag[i]=0;
        if (w[i]<=90 && w[i]>=65) {w[i]+=32;}
        if (!((w[i]>=65 && w[i]<=90) || (w[i]>=97 && w[i]<=122))) {
            flag[i]=1;
            j++;
        }
    }
    if (j!=n) {
        if (!((w[0]>=65 && w[0]<=90) || (w[0]>=97 && w[0]<=122))) {
            for (i=0;i<n;i++) {
                w[i] = w[i+1];
            }
            n=n-1;
        }
        if (!((w[n-1]>=65 && w[n-1]<=90) || (w[n-1]>=97 && w[n-1]<=122))) {
            w[n-1]=0;
            n=n-1;
        }
    }
    // int cnt=0;
    // for (i=0;i<n-j;i++) {
    //     w[i-cnt]=w[i];
    //     if (flag[i]) {cnt++;}
    // }
    // w[i]=0;
}

void printTable(struct LLitem** h, int n) {
    int i;
    struct LLitem *p;
    for (i=0;i<n;i++) {
        printf("H[%d]",i);
        if ( *(h+i) != NULL) {
            p = *(h+i);
            //printf("Printing word at %p\n",&((*(h+i))->word));
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

void insert (struct LLitem** h, char* w) {
    if (strlen(w)>WORD_LENGTH) {printf("Word %s is bigger than word length",w);}
    if (*h==NULL) {
        struct LLitem* elem = (struct LLitem*) malloc(sizeof(struct LLitem));
        *h = elem;
        elem->cnt=1;
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
            p->cnt++;
            flag=1;
            break;
        }
        p=p->nextptr;
    }while (p!=NULL);
    if (!flag) {
        struct LLitem* elem = (struct LLitem*) malloc(sizeof(struct LLitem));
        p=*h;
        *h = elem;
        elem->cnt=1;
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
    while (*h!=NULL) {
        sprintf(filename,"%d_reducerFile_%d_%d",pid,index,file_count);
        FILE* f = fopen(filename,"w");
        int count = 0;
        while ((*h!=NULL) && (count<KEYS_PER_REDUCER)) {
            p = *h;
            fprintf(f,"%s,%d\n",p->word,p->cnt);
            *h = p->nextptr;
            count++;
        }
        fclose(f);
        file_count++;
    }
    //printf("PID %d Index %d word count %d\n",pid,index,file_count);
    return file_count;
}

void printFlag(int* flag, int n, char* name) {
    int i =0;
    printf("%s: ",name);
    for (i=0;i<n;i++) {
        printf("%d ",*(flag+i));
    }
    printf("\n");
}

void mapper(struct Q* W, int* done, int num_read_threads, int pid, int* num_scratch_files, omp_lock_t* lck) {
    printf("PID %d in Mapper\n",pid);
    struct LLitem* hTable[NUM_REDUCERS];
    int i;
    for (i=0;i<NUM_REDUCERS;i++) {
        hTable[i]=NULL;
    }
    int hIndex;
    int returnVal;
    char* buf = (char*) malloc(LINE_LENGTH*sizeof(char));
    char* word = (char*) malloc(WORD_LENGTH*sizeof(char));
    while(1) {
        omp_set_lock(lck);
        buf=getWorkWQ(W);
        omp_unset_lock(lck);
        if (buf!=NULL) {
            //printf("PID %d Pulled string %s from the buffer\n",pid,buf);
            do {
                returnVal=getWord(&buf,word);
                normalizeWord(word);
                if (!strlen(word)) {continue;}
                //printf("%s\n",word);
                hIndex=hashFunc(word)%NUM_REDUCERS;
                //printf("Pushing %s into table (Hash value %d)\n",word, hIndex);
                insert(&hTable[hIndex],word);
            } while(returnVal);
        } else {
            if (*done==num_read_threads) {
                break;
            }
            else {
                usleep(1500);
                printf("PID %d Empty Q but reading not done\n",pid);
                continue;
            }
        }
    } 
    if (pid == 1) {printTable(hTable,NUM_REDUCERS);}
    //int num_scratch_files[NUM_REDUCERS];
    for (i=0;i<NUM_REDUCERS;i++) {
        num_scratch_files[i]=writer(&hTable[i],i,pid);
    }
    printf("PID %d Out of the mapper loop\n",pid);
    if (pid == 1) {
        printTable(hTable,NUM_REDUCERS);
        //char name[18]="Num Scratch Files";
        //printFlag(num_scratch_files,NUM_REDUCERS,name);
    }
    return;
}

struct keyData* pullFromHashTable(struct LLitem** h) {
    struct keyData* k;
    if (*h != NULL) {
        k = (struct keyData*) malloc(sizeof(keyData));
        k->cnt = (*h)->cnt;
        k->word = (char*) malloc(sizeof(WORD_LENGTH*sizeof(char)));
        memcpy(k->word,(*h)->word,(strlen((*h)->word)+1)*sizeof(char));
        *h = (*h)->nextptr;
        return k;
    } else return NULL;
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
    int num_read_threads = 3;
    int pid;
    int numP,provided;
    MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&provided);
    if (provided != MPI_THREAD_MULTIPLE)
    {
        printf("Sorry, this MPI implementation does not support multiple threads\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    MPI_Comm_size(MPI_COMM_WORLD,&numP);
    int num_readers = numP-1;
    MPI_Comm_rank(MPI_COMM_WORLD,&pid);
    int i,j;

    if (!pid) {
        int reader_file_ptr = 0;
        int reader_msg[2] = {0,0};
        int reader_pid;
        MPI_Request reader_req_for_work;
        MPI_Status reader_req_status;
        int flag[num_readers];
        int got_scratch_info[num_readers];
        int scratch_buf[NUM_REDUCERS+1];
        int scratch_ptr[NUM_REDUCERS];
        for (i=0;i<num_readers;i++) {
            flag[i] = 0;
        }
        for (i=0;i<num_readers;i++) {
            got_scratch_info[i] = 0;
        }
        //MPI Master process messaging bit : 0 - reader asking for work, 2 -reducer asking for file, 1 - Mapper giving scratch info
        //MPI Master messaging ettiquette - send 2 integers - 1st is your pid and the second is messaging bit - 0/1/2
        //MPI Process tag for getting and asking for work - 0
        MPI_Request node_init_done;
        //Assuming numP is < num file chunks
        for (i=1;i<numP;i++) {
            for (j=0;j<num_read_threads;j++) {
                MPI_Isend(&reader_file_ptr,1,MPI_INT,i,0,MPI_COMM_WORLD,&node_init_done);
                //printf("PID %d sending file %d.txt to destination %d\n",pid,reader_file_ptr,i);
                MPI_Wait(&node_init_done,MPI_STATUS_IGNORE);
                reader_file_ptr++;
            }
        }
        //MPI_Waitall(numP-1,node_init_done,MPI_STATUSES_IGNORE); Don't need a waitall - what if one node finishes the file given to it initially> shouldnt have to wait
        while (reader_file_ptr<NUM_FILE_CHUNKS) {
            MPI_Irecv(&reader_msg,2,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&reader_req_for_work);
            MPI_Wait(&reader_req_for_work,&reader_req_status);
            if (reader_msg[1] == 0) {
                MPI_Send(&reader_file_ptr,1,MPI_INT,reader_msg[0],0,MPI_COMM_WORLD);
                //printf("PID %d sending file %d.txt to destination %d\n",pid,reader_file_ptr,reader_msg[0]);
                reader_file_ptr++;
            }
        }
        while (!readingDone(flag,num_readers,num_read_threads)) {
            MPI_Irecv(&reader_msg,2,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&reader_req_for_work);
            MPI_Wait(&reader_req_for_work,&reader_req_status);
            if (reader_msg[1]==0) {
                MPI_Send(&read_done,1,MPI_INT,reader_msg[0],0,MPI_COMM_WORLD);
                //printf("PID %d sending DONE to destination %d\n",pid,reader_msg[0]);
                flag[reader_msg[0]-1]++;
                //char name[20] = "MPI_READ_FLAG";
                //printFlag(flag,num_readers,name);
            //} else if (reader_msg[1]==1) {
                //Reading not done for all tasks but some tasks have already started reducer work - need to add here
            }
        }
        // while (!readingDone(got_scratch_info,num_readers,1)) {
        //     MPI_Irecv();
        // }
        printf("Master process: All tasks have finished reading\n");
        //All tasks have finished reading and now all are running reducer work - need to add here
    } else {
        struct Q* reader_Q = InitQ(READER_Q_SIZE);
        MPI_Request scratch_info;
        int num_scratch[NUM_REDUCERS+1];
        omp_lock_t lck, done_lck; //For mapper and readers to synchronize the work q
        omp_init_lock(&lck);
        omp_init_lock(&done_lck);
        int done=0; //When making multi-threaded reader need to make this an array
        #pragma omp parallel shared(done)
        {
            #pragma omp single 
            {
                for (int k=0;k<num_read_threads;k++) {
                    #pragma omp task 
                    {
                        int file_to_read;
                        char* filename;
                        int send_msg[2] = {pid,0};
                        MPI_Recv(&file_to_read,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                        filename=getReaderFileName(file_to_read);
                        printf("PID: %d, Thread %d Got file %s to read from Master process\n",pid,omp_get_thread_num(),filename);
                        while (1) {
                            reader(reader_Q,filename,pid,&lck); //What happens if Q is full but files are not finished yet? Should wait - need to update
                            //sleep(2);
                            MPI_Send(&send_msg,2,MPI_INT,0,0,MPI_COMM_WORLD);
                            MPI_Recv(&file_to_read,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
                            if (file_to_read==read_done) {
                                omp_set_lock(&done_lck);
                                done++;
                                omp_unset_lock(&done_lck);
                                printf("PID: %d, Thread %d Done reading\n",pid,omp_get_thread_num());  
                                break;              
                            } else {
                                filename = getReaderFileName(file_to_read);
                                printf("PID: %d, Thread %d Got file %s to read from Master process\n",pid,omp_get_thread_num(),filename);
                            }
                        } 
                    }
                }
                #pragma omp task
                    mapper(reader_Q,&done,num_read_threads,pid,&num_scratch[0],&lck);
            }
        }
        num_scratch[NUM_REDUCERS]=pid;
        // MPI_Isend(&num_scratch[0],NUM_REDUCERS+1,MPI_INT,0,1,MPI_COMM_WORLD,&scratch_info);
        omp_destroy_lock(&lck);
        //char name[18] = "Num Scratch";
        //printFlag(num_scratch,NUM_REDUCERS,name);
    }
    MPI_Finalize();
}
