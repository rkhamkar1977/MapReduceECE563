#include <stdio.h>
#include <omp.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

#define LINE_LENGTH 256
#define WORD_LENGTH 70 
#define NUM_FILE_CHUNKS 20
#define READER_Q_SIZE 100000
#define OUTPUT_WRITE 10001
#define READ_THREADS 2
#define WRITE_THREADS 2
#define MAP_THREADS 2
#define NUM_REDUCERS 2



typedef struct Q {
    int size;
    int pos;
    char** QHead;
} Q;

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
        printf("ERROR:Trying to get work from empty Work Q\n");
        return 0;
    }
}

int putWorkWQ (struct Q* W, char* buf) {
    if (W->pos < W->size-1) {
        W->pos++;
        memcpy(W->QHead[W->pos],buf,(strlen(buf)+1)*sizeof(char));
        return 1;
    } else {
        printf("Trying to push into full Q\n");
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
                printf("PID %d Q currently full, reader waiting\n",pid);
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

int writer(struct LLitem** h, int pid) {
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

void printFlag(int* flag, int n, char* name) {
    int i =0;
    printf("%s: ",name);
    for (i=0;i<n;i++) {
        printf("%d ",*(flag+i));
    }
    printf("\n");
}

void mapper(struct Q* W, int* done, int num_read_threads, struct LLitem** reducerQ, omp_lock_t* lck, omp_lock_t* reducer_q_lck) {
    struct LLitem* hTable[NUM_REDUCERS];
    int i;
    for (i=0;i<NUM_REDUCERS;i++) {
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
                hIndex=hashFunc(word)%NUM_REDUCERS;
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
    printf("thread %d Mapping Done, sending files to reducer\n",omp_get_thread_num());
    #pragma omp parallel for num_threads(WRITE_THREADS)
    for (i=0;i<NUM_REDUCERS;i++) {
        omp_set_lock(&reducer_q_lck[i]);
        send_to_reducer(&hTable[i],i,reducerQ); // Need to parallelize this
        omp_unset_lock(&reducer_q_lck[i]);
    }
    printf("thread %d Done sending files to reducer\n",omp_get_thread_num());
    return;
}


int reducer(int rid, struct LLitem** rQ, omp_lock_t* lck, int* map_done) {
    printf("thread %d inside reducer %d\n",omp_get_num_threads(),rid);
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
    printf("thread %d done reducing\n",omp_get_num_threads());
    writer(&internal_reducerQ,rid);
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

int main() {
    int read_file_ptr=0;
    int reducer_id_track=0;
    int num_read_threads = READ_THREADS;
    int num_write_threads = WRITE_THREADS;
    int i,j;
    int done=0;
    int map_done=0;
    struct Q* reader_Q = InitQ(READER_Q_SIZE);
    omp_lock_t read_file_lck,reader_q_lck,reducer_id_lck;
    omp_lock_t reducer_q_lck[NUM_REDUCERS];
    omp_init_lock(&read_file_lck);
    omp_init_lock(&reader_q_lck);
    omp_init_lock(&reducer_id_lck);
    struct LLitem* hTable[NUM_REDUCERS];
    struct LLitem* rQ[NUM_REDUCERS];
    for (i=0;i<NUM_REDUCERS;i++) {
        omp_init_lock(&reducer_q_lck[i]);
        hTable[i]=NULL;
        rQ[i]=NULL;
    }
    printf("Doing Mapreduce in openmp using %d readers, %d mappers and %d reducers\n",READ_THREADS,MAP_THREADS,NUM_REDUCERS);
    double elapsed_time = -omp_get_wtime();
    #pragma omp parallel
    {
        #pragma omp single
        {
            for(i=0;i<READ_THREADS;i++) {
                #pragma omp task shared(read_file_ptr,done) // reader thread
                {
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
                    mapper(reader_Q,&done,num_read_threads,rQ,&reader_q_lck,reducer_q_lck);
                    map_done++;
                    printf("thread %d is done mapping, map_done=%d\n",omp_get_thread_num(),map_done);
                }
            }

            for(i=0;i<NUM_REDUCERS;i++) {
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
    printf("Finished Mapreduce in %.2lf seconds\n",elapsed_time);
}