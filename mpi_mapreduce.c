#include <stdio.h>
#include <mpi.h>
#include <omp.h>
#include <string.h>


#define LINE_LENGTH 256
#define WORD_LENGTH 20
#define NUM_REDUCERS 8
#define NUM_FILE_CHUNKS 20
#define READER_Q_SIZE 600

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

void reader(struct Q* W, char* fileName) {
    printf("In reader\n");
    //char* fileName="2.txt";
    int returnVal=1;
    FILE* file = fopen(fileName,"r");
    char* buf = (char*) malloc(LINE_LENGTH*sizeof(char));
    while (fgets(buf,LINE_LENGTH,file) && returnVal) {
        if (buf[0]!='\n') {
        printf("Putting %s into the Work queue\n",buf);
        returnVal=putWorkWQ(W,buf);
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

void mapper(struct Q* W) {
    struct LLitem* hTable[NUM_REDUCERS];
    for (int i=0;i<NUM_REDUCERS;i++) {
        hTable[i]=NULL;
    }
    int hIndex;
    int returnVal;
    char* buf = (char*) malloc(LINE_LENGTH*sizeof(char));
    char* word = (char*) malloc(WORD_LENGTH*sizeof(char));
    while(1) {
        buf=getWorkWQ(W);
        if (buf!=NULL) {
            //printf("Pulled string %s from the buffer\n",buf);
            do {
                returnVal=getWord(&buf,word);
                normalizeWord(word);
                if (!strlen(word)) {continue;}
                //printf("%s\n",word);
                hIndex=hashFunc(word)%NUM_REDUCERS;
                //printf("Pushing %s into table (Hash value %d)\n",word, hIndex);
                insert(&hTable[hIndex],word);
            } while(returnVal);
        } else {break;}
    } 
    //printf("Out of the loop\n");
    printTable(hTable,NUM_REDUCERS);
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

int readingDone(int* flag, int num_readers) {
    int i=0;
    int f=1;
    while(i<num_readers) {
        if (!(*(flag+i))) {
            f=0;
            break;
        }
    }
    return f;
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
    int pid;
    int numP;
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD,&numP);
    int num_readers = numP*1;
    MPI_Comm_rank(MPI_COMM_WORLD,&pid);
    int i;

    if (!pid) {
        int reader_file_ptr = 0;
        int reader_msg[2] = {0,0};
        int reader_pid;
        MPI_Request reader_req_for_work;
        MPI_Status reader_req_status;
        int flag[num_readers];
        for (i=0;i<num_readers;i++) {
            flag[i] = 0;
        }
        //MPI Master process messaging bit : 0 - reader asking for work, 1 -reducer asking for file
        //MPI Master messaging ettiquette - send 2 integers - 1st is your pid and the second is messaging bit - 0/1/2
        //MPI Process tag for getting and asking for work - 0
        MPI_Request node_init_done[numP];
        for (i=0;i<min(numP,NUM_FILE_CHUNKS);i++) {
            MPI_Isend(&reader_file_ptr,1,MPI_INT,i,0,MPI_COMM_WORLD,MPI_Reduce_scatter);
            reader_file_ptr++;
        }
        while (reader_file_ptr<NUM_FILE_CHUNKS) {
            MPI_Irecv(&reader_msg,2,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&reader_req_for_work);
            MPI_Wait(&reader_req_for_work,&reader_req_status);
            if (reader_msg[1] == 0) {
                MPI_Send(&reader_file_ptr,1,MPI_INT,reader_msg[0],0,MPI_COMM_WORLD);
                reader_file_ptr++;
            }
        }
        while (!readingDone(flag,num_readers)) {
            MPI_Irecv(&reader_msg,2,MPI_INT,MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&reader_req_for_work);
            MPI_Wait(&reader_req_for_work,&reader_req_status);
            if (reader_msg[1]==0) {
                MPI_Send(&read_done,1,MPI_INT,reader_msg[0],0,MPI_COMM_WORLD);
            } else if (reader_msg[1]==1) {

            }
        }
    } else {
        struct Q* reader_Q = InitQ(READER_Q_SIZE);
        int file_to_read;
        char* filename;
        int done=0;
        int send_msg[2] = {pid,0};
        MPI_Recv(&file_to_read,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        filename=getReaderFileName(file_to_read);
        do {
            reader(reader_Q,filename);
            MPI_Send(&send_msg,2,MPI_INT,0,0,MPI_COMM_WORLD);
            MPI_Recv(&file_to_read,1,MPI_INT,0,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
            if (file_to_read==read_done) {
                done=1;
            } else {
                filename = getReaderFileName(file_to_read);
                if (pid == 1) {printf("Got file %s to read from Master process\n",filename);}
            }
        } while (!done);
        mapper(reader_Q);
    }
}