#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <string.h>

#define LINE_LENGTH 256
#define WORD_LENGTH 20
#define NUM_REDUCERS 8

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

// void** InitTable(int n) {
//     void* h;
//     int i;
//     printf("Inside InitTable1\n");
//     for (i=0;i<n;i++) {
//         //h[i] = (LLitem*) malloc(sizeof(LLitem));
//         h[i] = NULL;
//     }
//     return h;
// }

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

void reader(struct Q* W) {
    printf("In reader\n");
    char* fileName="2.txt";
    int returnVal=1;
    FILE* file = fopen(fileName,"r");
    char* buf = (char*) malloc(LINE_LENGTH*sizeof(char));
    while (fgets(buf,LINE_LENGTH,file) && returnVal) {
        if (buf[0]!='\n') {
        //printf("Putting %s into the Work queue\n",buf);
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

int putWorkRQ (struct reducerQ* r, struct keyData* k) {
    if (r->pos < r->size) {
        r->pos++;
        (*(r->k + r->pos))->cnt = k->cnt;
        memcpy((*(r->k + r->pos))->word,k->word,(strlen(k->word)+1)*sizeof(char));
        return 1;
    } else {
        printf("Trying to push into full ReducerQ\n");
        return 0;
    }
}

struct keyData* getWorkRQ (struct reducerQ* r) {
    struct keyData* k = (struct keyData*) malloc(sizeof(struct keyData));
    if (r->pos > -1) {
        k->cnt = (*(r->k + r->pos))->cnt;
        memcpy(k->word,(*(r->k + r->pos))->word,(strlen((*(r->k + r->pos))->word)+1)*sizeof(char));
        return k;
    } else {
        printf("Trying to pull from empty reducerQ\n");
        return NULL;
    }
}

void insertToReducerQ (struct LLitem** h, int index, struct Q** reducerQ) {
    struct keyData* k;
    while (k = pullFromHashTable(h)) {
        int returnVal = putWorkWQ(reducerQ+index,k->word);
    }
    return;
}

void reducer (struct Q** reducerQ, int index) {
    return;
}

int main() {
    struct Q* WorkQ = InitQ(50);
    struct Q* ReducerQ[NUM_REDUCERS];
    int i;
    for (i=0;i<NUM_REDUCERS;i++) {
        ReducerQ[i] = InitQ(100);
    }
    reader(WorkQ);
    //printf("Out of the reader\n");

    //Testing Hash table insertion mechanism
    // char* l = "The Project Gutenberg EBook of Master and Man, by the man leo, Leo The Tolstoy ***";
    // char* w = (char*) malloc(sizeof(char)*WORD_LENGTH);
    // printf("DEBUG1\n");
    // struct LLitem* hTable[NUM_REDUCERS];
    // for (int i=0;i<NUM_REDUCERS;i++) {
    //     hTable[i]=NULL;
    // }
    // printTable(hTable,NUM_REDUCERS);
    // printf("DEBUG2\n");
    // int returnVal;
    // int hIndex;
    // do {
    //     returnVal=getWord(&l,w);
    //     normalizeWord(w);
    //     if (!strlen(w)) {continue;}
    //     printf("%s\n",w);
    //     hIndex=hashFunc(w)%NUM_REDUCERS;
    //     printf("Pushing %s into table (Hash value %d)\n",w, hIndex);
    //     // push2table(&hTable[hIndex],w);
    //     insert(&hTable[hIndex],w);
    //     //printTable(hTable,NUM_REDUCERS);
    // } while(returnVal);
    // printTable(hTable,NUM_REDUCERS);
    //End of Hash table testing
    mapper(WorkQ);
    return 0;
}