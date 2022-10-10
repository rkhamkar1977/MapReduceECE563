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

typedef struct LLitem {
    int cnt;
    char* word;
    struct LLitem* nextptr;
} LLitem;

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

char* getWork (struct Q* W) {
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

int putWork (struct Q* W, char* buf) {
    if (W->pos < W->size-1) {
        W->pos++;
        printf("Inside putwork, workpos is %d\n",W->pos);
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
        printf("Putting %s into the Work queue\n",buf);
        returnVal=putWork(W,buf);
        }
    }
    fclose(file);
    return;
}

int hashFunc(char* a) {
    int i,hash;
    hash=0;
    int tmp=0;
    for (i=0;i<strlen(a);i++) {
        if ((a[i]>=65 && a[i]<=90) || (a[i]>=97 && a[i]<=122)) {
            hash=hash^a[i];
            tmp=hash;
            hash=hash<<4 | tmp>>4;
        }
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

// int push2table(void** i,char* w) {
//     int flag=0;
//     LLitem* p;
//     if ((*i)==NULL) {
//         LLitem* ll_element = (LLitem*) malloc(sizeof(LLitem));
//         (*i)=ll_element;
//         ll_element->word = (char*) malloc(WORD_LENGTH*sizeof(char));
//         ll_element->nextptr=NULL;
//         ll_element->cnt=1;
//         memcpy(ll_element->word,w,(strlen(w)+1)*sizeof(char));
//         return 1;
//     } else {
//         p = (LLitem*) *i;
//         while(p->nextptr!=NULL) {
//             if (strcmp(p->word,w)==0) {
//                 p->cnt++;
//                 flag=1;
//                 break;
//             }
//             p = p->nextptr;
//         }
//         if (!flag) {
//             LLitem* ll_element = (LLitem*) malloc(sizeof(LLitem));
//             ll_element->nextptr=(LLitem*) *i;
//             *i=ll_element;
//             ll_element->cnt=1;
//             ll_element->word = (char*) malloc(WORD_LENGTH*sizeof(char));
//             memcpy(ll_element->word,w,(strlen(w)+1)*sizeof(char));
//             return 1;
//         } else {return 1;}
//         return 0;
//     }
// }

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
    int cnt=0;
    for (i=0;i<n-j;i++) {
        w[i-cnt]=w[i];
        if (flag[i]) {cnt++;}
    }
    w[i]=0;
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
        printf("Writing word %s to %p\n",elem->word,&(elem->word));
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
        printf("Writing word %s to %p\n",elem->word,&(elem->word));
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
        buf=getWork(W);
        if (buf!=NULL) {
            printf("Pulled string %s from the buffer\n",buf);
            do {
                returnVal=getWord(&buf,word);
                normalizeWord(word);
                if (!strlen(word)) {continue;}
                printf("%s\n",word);
                hIndex=hashFunc(word)%NUM_REDUCERS;
                printf("Pushing %s into table (Hash value %d)\n",word, hIndex);
                insert(&hTable[hIndex],word);
            } while(returnVal);
        } else {break;}
    } 
    printf("Out of the loop\n");
    printTable(hTable,NUM_REDUCERS);
    return;
}

int main() {
    struct Q* WorkQ = InitQ(10);
    reader(WorkQ);
    printf("Out of the reader\n");

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