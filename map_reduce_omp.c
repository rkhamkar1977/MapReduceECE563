#include <stdio.h>
#include <stdlib.h>
#include <omp.h>
#include <string.h>

#define LINE_LENGTH 256
#define WORD_LENGTH 20

typedef struct Q {
    int size;
    int pos;
    char** QHead;
} Q;

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
        //printf("Putting %s into the Work queue\n",buf);
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
        hash=hash^a[i];
        tmp=hash;
        hash=hash<<4 | tmp>>4;
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

void mapper(struct Q* W) {
    printf("In Mapper\n");
    int returnVal;
    char* buf = (char*) malloc(LINE_LENGTH*sizeof(char));
    char* word = (char*) malloc(WORD_LENGTH*sizeof(char));
    while(buf!=NULL) {
        buf=getWork(W);
        printf("Pulled string %s from the buffer\n",buf);
        do {
            returnVal=getWord(&buf,word);
            printf("%s\n",word);
            printf("Word Hash value is %d\n",hashFunc(word));
        } while(returnVal);
    }
    return;
}

int main() {
    struct Q* WorkQ = InitQ(1);
    reader(WorkQ);
    printf("Out of the reader\n");
    mapper(WorkQ);
    return 0;
}