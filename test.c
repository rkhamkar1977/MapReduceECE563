#include <stdio.h>
#include <stdlib.h>

int main() {
    char** ptr;
    ptr = malloc(256*sizeof(char)*1000);
    ptr[0] = "Rahul";
    ptr[1] = "Dinesh";
    printf("%s \n",*(ptr));
    return 0;
}