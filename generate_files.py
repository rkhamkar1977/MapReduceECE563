read_f = open('2.txt','r')
num_files = 20
lines = read_f.readlines()
read_f.close()
j=0
lines_in_a_file = int(len(lines)/num_files)
for i in range(0,num_files):
    write_f = open('file_chunks/'+str(i)+'.txt','w')
    ul = (i+1)*lines_in_a_file if i!=num_files-1 else len(lines)
    while j<ul:
        write_f.write(lines[j])
        j+=1
    write_f.close()