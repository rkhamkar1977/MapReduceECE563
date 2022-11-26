#!/apps/spack/scholar/fall20/apps/anaconda/5.1.0-py36-gcc-4.8.5-5cfpkzk/bin/python

import os
import subprocess
import argparse
import glob


def find_dir(path):
    # find file chunks here
    all_subdirs = {d.split("_")[-1]: d for d in os.listdir(path) if os.path.isdir(d) and 'file_chunks_' in d}
    return all_subdirs


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='run MapReduce')
    parser.add_argument('--sbatch', dest="sbatch", action='store_true')

    args = parser.parse_args()
    run = (args.sbatch)

    dirs = find_dir(os.getcwd())

    print(dirs)

    num_process = [str(2 ** i) for i in range(1, 5)]
    # num_threads = [str(2 ** i) for i in range(0, 5)]
    num_threads = ["10"]
    read_threads = [str(2 ** i) for i in range(3, 5)]
    write_threads = [str(2 ** i) for i in range(3, 5)]
    map_threads = [str(2 ** i) for i in range(0, 3)]
    # reduce_threads = [str(2 ** i) for i in range(3, 5)]
    reduce_threads = ["10"]
    print("NUM_PROCESS:", num_process)
    print("NUM_CHUNKS:", dirs.keys())
    print("NUM_THREADS:", num_threads)
    print("READ_THREADS:", read_threads)
    print("WRITE_THREADS:", write_threads)
    print("MAP_THREADS:", map_threads)
    print("REDUCE_THREADS:", reduce_threads)

    if run:
        for file in glob.glob("*.sub"):
            # run for new or failed jobs only
            filename = os.path.splitext(file)[0].replace("mpi_mm_", "output_")
            if os.path.isfile(filename):
                print("{} exists already".format(filename))
            else:
                print(" ".join(["sbatch", "--exclusive", file]))


                subprocess.run(["sbatch", "--exclusive", file])
    else:
        # read mpi code
        with open('mpi_mapreduce.c', 'r') as file:
            c_data = file.read().splitlines()

        count = 1
        os.makedirs("codes", exist_ok=True)

        for chunk in dirs.keys():
            for procs in num_process:
                for thread in num_threads:
                    for rt in read_threads:
                        for wt in write_threads:
                            for mt in map_threads:
                                for redt in reduce_threads:
                                    if int(procs) * int(rt) <= int(chunk) and int(rt) == int(wt) and int(rt) + int(mt) <= int(thread):
                                        print("{}: mpi_mapreduce_numP{}_numChunk{}_numThread{}_rT{}_wT{}_mT{}_redT{}.c".format(count, procs, chunk, thread, rt, wt, mt, redt))
                                        count += 1
                                        c_file = "codes/mpi_mapreduce_numP{}_numChunk{}_numThread{}_rT{}_wT{}_mT{}_redT{}.c".format(procs, chunk, thread, rt, wt, mt, redt)
                                        exe_file = "mpi_mapreduce_numP{}_numChunk{}_numThread{}_rT{}_wT{}_mT{}_redT{}.exe".format(procs, chunk, thread, rt, wt, mt, redt)
                                        sub_file = "mpi_mm_numP{}_numChunk{}_numThread{}_rT{}_wT{}_mT{}_redT{}.sub".format(procs, chunk, thread, rt, wt, mt, redt)
                                        output_file = "output_numP{}_numChunk{}_numThread{}_rT{}_wT{}_mT{}_redT{}".format(procs, chunk, thread, rt, wt, mt, redt)
                                        with open(c_file, 'w') as file:
                                            for line in c_data:
                                                if line.startswith("#define NUM_PROCESS"):
                                                    file.write(line.replace(line.split()[-1], procs))
                                                    file.write("\n")
                                                elif line.startswith("#define NUM_FILE_CHUNKS"):
                                                    file.write(line.replace(line.split()[-1], chunk))
                                                    file.write("\n")
                                                elif line.startswith("#define READ_THREADS"):
                                                    file.write(line.replace(line.split()[-1], rt))
                                                    file.write("\n")
                                                elif line.startswith("#define WRITE_THREADS"):
                                                    file.write(line.replace(line.split()[-1], wt))
                                                    file.write("\n")
                                                elif line.startswith("#define MAP_THREADS"):
                                                    file.write(line.replace(line.split()[-1], mt))
                                                    file.write("\n")
                                                elif line.startswith("#define REDUCE_THREADS"):
                                                    file.write(line.replace(line.split()[-1], redt))
                                                    file.write("\n")
                                                else:
                                                    file.write(line)
                                                    file.write("\n")

                                        print(" ".join(["mpicc", "-qopenmp", "-std=gnu99", c_file, "-o", "./{}/{}".format(dirs[chunk], exe_file)]))
                                        subprocess.run(["mpicc", "-qopenmp", "-std=gnu99", c_file, "-o","./{}/{}".format(dirs[chunk], exe_file)])

                                        with open("./{}/{}".format(dirs[chunk], sub_file), 'w') as file:
                                            file.write("#!/bin/bash\n")
                                            file.write("# FILENAME:  {}\n" .format(sub_file))
                                            file.write("\n")
                                            file.write("#SBATCH --nodes={}\n".format(procs))
                                            file.write("#SBATCH --ntasks-per-node=1\n")
                                            file.write("#SBATCH --time=00:01:00\n")
                                            file.write("#SBATCH --cpus-per-task={}\n".format(thread))
                                            file.write("#SBATCH -A scholar\n")
                                            file.write("export OMP_NUM_THREADS={}\n".format(thread))
                                            file.write("srun --mpi=pmi2 -n {} ./{}\n".format(procs, exe_file))
                                            file.write("cat {}_* > {}\n" .format(output_file, output_file))
                                            file.write("rm {}_*\n".format(output_file))
                                            file.write("\n")


