#!/bin/sh

# dataset
datasets="books"
# datasets="books fb osm"

# Read/Insert ratios
ratios="0.2:0.8"
# ratios="1:0 0:1 0.5:0.5"

# # sbuck_sizes
sbuck_sizes="8"

# dbuck_sizes
dbuck_sizes="16384"

#indexes
# indexes="alex lipp finedex pgm"
indexes="alex"

# # Iterate the string array using for loop
for ratio in $ratios; do
    for dataset in $datasets; do
        # bli
        for sbuck_size in $sbuck_sizes; do
            for dbuck_size in $dbuck_sizes; do
                    command="numactl --cpunodebind=0 --membind=0  ./microbench --keys_file=../datasets/${dataset} --keys_file_type=binary --read=${ratio%:*} --insert=${ratio#*:} --operations_num=100000000 --table_size=200000000 --init_table_ratio=0.5 --thread_num=1 --index=buckindex --memory --bli_sbuck_size=${sbuck_size} --bli_dbuck_size=${dbuck_size} --bli_initial_filled_ratio=0.6  --error_bound=4.8"
                    echo "command is $command"
                    $command
                    sleep 1
            done
        done

        # others
        for index in $indexes; do
            command="numactl --cpunodebind=0 --membind=0 ./microbench --keys_file=../datasets/${dataset} --keys_file_type=binary --read=${ratio%:*} --insert=${ratio#*:} --operations_num=100000000 --table_size=200000000 --init_table_ratio=0.5 --thread_num=1 --index=${index} --memory"
            echo "command is $command"
            $command
            sleep 1
        done
    done
done
