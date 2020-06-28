num_threads=24
#file_sig=${2}
start_file="/scratch/graph_data/${1}/snap.txt.startv3"
stream_file_start="/scratch/graph_data/${1}/snap.txt"
stream_file="${stream_file_start}.changebatch.0.txt"
command_start="cargo run --release --example bfsgi"
# to change whether or not to print output 
insp="inspect"
echo "1 batch of 1k"
${command_start} ${start_file} ${stream_file} 1000 1 ${insp} -w$num_threads
echo "10 batch of 100"
${command_start} ${start_file} ${stream_file} 100 10 ${insp} -w$num_threads
echo "100 batch of 10"
${command_start} ${start_file} ${stream_file} 10 100 ${insp} -w$num_threads
echo "1000 batch of 1"
${command_start} ${start_file} ${stream_file} 1 1000 ${insp} -w$num_threads
stream_file="${stream_file_start}.100k-add.0"
echo "100 batch of 1000"
${command_start} ${start_file} ${stream_file} 1000 100 ${insp} -w$num_threads
echo "10 batch of 10000"
${command_start} ${start_file} ${stream_file} 10000 10 ${insp} -w$num_threads
echo "1 batch of 100000"
${command_start} ${start_file} ${stream_file} 100000 1 ${insp} -w$num_threads
stream_file="${stream_file_start}.100k-mixs.0"
echo "100 batch of 1000 mixed"
${command_start} ${start_file} ${stream_file} 1000 100 ${insp} -w$num_threads
echo "10 batch of 10000 mixed"
${command_start} ${start_file} ${stream_file} 10000 10 ${insp} -w$num_threads
echo "1 batch of 100000 mixed"
${command_start} ${start_file} ${stream_file} 100000 1 ${insp} -w$num_threads 
stream_file="${stream_file_start}.1m-add.0"
echo "100 batch of 10000"
${command_start} ${start_file} ${stream_file} 10000 100 ${insp} -w$num_threads
echo "10 batch of 100000"
${command_start} ${start_file} ${stream_file} 100000 10 ${insp} -w$num_threads
echo "1 batch of 1000000"
${command_start} ${start_file} ${stream_file} 1000000 1 ${insp} -w$num_threads
stream_file="${stream_file_start}.1m-mixs.0"
echo "100 batch of 10000 mixed"
${command_start} ${start_file} ${stream_file} 10000 100 ${insp} -w$num_threads
echo "10 batch of 100000 mixed"
${command_start} ${start_file} ${stream_file} 100000 10 ${insp} -w$num_threads
echo "1 batch of 1000000 mixed"
${command_start} ${start_file} ${stream_file} 1000000 1 ${insp} -w$num_threads


