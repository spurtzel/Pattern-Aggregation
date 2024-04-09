for j in 0 1 2 3 4 
do
    nohup python sender.py $j > "output_$j.txt" 2>&1 &
done
