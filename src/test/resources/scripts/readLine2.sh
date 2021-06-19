if [ $# -eq 0 ]; then
    echo "ERROR: incorrect argument specified: " "$@"
        echo "Usage:./readLine.sh <fileName> <no of diff queries>"
            exit 1
            fi
x=0
while read line
do
   ((x++))
   echo $line >> $x.txt
     [ $x -eq $2 ] && { x=0;}
     done < $1
