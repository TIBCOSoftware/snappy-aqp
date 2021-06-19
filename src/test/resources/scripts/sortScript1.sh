set -x
if [ $# -eq 0 ]; then
    echo "ERROR: incorrect argument specified: " "$@"
        echo "Usage:./sortScript.sh <fileName>"
            exit 1
            fi
#sed -n -e '/Query/p' $1 >> OutPut.txt
#sed -n -e '/Query1/,/Query2/p' InputFile.out >> Query1.txt
#sed -n -e '/'$1'/,/'$2'/p' $3
awk '/'$1'/{flag=1} /'$2'/{flag=0} flag' $3 >> $4
#sed '/'$2'/d' $3 >> $4

#delete unwanted entries.
echo " The paramter is "
echo $2
sed "s/'$1'/\n/" $4 >> $5
#sed '/'$2'/\n' $4 >> $5
