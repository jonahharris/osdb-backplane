#!/bin/csh
#
# USAGE:
#
# ~/rdbms/tests/words.csh | ( time dsql -q test )

echo "BEGIN;"

cat << EOF
drop table test.words;
create schema test;
create table test.words ( key varchar primary key, data varchar );
EOF

set count = 1
set sq = "'"
foreach word ( `cat /usr/share/dict/words` )
    echo "INSERT INTO test.words ( key, data ) VALUES ( $sq$word$sq, $sq$count$sq );"
    @ count = $count + 1
    @ every = $count % 1000
    if ( $every == 0 ) then
	echo "COMMIT;"
	echo "BEGIN;"
	echo "SELECT * FROM test.words WHERE key = $sq$word$sq;"
    endif
end

echo "COMMIT;"
