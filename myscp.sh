#!/bin/sh

tm=`date +'%s'`

expect<<!!!
set SERVERS { "192.168.1.214" }
set timeout 10
foreach x \$SERVERS {
	puts \$x;
	spawn scp -o GSSAPIAuthentication=no -r yesmywine.com.item.log root@\$x:/home/system/opt/esp/queue/other/item.$tm.log
	expect {
	    "*yes/no" { send "yes\r"; exp_continue; }
	    "*password:" { send "super@BeHe@2015_Ssp\r" }
	}
	expect eof
}
!!!
