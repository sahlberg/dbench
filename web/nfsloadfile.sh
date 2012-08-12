#!/bin/sh

TSHARK=tshark
ECHO=/bin/echo
DBDIR=/tmp/nldbd

extract_field() {
	$ECHO "$1" | sed -e "s/^.*$2 == //" -e "s/ .*$//"
}

extract_quoted_field() {
	$ECHO "$1" | sed -e "s/^.*$2 == \"//" -e "s/\" .*$//"
}


do_readdirplus() {
	if [ $2 != "1" ]; then
		echo $TIMESTAMP READDIRPLUS3 \"$3\" $4 >$DBDIR/$1
	else
		CMD=`cat $DBDIR/$1`
		# we only generate the operation if we saw the request
		if [ "${CMD}x" != "x" ]; then
			echo $CMD $5
		fi
	fi
}

do_write() {
	if [ $2 != "1" ]; then
		echo $TIMESTAMP WRITE3 \"$3\" $4 $5 $6 >$DBDIR/$1
	else
		CMD=`cat $DBDIR/$1`
		echo $CMD $7
	fi
}

do_read() {
	if [ $2 != "1" ]; then
		echo $TIMESTAMP READ3 \"$3\" $4 $5 >$DBDIR/$1
	else
		CMD=`cat $DBDIR/$1`
		echo $CMD $6
	fi
}

do_create() {
	if [ $2 != "1" ]; then
		echo $TIMESTAMP CREATE3 \"$3\" $4 >$DBDIR/$1
	else
		CMD=`cat $DBDIR/$1`
		echo $CMD $5
	fi
}

do_lookup() {
	if [ $2 != "1" ]; then
		echo $TIMESTAMP LOOKUP3 \"$3\" >$DBDIR/$1
	else
		CMD=`cat $DBDIR/$1`
		echo $CMD $4
	fi
}

do_getattr() {
	if [ $2 != "1" ]; then
		echo $TIMESTAMP GETATTR3 \"$3\" >$DBDIR/$1
	else
		CMD=`cat $DBDIR/$1`
		echo $CMD $4
	fi
}

do_fsinfo() {
	if [ $2 != "1" ]; then
		echo $TIMESTAMP FSINFO3 >$DBDIR/$1
	else
		CMD=`cat $DBDIR/$1`
		echo $CMD $3
	fi
}

do_access() {
	if [ $2 != "1" ]; then
		echo $TIMESTAMP ACCESS3 \"$3\" 0 0 >$DBDIR/$1
	else
		CMD=`cat $DBDIR/$1`
		echo $CMD $4
	fi
}




rm -rf $DBDIR
mkdir -p $DBDIR

$TSHARK -n -r $1 -R "nfs" \
	-o "nfs.file_name_snooping:TRUE" \
	-o "nfs.file_full_name_snooping:TRUE" \
	-o "nfs.fhandle_find_both_reqrep:TRUE" \
	-z "proto,colinfo,rpc.xid,rpc.xid" \
	-z "proto,colinfo,rpc.msgtyp,rpc.msgtyp" \
	-z "proto,colinfo,nfs.nfsstat3,nfs.nfsstat3" \
	-z "proto,colinfo,nfs.name,nfs.name" \
	-z "proto,colinfo,nfs.full_name,nfs.full_name" \
	-z "proto,colinfo,nfs.createmode,nfs.createmode" \
	-z "proto,colinfo,nfs.offset3,nfs.offset3" \
	-z "proto,colinfo,nfs.count3,nfs.count3" \
	-z "proto,colinfo,nfs.cookie3,nfs.cookie3" \
	-z "proto,colinfo,nfs.write.stable,nfs.write.stable" \
	-z "proto,colinfo,nfs.procedure_v3,nfs.procedure_v3" \
	-z "proto,colinfo,frame.time_relative,frame.time_relative" \
| while read PACKET; do

TIMESTAMP=`extract_field "$PACKET" "frame.time_relative"`

#echo
#echo 
#echo packet:$PACKET

case "$PACKET" in
	# READDIRPLUS
	*"nfs.procedure_v3 == 17"*)
		XID=`extract_field "$PACKET" "rpc.xid"`
		MSGTYP=`extract_field "$PACKET" "rpc.msgtyp"`
		STATUS=`extract_field "$PACKET" "nfs.nfsstat3" | awk '{ printf "0x%08x", $1 }'`
		FULLNAME=`extract_quoted_field "$PACKET" "nfs.full_name"`
		COOKIE=`extract_field "$PACKET" "nfs.cookie3"`
		do_readdirplus "$XID" "$MSGTYP" "$FULLNAME" "$COOKIE" "$STATUS"
		
		;;
	# READ
	*"nfs.procedure_v3 == 6"*)
		XID=`extract_field "$PACKET" "rpc.xid"`
		MSGTYP=`extract_field "$PACKET" "rpc.msgtyp"`
		STATUS=`extract_field "$PACKET" "nfs.nfsstat3" | awk '{ printf "0x%08x", $1 }'`
		NAME=`extract_quoted_field "$PACKET" "nfs.name"`
		FULLNAME=`extract_quoted_field "$PACKET" "nfs.full_name"`
		OFFSET=`extract_field "$PACKET" "nfs.offset3"`
		COUNT=`extract_field "$PACKET" "nfs.count3"`
		do_read "$XID" "$MSGTYP" "$FULLNAME" "$OFFSET" "$COUNT" "$STATUS"
		
		;;
	# WRITE
	*"nfs.procedure_v3 == 7"*)
		XID=`extract_field "$PACKET" "rpc.xid"`
		MSGTYP=`extract_field "$PACKET" "rpc.msgtyp"`
		STATUS=`extract_field "$PACKET" "nfs.nfsstat3" | awk '{ printf "0x%08x", $1 }'`
		NAME=`extract_quoted_field "$PACKET" "nfs.name"`
		FULLNAME=`extract_quoted_field "$PACKET" "nfs.full_name"`
		OFFSET=`extract_field "$PACKET" "nfs.offset3"`
		COUNT=`extract_field "$PACKET" "nfs.count3"`
		STABLE=`extract_field "$PACKET" "nfs.write.stable"`
		do_write "$XID" "$MSGTYP" "$FULLNAME" "$OFFSET" "$COUNT" "$STABLE" "$STATUS"
		
		;;
	# CREATE
	*"nfs.procedure_v3 == 8"*)
		XID=`extract_field "$PACKET" "rpc.xid"`
		MSGTYP=`extract_field "$PACKET" "rpc.msgtyp"`
		STATUS=`extract_field "$PACKET" "nfs.nfsstat3" | awk '{ printf "0x%08x", $1 }'`
		NAME=`extract_quoted_field "$PACKET" "nfs.name"`
		FULLNAME=`extract_quoted_field "$PACKET" "nfs.full_name"`
		MODE=`extract_field "$PACKET" "nfs.createmode"`
		do_create "$XID" "$MSGTYP" "$FULLNAME/$NAME" "$MODE" "$STATUS"
		
		;;
	# LOOKUP
	*"nfs.procedure_v3 == 3"*)
		XID=`extract_field "$PACKET" "rpc.xid"`
		MSGTYP=`extract_field "$PACKET" "rpc.msgtyp"`
		STATUS=`extract_field "$PACKET" "nfs.nfsstat3" | awk '{ printf "0x%08x", $1 }'`
		NAME=`extract_quoted_field "$PACKET" "nfs.name"`
		FULLNAME=`extract_quoted_field "$PACKET" "nfs.full_name"`
		do_lookup "$XID" "$MSGTYP" "$FULLNAME/$NAME" "$STATUS"
		
		;;
	# FSINFO
	*"nfs.procedure_v3 == 19 "*)
		XID=`extract_field "$PACKET" "rpc.xid"`
		MSGTYP=`extract_field "$PACKET" "rpc.msgtyp"`
		STATUS=`extract_field "$PACKET" "nfs.nfsstat3" | awk '{ printf "0x%08x", $1 }'`
		do_fsinfo "$XID" "$MSGTYP" "$STATUS"
		;;
	# GETATTR
	*"nfs.procedure_v3 == 1 "*)
		XID=`extract_field "$PACKET" "rpc.xid"`
		MSGTYP=`extract_field "$PACKET" "rpc.msgtyp"`
		STATUS=`extract_field "$PACKET" "nfs.nfsstat3" | awk '{ printf "0x%08x", $1 }'`
		FULLNAME=`extract_quoted_field "$PACKET" "nfs.full_name"`
		do_getattr "$XID" "$MSGTYP" "$FULLNAME" "$STATUS"
		;;
	# ACCESS
	*"nfs.procedure_v3 == 4"*)
		XID=`extract_field "$PACKET" "rpc.xid"`
		MSGTYP=`extract_field "$PACKET" "rpc.msgtyp"`
		STATUS=`extract_field "$PACKET" "nfs.nfsstat3" | awk '{ printf "0x%08x", $1 }'`
		FULLNAME=`extract_quoted_field "$PACKET" "nfs.full_name"`
		do_access "$XID" "$MSGTYP" "$FULLNAME" "$STATUS"
		;;
	*)
		echo "XXX unknown packet $PACKET"
		;;
esac

done

rm -rf $DBDIR
