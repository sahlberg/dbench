#!/bin/sh

TSHARK=./tshark
ECHO=/bin/echo

# Process the tempfile and convert it into a NBENCH file

extract_field() {
	$ECHO "$1" | sed -n -e "s/^.*$2 == //; T; s/ .*$//; p"
}

extract_name() {
	$ECHO "$1" | sed -n -e "s/^.*$2 == \"\([^\"]*\)\".*$/\"\/clients\/client1\1\"/; T; p"
}

write_andx_pkt() {
	TIMESTAMP=$1
	PACKET=$2

	FID=`extract_field "$PACKET" "smb.fid"`
	OFFSET=`extract_field "$PACKET" "smb.file.rw.offset"`
	LENGTH=`extract_field "$PACKET" "smb.file.rw.length"`

	COUNT_LOW=`extract_field "$PACKET" "smb.count_low"`
	COUNT_HIGH=`extract_field "$PACKET" "smb.count_high"`
	if [ $COUNT_LOW ] && [ $COUNT_HIGH ]; then
	    COUNT=$(( $COUNT_LOW + ($COUNT_HIGH << 16) ))
	else
	    COUNT=
	fi

	STATUS=`extract_field "$PACKET" "smb.nt_status"`

	if [ $FID ] && [ $OFFSET ] && [ $LENGTH ] && [ $COUNT ] && [ $STATUS ]; then
		$ECHO $TIMESTAMP "WriteX" $FID $OFFSET $LENGTH $COUNT $STATUS
	else
		$ECHO "Incomplete WriteX: TIMESTAMP: $TIMESTAMP, "	\
					  "FID: $FID, "			\
					  "OFFSET: $OFFSET, "		\
					  "LENGTH: $LENGTH, "		\
					  "COUNT: $COUNT, "		\
					  "STATUS: $STATUS" 1>&2
	fi
}

close_pkt() {
	TIMESTAMP=$1
	PACKET=$2

	FID=`extract_field "$PACKET" "smb.fid"`
	STATUS=`extract_field "$PACKET" "smb.nt_status"`

	if [ $FID ]; then
	    $ECHO $TIMESTAMP "Close" $FID $STATUS
	fi
}

ntcreate_andx_pkt() {
	TIMESTAMP=$1
	PACKET=$2

	NAME=`extract_name "$PACKET" "smb.file"`
	FID=`extract_field "$PACKET" "smb.fid"`
	OPTIONS=`extract_field "$PACKET" "smb.create_options"`
	DISPOSITION=`extract_field "$PACKET" "smb.create.disposition"`
	STATUS=`extract_field "$PACKET" "smb.nt_status"`

	if [ $NAME ] && [ $OPTIONS ] && [ $DISPOSITION ] && [ $FID ] && [ $STATUS ]; then
		$ECHO "$TIMESTAMP NTCreateX $NAME $OPTIONS $DISPOSITION $FID $STATUS"
	else
		$ECHO "Incomplete NTCreateX: TIMESTAMP: $TIMESTAMP, "	   \
					    "NAME: $NAME, "		   \
					    "OPTIONS: $OPTIONS, "	   \
					    "DISPOSITION: $DISPOSITION, " \
					    "FID: $FID, "		   \
					    "STATUS: $STATUS" 1>&2
	fi
}

read_andx_pkt() {
	TIMESTAMP=$1
	PACKET=$2

	FID=`extract_field "$PACKET" "smb.fid"`
	OFFSET=`extract_field "$PACKET" "smb.file.rw.offset"`
	LENGTH=`extract_field "$PACKET" "smb.file.rw.length"`
	COUNT=`extract_field "$PACKET" "smb.data_len_low"`
	STATUS=`extract_field "$PACKET" "smb.nt_status"`

	if [ $FID ] && [ $OFFSET ] && [ $LENGTH ] && [ $COUNT ] && [ $STATUS ]; then
		$ECHO "$TIMESTAMP ReadX $FID $OFFSET $LENGTH $COUNT $STATUS"
	else
		$ECHO "Incomplete ReadX: TIMESTAMP: $TIMESTAMP, "	\
					"FID: $FID, "			\
					"OFFSET: $OFFSET, "		\
					"LENGTH: $LENGTH, "		\
					"COUNT: $COUNT, "		\
					"STATUS: $STATUS" 1>&2
	fi
}

trans2_qfi_pkt() {
	FID=`extract_field "$1" "smb.fid"`
	LEVEL=`extract_field "$1" "smb.qpi_loi"`
	STATUS=`extract_field "$1" "smb.nt_status"`

	$ECHO "QUERY_FILE_INFORMATION" $FID $LEVEL $STATUS
}

trans2_qfsi_pkt() {
	FID=`extract_field "$1" "smb.fid"`
	LEVEL=`extract_field "$1" "smb.qfsi_loi"`
	STATUS=`extract_field "$1" "smb.nt_status"`

	$ECHO "QUERY_FS_INFORMATION" $LEVEL $STATUS
}

trans2_qpi_pkt() {
	NAME=`extract_name "$1" "smb.file"`
	LEVEL=`extract_field "$1" "smb.qpi_loi"`
	STATUS=`extract_field "$1" "smb.nt_status"`

	$ECHO "QUERY_PATH_INFORMATION" "$NAME" $LEVEL $STATUS
}

trans2_ff2_pkt() {
	NAME=`extract_name "$1" "smb.search_pattern"`
	LEVEL=`extract_field "$1" "smb.ff2_loi"`
	STATUS=`extract_field "$1" "smb.nt_status"`

	$ECHO "FIND_FIRST" "$NAME" $LEVEL 1000 0 $STATUS
}

delete_pkt() {
	NAME=`extract_name "$1" "smb.file"`
	STATUS=`extract_field "$1" "smb.nt_status"`

       $ECHO "Unlink" "$NAME" 0x16 $STATUS
}

createdir_pkt() {
	NAME=`extract_name "$1" "smb.file"`
	STATUS=`extract_field "$1" "smb.nt_status"`

	$ECHO "Mkdir" "$NAME" $STATUS
}

deletedir_pkt() {
	NAME=`extract_name "$1" "smb.file"`
	STATUS=`extract_field "$1" "smb.nt_status"`

	$ECHO "Rmdir" "$NAME" $STATUS
}

checkdir_pkt() {
	NAME=`extract_name "$1" "smb.file"`
	STATUS=`extract_field "$1" "smb.nt_status"`

	$ECHO "Checkdir" "$NAME" $STATUS
}

trans2_sfi_pkt() {
	TIMESTAMP=$1
	PACKET=$2

# !!! TEMPORAL !!!
	return;

	FID=`extract_field "$PACKET" "smb.fid"`
	LEVEL=`extract_field "$PACKET" "smb.sfi_loi"`
	STATUS=`extract_field "$PACKET" "smb.nt_status"`

	$ECHO $PACKET 1>&2
	$ECHO "SET_FILE_INFORMATION: FID: $FID, LEVEL: $LEVEL, STATUS: $STATUS" 1>&2

	$ECHO "$TIMESTAMP SET_FILE_INFORMATION" $FID $LEVEL $STATUS
}

lockandx_pkt() {
	FID=`extract_field "$1" "smb.fid"`
	OFFSET=`extract_field "$1" "smb.lock.offset"`
	LENGTH=`extract_field "$1" "smb.lock.length"`
	STATUS=`extract_field "$1" "smb.nt_status"`

	$ECHO "LockX" $FID $OFFSET $LENGTH $STATUS
}

unlockandx_pkt() {
	FID=`extract_field "$1" "smb.fid"`
	OFFSET=`extract_field "$1" "smb.lock.offset"`
	LENGTH=`extract_field "$1" "smb.lock.length"`
	STATUS=`extract_field "$1" "smb.nt_status"`

	$ECHO "UnlockX" $FID $OFFSET $LENGTH $STATUS
}


flush_pkt() {
	TIMESTAMP=$1
	PACKET=$2

	FID=`extract_field "$PACKET" "smb.fid"`
	STATUS=`extract_field "$PACKET" "smb.nt_status"`

	if [ $FID ] && [ $STATUS ]; then
	    $ECHO $TIMESTAMP "Flush" $FID $STATUS
	else
	    $ECHO "Incomplete Flush: TIMESTAMP: $TIMESTAMP, "	\
				    "FID: $FID, "		\
				    "STATUS: $STATUS" 1>&2
	fi
}

rename_pkt() {
	TIMESTAMP=$1
	PACKET=$2

	OLDNAME=`extract_name "$PACKET" "smb.old_file"`
	NEWNAME=`extract_name "$PACKET" "smb.file"`
	STATUS=`extract_field "$PACKET" "smb.nt_status"`

	if [ $OLDNAME ] && [ $NEWNAME ] && [ $STATUS ]; then
	    $ECHO $TIMESTAMP "Rename" $OLDNAME $NEWNAME $STATUS
	else
	    $ECHO "Incomplete Rename: TIMESTAMP: $TIMESTAMP, "	\
				     "OLDNAME: $OLDNAME, "	\
				     "NEWNAME: $NEWNAME, "	\
				     "STATUS: $STATUS" 1>&2
	fi
}

# This script takes a network capture file and generates an nbench load file
# 
# Currently supported SMB commands are
#   Close
#   CheckDirectory
#   CreateDirectory
#   Delete
#   DeleteDirectory
#   FIND_FIRST2
#   Flush
#   LockingAndX
#   NTCreateAndX
#   QueryFileInfo
#   QueryFSInfo
#   QueryPathInfo
#   ReadAndX
#   Rename
#   WriteAndX
#

$ECHO 0.000000000 Deltree \"/clients/client1\" 0x00000000
$ECHO 0.000000000 Mkdir \"/clients\" 0x00000000
$ECHO 0.000000000 Mkdir \"/clients/client1\" 0x00000000

$TSHARK -n -r $1 -R "smb.flags.response==1 and smb.cmd"	\
    -z "proto,colinfo,smb.locking.num_unlocks and smb.cmd==0x24,smb.locking.num_unlocks"		\
    -z "proto,colinfo,smb.locking.num_locks and smb.cmd==0x24,smb.locking.num_locks"		\
    -z "proto,colinfo,smb.pid and smb.cmd==0x24,smb.pid"		\
    -z "proto,colinfo,smb.lock.offset and smb.cmd==0x24,smb.lock.offset"		\
    -z "proto,colinfo,smb.lock.length and smb.cmd==0x24,smb.lock.length"		\
    -z "proto,colinfo,smb.pid and smb.cmd==0x2f,smb.pid"		\
    -z "proto,colinfo,smb.count_low and smb.cmd==0x2f,smb.count_low"		\
    -z "proto,colinfo,smb.count_high and smb.cmd==0x2f,smb.count_high"		\
    -z "proto,colinfo,smb.file.rw.offset and smb.cmd==0x2f,smb.file.rw.offset"		\
    -z "proto,colinfo,smb.file.rw.length and smb.cmd==0x2f,smb.file.rw.length"		\
    -z "proto,colinfo,smb.qpi_loi and smb.cmd==0x32,smb.qpi_loi"		\
    -z "proto,colinfo,smb.spi_loi and smb.cmd==0x32,smb.spi_loi"		\
    -z "proto,colinfo,smb.qfsi_loi and smb.cmd==0x32,smb.qfsi_loi"		\
    -z "proto,colinfo,smb.ff2_loi and smb.cmd==0x32,smb.ff2_loi"		\
    -z "proto,colinfo,smb.search_pattern and smb.cmd==0x32,smb.search_pattern"	\
    -z "proto,colinfo,smb.trans2.cmd and smb.cmd==0x32,smb.trans2.cmd"		\
    -z "proto,colinfo,smb.data_len_low and smb.cmd==0x2e,smb.data_len_low"	\
    -z "proto,colinfo,smb.create_flags and smb.cmd==0xa2,smb.create_flags"	\
    -z "proto,colinfo,smb.create_options and smb.cmd==0xa2,smb.create_options"	\
    -z "proto,colinfo,smb.create.disposition and smb.cmd==0xa2,smb.create.disposition"	\
    -z "proto,colinfo,smb.share_access and smb.cmd==0xa2,smb.share_access"	\
    -z "proto,colinfo,smb.access_mask and smb.cmd==0xa2,smb.access_mask"	\
    -z "proto,colinfo,smb.nt.function and smb.cmd==0xa0,smb.nt.function"	\
    -z "proto,colinfo,smb.file.rw.offset,smb.file.rw.offset"			\
    -z "proto,colinfo,smb.file.rw.length,smb.file.rw.length"			\
    -z "proto,colinfo,smb.file,smb.file"					\
    -z "proto,colinfo,smb.old_file,smb.old_file"					\
    -z "proto,colinfo,smb.fid,smb.fid"						\
    -z "proto,colinfo,smb.nt_status,smb.nt_status"				\
    -z "proto,colinfo,smb.cmd,smb.cmd" \
    -z "proto,colinfo,frame.time_relative,frame.time_relative" \
| while read PACKET; do

TIMESTAMP=`extract_field "$PACKET" "frame.time_relative"`
case "$PACKET" in

  # Close
  *"smb.cmd == 0x04"*)
	close_pkt "$TIMESTAMP" "$PACKET"
	;;

  # CheckDir
  *"smb.cmd == 0x10"*)
	$ECHO -n "$TIMESTAMP "
	checkdir_pkt "$PACKET"
	;;

  # CreateDir
  *"smb.cmd == 0x00"*)
	$ECHO -n "$TIMESTAMP "
	createdir_pkt "$PACKET"
	;;

  # Delete
  *"smb.cmd == 0x06"*)
	$ECHO -n "$TIMESTAMP "
	delete_pkt "$PACKET"
	;;

  # DeleteDir
  *"smb.cmd == 0x01"*)
	$ECHO -n "$TIMESTAMP "
	deletedir_pkt "$PACKET"
	;;

  # Flush
  *"smb.cmd == 0x05"*)
	flush_pkt "$TIMESTAMP" "$PACKET"
	;;

  # LockingAndX   Lock
  *"smb.cmd == 0x24"*"smb.locking.num_locks == 1"*"smb.locking.num_unlocks == 0"*)
	lockandx_pkt "$PACKET"
	;;

  # LockingAndX   Unlock
  *"smb.cmd == 0x24"*"smb.locking.num_locks == 0"*"smb.locking.num_unlocks == 1"*)
	unlockandx_pkt "$PACKET"
	;;

  # Logoff
  *"smb.cmd == 0x74"*)
	# just ignore any logoffs
	;;

  # NegotiateProtocol
  *"smb.cmd == 0x72"*)
	# just ignore any negotiate protocol
	;;

  # NTCreateAndX
  *"smb.cmd == 0xa2"*)
	ntcreate_andx_pkt "$TIMESTAMP" "$PACKET"
	;;

  # NT Trans ioctl
  *"smb.cmd == 0xa0"*"smb.nt.function == 2"*)
	# ignore
	;;

  # NT Trans NOTIFY 
  *"smb.cmd == 0xa0"*"smb.nt.function == 4"*)
	# ignore
	;;

  # Rename
  *"smb.cmd == 0x07"*)
	rename_pkt "$TIMESTAMP" "$PACKET"
	;;

  # SessionSetup
  *"smb.cmd == 0x73"*)
	# just ignore any sessionsetup
	;;

  # ProcessExit
  *"smb.cmd == 0x11"*)
	# just ignore any process exit
	;;

  # TreeConnect
  *"smb.cmd == 0x75"*)
	# just ignore any tree connect
	;;

  # TreeDisconnect
  *"smb.cmd == 0x71"*)
	# just ignore any tree disconnect
	;;

  # Trans
  *"smb.cmd == 0x25"*)
	# ignore
	;;

  # Trans2 FF2 
  *"smb.cmd == 0x32"*"smb.trans2.cmd == 0x0001"*)
	$ECHO -n "$TIMESTAMP "
	trans2_ff2_pkt "$PACKET"
	;;

  # Trans2 GetDfsReferral
  *"smb.cmd == 0x32"*"smb.trans2.cmd == 0x0010"*)
	# ignore for now
	;;

  # Trans2 QFI 
  *"smb.cmd == 0x32"*"smb.trans2.cmd == 0x0007"*)
	$ECHO -n "$TIMESTAMP "
	trans2_qfi_pkt "$PACKET"
	;;

  # Trans2 QFSI 
  *"smb.cmd == 0x32"*"smb.trans2.cmd == 0x0003"*)
	$ECHO -n "$TIMESTAMP "
	trans2_qfsi_pkt "$PACKET"
	;;

  # Trans2 QPI 
  *"smb.cmd == 0x32"*"smb.trans2.cmd == 0x0005"*)
	$ECHO -n "$TIMESTAMP "
	trans2_qpi_pkt "$PACKET"
	;;

  # Trans2 SFI
  *"smb.cmd == 0x32"*"smb.trans2.cmd == 0x0008"*)
	trans2_sfi_pkt "$TIMESTAMP" "$PACKET"
	;;

  # ReadAndX
  *"smb.cmd == 0x2e"*)
	read_andx_pkt "$TIMESTAMP" "$PACKET"
	;;

  # WriteAndX
  *"smb.cmd == 0x2f"*)
	write_andx_pkt "$TIMESTAMP" "$PACKET"
	;;

  *)
	$ECHO "Unknown command:$PACKET" 1>&2
	;;

esac
done

$ECHO 0.000000000 Deltree \"/clients/client1\" 0x00000000

