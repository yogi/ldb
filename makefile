
#SERVER_DIR=/var/www/host/server

wrk-restart:
	ssh ubuntu@${TML_LOADGEN} 'cd bigo && (/usr/bin/pkill -f wrk || true) && ls'

#.PHONY: wrk-restart