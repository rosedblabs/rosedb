version=1.0
flags=-X main.gitHash=`git show -s --format=%H`\
	  -X main.buildTime=`date -u '+%Y-%m-%d'`\
	  -X main.rosedbVersion=$(version)

build:
	go mod download
	go build -ldflags "$(flags)" \
			 -o rosedb-server ./cmd/