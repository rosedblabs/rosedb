module github.com/rosedblabs/rosedb/v2

go 1.19

replace github.com/rosedblabs/wal => github.com/izouxv/wal v0.0.9

require (
	github.com/google/btree v1.1.2
	github.com/robfig/cron/v3 v3.0.0
	github.com/rosedblabs/wal v1.3.6-0.20230924022528-3202245af020
	github.com/spf13/afero v1.11.0
	github.com/valyala/bytebufferpool v1.0.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.2 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/bwmarrin/snowflake v0.3.0
	github.com/gofrs/flock v0.8.1
	github.com/stretchr/testify v1.9.0
)
