export DISABLE_CLOUD_LOGGING=1

test:
	go run .\cmd\stat batch -ct -dt -s test -v batch_test.mmd
prod:
	go run .\cmd\stat batch -s stats -v batch_prod.mmd

