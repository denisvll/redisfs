# redisfs
Master

[![Build Status](https://travis-ci.org/denisvll/redisfs.svg?branch=master)](https://travis-ci.org/denisvll/redisfs)


Dev

[![Build Status](https://travis-ci.org/denisvll/redisfs.svg?branch=dev)](https://travis-ci.org/denisvll/redisfs)

Quick start

```
docker build . -t redisfs
docker run --name redisfs -it --rm  -v $(pwd):/project --device /dev/fuse   --cap-add SYS_ADMIN  redisfs:latest
redis-server &
mkdir /redisfs
python3 /project/main.py /redisfs/

## Run tests
cd /redisfs/ && prove -r ../src/ntfs-3g-pjd-fstest/
```

run in foreground mode
`python3 /project/main.py /redisfs/ --foreground`
