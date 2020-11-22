FROM debian:buster

RUN apt-get update && apt-get install -y python3 git make gcc python3-pip fuse redis bc strace vim procps
ADD ./ /src/redisfs
RUN pip3 install -r /src/redisfs/requirements.txt
RUN cd /src/ && \
    git clone https://git.code.sf.net/p/ntfs-3g/pjd-fstest ntfs-3g-pjd-fstest && \
    cd ntfs-3g-pjd-fstest && \
    sed -i 's/^CFLAGS+=-DHAS_ACL -lacl/#CFLAGS+=-DHAS_ACL -lacl/' ./Makefile && \
    make && \
    patch tests/chown/00.t < /src/redisfs/chown-dir-1-1.patch #https://github.com/libfuse/libfuse/issues/184
