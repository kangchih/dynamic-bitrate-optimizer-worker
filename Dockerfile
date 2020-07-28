FROM python:3.7-alpine
RUN apk update && apk add --no-cache build-base unzip curl coreutils nasm git cmake zlib-dev jpeg-dev libressl-dev
# mysql-config not found issue: https://stackoverflow.com/questions/25682408/docker-setup-with-a-mysql-container-for-a-python-app/51185814
# https://hub.docker.com/r/psitrax/powerdns/dockerfile
# These libs make this image size > 1GB
RUN set -e; \
        apk add --no-cache --virtual .build-deps \
                mariadb-dev \
        ;
RUN git clone --depth 1 https://github.com/Netflix/vmaf.git vmaf \
        && cd vmaf \
        && make \
        && make install \
        && make clean


RUN apk add --no-cache x264-dev

# Fix 'libwebp not found using pkg-config' issue. Refer to https://github.com/lovell/sharp/issues/190
RUN curl -O https://storage.googleapis.com/downloads.webmproject.org/releases/webp/libwebp-1.0.2.tar.gz \
        && tar xvzf libwebp-1.0.2.tar.gz && rm libwebp-1.0.2.tar.gz \
        && cd libwebp-1.0.2 \
        && ./configure \
        && make \
        && make install; exit 0 \
        && make clean \
        && ldconfig \
        && pkg-config --modversion libwebp

COPY library/lame-3.100.tar.gz ./
RUN tar -zxvf lame-3.100.tar.gz && rm lame-3.100.tar.gz \
        && cd lame-3.100 \
        && ./configure \
        && make \
        && make install \
        && make clean

RUN curl -s http://ffmpeg.org/releases/ffmpeg-snapshot.tar.bz2 | tar jxf - -C . \
        && cd ffmpeg \
        && ./configure --enable-libvmaf --enable-version3  --enable-libx264 --enable-gpl --enable-libwebp \
        --enable-libmp3lame --enable-openssl --enable-nonfree --extra-ldflags=-L/usr/local/lib \
        --disable-debug --disable-doc --disable-ffplay \
        && make -j32 \
        && make install \
        && make clean

COPY requirements.txt /app/
WORKDIR /app
RUN pip3.7 install -r requirements.txt
COPY . /app
CMD ["python3.7", "start_worker.py"]
