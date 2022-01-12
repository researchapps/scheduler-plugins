FROM ubuntu:latest as basegoflux

ENV TZ="America/Los_Angeles"
ENV DEBIAN_FRONTEND="noninteractive"

RUN apt-get update && apt-get -y upgrade && apt-get -y --no-install-recommends install \
    less \
    libc6-dev \
    wget \
    git \
    autoconf \
    automake \
    libtool \
    libelf-dev \ 
    libncurses5-dev \
    make \
    libsodium-dev \
    libzmq3-dev \
    libczmq-dev \
    uuid-dev \
    libjansson-dev \
    liblz4-dev \
    libhwloc-dev \
    libsqlite3-dev \
    lua5.1 \
    liblua5.1-dev \
    lua-posix \
    python3-dev \
    python3-cffi \
    python3-six \
    python3-yaml \
    python3-jsonschema \
    python3-sphinx \
    python3-pip \
    python3-setuptools \
    libmpich-dev && apt-get purge -y python2.7-minimal && apt-get -y clean  && apt-get -y autoremove

RUN \
   echo 'alias python="/usr/bin/python3.8"' >> /root/.bashrc && \
   echo 'alias pip="/usr/bin/pip3"' >> /root/.bashrc && \
   . /root/.bashrc

RUN \
   echo 'set number' >> /root/.vimrc 

# Remove python2
# You already have Python3 but
# don't care about the version
RUN ln -s /usr/bin/python3 /usr/bin/python
#RUN apt-get install -y python3-pip && apt-get -y clean  && apt-get -y autoremove # && ln -s /usr/bin/pip3 /usr/bin/pip
 
#Fluxion
RUN apt-get -y --no-install-recommends install \
    libhwloc-dev \
    libboost-dev \
    libboost-system-dev \
    libboost-filesystem-dev \
    libboost-graph-dev \
    libboost-regex-dev \
    libxml2-dev \
    libyaml-cpp-dev \ 
    pkg-config && apt-get -y clean  && apt-get -y autoremove

RUN cd /root/ && mkdir flux-install
WORKDIR /root/
RUN git clone https://github.com/flux-framework/flux-core.git --branch v0.25.0 --single-branch && \
	cd flux-core/ && ./autogen.sh && PYTHON_VERSION=3.8 ./configure --prefix=/root/flux-install \ 
    	&& make && make install && cd /root && rm -rf /root/flux-core


# Install go 15
WORKDIR /home
RUN wget https://dl.google.com/go/go1.16.10.linux-amd64.tar.gz  && tar -xvf go1.16.10.linux-amd64.tar.gz && \
         mv go /usr/local  &&  rm go1.16.10.linux-amd64.tar.gz

ENV GOROOT "/usr/local/go"
ENV GOPATH "/go"
ENV PATH "$GOROOT/bin:$PATH"
RUN mkdir -p /go/src

WORKDIR /root/
ENV PATH "/root/flux-install/bin:$PATH"
ENV LD_LIBRARY_PATH "/root/flux-install/lib/flux:/root/flux-install/lib"
RUN flux keygen

RUN  git clone https://github.com/cmisale/flux-sched.git --branch gobind-dev --single-branch \ 
	&& cd /root/flux-sched/ \
	&& ./autogen.sh && PYTHON_VERSION=3.8 ./configure --prefix=/root/flux-install && make && make install \
	&& cp t/data/resource/jgfs/tiny.json /home \
	&& cp -r resource/hlapi/bindings/c/.libs/* resource/.libs/* /root/flux-install/lib/ \
	&& cp -r resource/hlapi/bindings/go/src/fluxcli /go/src/ \
	&& mv  resource/hlapi/bindings /tmp \
	&& cd /root && rm -rf flux-sched && mkdir -p flux-sched/resource/hlapi && mv /tmp/bindings flux-sched/resource/hlapi

RUN apt-get purge -y git  python3-dev \
    python3-cffi \
    python3-six \
    python3-yaml \
    python3-jsonschema \
    python3-sphinx \
    python3-pip \
    python3-setuptools \
    && apt-get -y clean && apt-get -y autoremove

WORKDIR /go/src
RUN cd fluxcli &&  go mod init fluxcli && go mod tidy &&  GOOS=linux CGO_CFLAGS="-I/root/flux-sched/resource/hlapi/bindings/c -I/root/flux-install/include" CGO_LDFLAGS="-L/root/flux-install/lib/ -lreapi_cli  -L/root/flux-install/lib/ -lresource -lstdc++ -lczmq -ljansson -lhwloc -lboost_system -L/root/flux-install/lib -lflux-hostlist -lboost_graph -lyaml-cpp" go install fluxcli 


WORKDIR /go/src/sigs.k8s.io/scheduler-plugins
COPY cmd cmd/
COPY hack hack/
COPY pkg  pkg/
COPY flux-k8s/flux-plugin/kubeflux pkg/kubeflux/
COPY test test/
COPY go.mod .
COPY go.sum .
COPY Makefile .
ARG ARCH
ARG RELEASE_VERSION

RUN RELEASE_VERSION=${RELEASE_VERSION} make build-scheduler.$ARCH 

RUN mkdir -p /home/data/jgf/ && mv /home/tiny.json /home/data/jgf/



FROM ubuntu:latest 

# Copy Flux libraries and headers
COPY --from=basegoflux /root/flux-install /root/flux-install/
COPY --from=basegoflux /root/flux-sched/resource/hlapi /root/flux-sched/resource/hlapi/
COPY --from=basegoflux /root/flux-install/lib/libflux-hostlist* /usr/local/lib/
COPY --from=basegoflux /go/src/fluxcli  /go/src/fluxcli/
COPY --from=basegoflux /go/src/sigs.k8s.io/scheduler-plugins/bin/kube-scheduler /bin/kube-scheduler
COPY --from=basegoflux /home/data/jgf/ /home/data/jgf/

# Reinstall dependencies we need
#Fluxion
RUN  apt -y upgrade && apt -u update && apt -y clean && apt-get -y autoremove

RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends tzdata && apt -y --no-install-recommends install \
    libhwloc-dev \
    libboost-dev \
    libboost-system-dev \
    libboost-filesystem-dev \
    libboost-graph-dev \
    libboost-regex-dev \
    libxml2-dev \
    libyaml-cpp-dev \ 
    libsodium-dev \
    libzmq3-dev \
    libczmq-dev \
    uuid-dev \
    libjansson-dev \
    liblz4-dev \
    libhwloc-dev \
    libsqlite3-dev \
    wget \
    make \
    libc6-dev  \
    lua5.1 liblua5.1-dev lua-posix && apt-get -y clean  && apt-get -y autoremove
RUN mkdir -p /home/data/jobspecs/
COPY flux-k8s/flux-plugin/manifests/kubeflux/sched-config.yaml /home/sched-config.yaml
WORKDIR /bin
CMD ["kube-scheduler"]
