FROM connormanning/entwine:dependencies
MAINTAINER Connor Manning <connor@hobu.co>

ARG branch=2.1.0-verbose
RUN echo Branch: $branch
ADD https://api.github.com/repos/tomap-app/entwine/commits?sha=$branch \
    /tmp/bust-cache

# RUN git clone https://github.com/tomap-app/entwine.git /var/entwine && \
COPY ./ /var/entwine/
RUN cd /var/entwine && \
    # git checkout $branch && \
    mkdir build && \
    cd build && \
    cmake -G Ninja \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=Release .. && \
    ninja && \
    ninja install

ENTRYPOINT ["entwine"]

