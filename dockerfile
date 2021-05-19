FROM nickgryg/alpine-pandas:3.9

COPY . .

RUN apk add --update --no-cache --virtual .build-deps \
        g++ \
        libxml2 \
        libxml2-dev && \
    apk add libxslt-dev && \
    pip install -r requirements.txt

CMD python3