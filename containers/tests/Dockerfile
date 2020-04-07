FROM python:3.8.2-alpine3.11

COPY apps/iqfeedserver/requirements.txt /apps/iqfeedserver/requirements.txt
COPY apps/tests/requirements.txt /apps/tests/requirements.txt
RUN apk add --no-cache build-base tini \
    && pip install -r /apps/iqfeedserver/requirements.txt \
    && pip install -r /apps/tests/requirements.txt \
    && apk del build-base

ENV PYTHONPATH=/apps
COPY containers/tests/files /
COPY apps /apps
ENTRYPOINT ["/apps/iqfeedserver/entrypoint.sh"]
CMD ["pytest", "/apps/tests"]
