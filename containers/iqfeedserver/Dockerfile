FROM python:3.8.2-alpine3.11

COPY apps/iqfeedserver/requirements.txt /apps/iqfeedserver/requirements.txt
RUN apk add --no-cache build-base tini \
    && pip install -r /apps/iqfeedserver/requirements.txt \
    && apk del build-base

EXPOSE 9999
ENV PYTHONPATH=/apps
COPY apps/iqfeedserver /apps/iqfeedserver
ENTRYPOINT ["/apps/iqfeedserver/entrypoint.sh"]
CMD ["python", "/apps/iqfeedserver/main.py"]
