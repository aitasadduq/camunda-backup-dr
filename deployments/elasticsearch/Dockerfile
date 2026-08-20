ARG ES_IMAGE=docker.elastic.co/elasticsearch/elasticsearch:9.2.4
FROM ${ES_IMAGE}

RUN elasticsearch-plugin install --batch repository-s3

COPY elasticsearch.yml /usr/share/elasticsearch/config/elasticsearch.yml
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

USER root
RUN chmod +x /usr/local/bin/entrypoint.sh \
  && chown -R elasticsearch:elasticsearch /usr/share/elasticsearch/config /usr/local/bin/entrypoint.sh
USER elasticsearch

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["eswrapper"]
