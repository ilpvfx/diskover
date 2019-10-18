import functools
import elasticsearch_dsl

try:
    from elasticsearch5 import (
        Elasticsearch,
        helpers,
        Urllib3HttpConnection
    )

except ImportError:
    try:
        from elasticsearch import (
            Elasticsearch,
            helpers,
            Urllib3HttpConnection
        )

    except ImportError:
        raise


def bulk(
    _original_fn, es, doclist, index=None, chunk_size=100, request_timeout=10
):
    for document in doclist:
        _type = document.pop(
            '_type'
        )

        if (
            '_op_type' not in document or document['_op_type'] not in ('update', )
        ):
            document['type'] = (
                _type
            )

    _original_fn(
        es, doclist, index=index, chunk_size=chunk_size, request_timeout=request_timeout
    )


class _ElasticSearch(Elasticsearch):
    def search(self, *args, **kwargs):
        if 'doc_type' in kwargs:
            document_type = (
                kwargs.pop('doc_type')
            )

            new_query = elasticsearch_dsl.Search.from_dict(
                kwargs['body']
            )

            kwargs['body'] = new_query.query(
                'match', type=document_type
            )

        result = super(_ElasticSearch, self).search(
            *args, **kwargs
        )

        _total_value = None
        for key in ('hits', 'total', 'value'):
            if _total_value is None:
                _total_value = result

            if key not in _total_value:
                break

            _total_value = (
                _total_value[key]
            )

        else:
            result['hits']['total'] = (
                _total_value
            )

        return result

    @property
    def indices(self):
        class Wrapper(object):
            def __init__(self, obj):
                self.obj = obj

            def __getattr__(self, item):
                if item in ('create', ):
                    def _remove_mapping(*args, **kwargs):
                        if u'mappings' in kwargs.get(u'body', {}):
                            kwargs[u'body'].pop(
                                u'mappings'
                            )

                        return self.obj.create(
                            *args, **kwargs
                        )

                    return _remove_mapping

                return getattr(
                    self.obj, item
                )

        return Wrapper(
            self._indices
        )

    @indices.setter
    def indices(self, value):
        self._indices = (
            value
        )

    def index(self, *args, **kwargs):
        pass


def patch_all():
    def patched_connect_to_elasticsearch():
        from diskover import (
            config
        )

        es_conn = _ElasticSearch(
            hosts=config['es_host'],
            port=config['es_port'],
            http_auth=(config['es_user'], config['es_password']),
            connection_class=Urllib3HttpConnection,
            timeout=config['es_timeout'], maxsize=config['es_maxsize'],
            max_retries=config['es_max_retries'], retry_on_timeout=True
        )

        import diskover_connections
        # Patch the global..
        diskover_connections.es_conn = (
            es_conn
        )

        return (
            es_conn
        )

    helpers.bulk = (
        functools.partial(
            bulk, helpers.bulk
        )
    )

    import diskover_connections

    diskover_connections.connect_to_elasticsearch = (
        patched_connect_to_elasticsearch
    )

