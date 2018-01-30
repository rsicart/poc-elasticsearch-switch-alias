import argparse
import logging
import sys
from string import Template
from elasticsearch import Elasticsearch

class SwitchAlias:
    def __init__(self, url=None, alias_name=None, index_name=None):
        self.es_client = Elasticsearch()
        self.alias_name = alias_name
        self.index_name_new = index_name
        self.index_name_current = None
        self.template_update = None


    def fetch_current_index(self):
        '''Get index name where alias_name is currently pointing
        '''
        if not self.alias_name:
            logger.error('Alias %s not specified, please init correctly')
            sys.exit(1)
        # example line: categories twitter   - - -
        alias_line = self.es_client.cat.aliases(name=self.alias_name)
        if not alias_line:
            logger.error('Alias %s not found', self.alias_name)
            sys.exit(1)
        alias_list = alias_line.split(" ")
        self.index_name_current = alias_list[1]
        logger.debug('Found alias %s on index %s', self.alias_name, self.index_name_current)


    def update_alias(self):
        '''Update alias current index to new index
        '''

        if not self.alias_name:
            logger.error('Alias %s not specified, please init correctly')
            sys.exit(1)

        if not self.index_name_current:
            logger.error('Current index %s not specified, please init correctly')
            sys.exit(1)

        if not self.index_name_new:
            logger.error('New index %s not specified, please init correctly')
            sys.exit(1)

        exists = self.es_client.indices.exists(index=self.index_name_new)
        if not exists:
            logger.error('New index %s does not exits')
            sys.exit(1)

        # update
        update_body = self.get_template().substitute(index_current=self.index_name_current, index_new=self.index_name_new, alias=self.alias_name)
        self.es_client.indices.update_aliases(body=update_body)

        # alias exists in new index
        self.es_client.indices.exists_alias(index=self.index_name_new, name=self.alias_name)
        logger.debug('Successfuly update alias %s, current index %s', self.alias_name, self.index_name_new)


    def get_template(self):
        return Template('''
            {
                "actions" : [
                    { "remove" : { "index" : "$index_current", "alias" : "$alias" } },
                    { "add" : { "index" : "$index_new", "alias" : "$alias" } }
                ]
            }
            ''')


if __name__ == '__main__':
    # args
    parser = argparse.ArgumentParser(description="Run")
    parser.add_argument('--url', '-u',
                        required=True,
                        default=15,
                        help='Elasticsearch URL, without path')
    parser.add_argument('--auth', '-a',
                        help='Enable http basic auth using username:password format')
    parser.add_argument('--timeout', '-o',
                        default=15,
                        help='Http timeout for requests sent to target, in seconds')
    parser.add_argument('--alias', '-l',
                        required=True,
                        help='Alias to switch')
    parser.add_argument('--index', '-i',
                        required=True,
                        help='New alias to point to')
    cli = parser.parse_args()

    # logger and loglevel
    logger = logging.getLogger('switch_alias')
    logger.setLevel(logging.DEBUG)
    # logger handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # logger formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # switch alias
    switch_alias = SwitchAlias(alias_name=cli.alias, index_name=cli.index)
    switch_alias.fetch_current_index()
    switch_alias.update_alias()

