import argparse
import logging
import sys
from string import Template
from elasticsearch import Elasticsearch

class SwitchAlias:
    def __init__(self):
        self.es_client = Elasticsearch()
        self.set_template_update(self.init_template_update())
        self.alias_name = None
        self.index_name_new = None
        self.index_name_current = None

    def set_alias_name(self, alias_name):
        self.alias_name = alias_name

    def get_alias_name(self):
        return self.alias_name

    def set_index_name_new(self, index_name_new):
        self.index_name_new = index_name_new

    def get_index_name_new(self):
        return self.index_name_new

    def set_index_name_current(self, index_name_current):
        self.index_name_current = index_name_current

    def get_index_name_current(self):
        return self.index_name_current

    def set_template_update(self, tpl):
        self.template_update = tpl

    def get_template_update(self):
        return self.template_update


    def init_template_update(self):
        '''Initialise self.template_update with a Template to update aliases
        '''
        return Template('''
            {
                "actions" : [
                    { "remove" : { "index" : "$index_name_current", "alias" : "$alias_name" } },
                    { "add" : { "index" : "$index_name_new", "alias" : "$alias_name" } }
                ]
            }
            ''')


    def get_body_update(self):
        '''Returns a string with values from self.template_update replaced
        '''
        return self.get_template_update().substitute(index_name_current=self.get_index_name_current(),
                                                    index_name_new=self.get_index_name_new(),
                                                    alias_name=self.get_alias_name())


    def fetch_current_index(self):
        '''Get index name where alias_name is currently pointing
        '''
        if not self.get_alias_name():
            logger.error('Alias %s not specified, please init correctly')
            sys.exit(1)
        # example line: categories twitter   - - -
        alias_line = self.es_client.cat.aliases(name=self.get_alias_name())
        if not alias_line:
            logger.error('Alias %s not found', self.get_alias_name())
            sys.exit(1)
        alias_list = alias_line.split(" ")
        self.set_index_name_current(alias_list[1])
        logger.debug('Found alias %s on index %s', self.get_alias_name(), self.get_index_name_current())


    def update_alias(self):
        '''Update alias current index to new index
        '''

        if not self.get_alias_name():
            logger.error('Alias %s not specified, please init correctly')
            sys.exit(1)

        if not self.get_index_name_current():
            logger.error('Current index %s not specified, please init correctly')
            sys.exit(1)

        if not self.get_index_name_new():
            logger.error('New index %s not specified, please init correctly')
            sys.exit(1)

        exists = self.es_client.indices.exists(index=self.get_index_name_new())
        if not exists:
            logger.error('New index %s does not exits')
            sys.exit(1)

        # update
        self.es_client.indices.update_aliases(body=self.get_body_update())

        # alias exists in new index
        self.es_client.indices.exists_alias(index=self.get_index_name_new(), name=self.get_alias_name())
        logger.debug('Successfuly updated alias %s, current index %s', self.get_alias_name(), self.get_index_name_new())



if __name__ == '__main__':
    # args
    parser = argparse.ArgumentParser(description="Run")
    parser.add_argument('--alias', '-l',
                        required=True,
                        help='Alias to switch')
    parser.add_argument('--index', '-i',
                        required=True,
                        help='New alias to point to')
    cli = parser.parse_args()

    # logger, loglevel, formatter and handler
    logger = logging.getLogger('switch_alias')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # switch alias
    switch_alias = SwitchAlias()
    switch_alias.set_alias_name(cli.alias)
    switch_alias.set_index_name_new(cli.index)
    switch_alias.fetch_current_index()
    switch_alias.update_alias()

