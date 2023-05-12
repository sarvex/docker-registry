# -*- coding: utf-8 -*-


def boot(application, config):
    if config and config['origins']:
        try:
            from flask.ext.cors import CORS
            for i in config.keys():
                application.config[f'CORS_{i.upper()}'] = config[i]
            CORS(application)
        except Exception as e:
            raise Exception(f'Failed to init cors support {e}')
