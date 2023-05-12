# -*- coding: utf-8 -*-


def boot(config_file, license):
    if (config_file or license):
        try:
            import newrelic.agent
            newrelic.agent.initialize()
        except Exception as e:
            raise Exception(f'Failed to init new relic agent {e}')
