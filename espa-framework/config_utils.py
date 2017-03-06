"""Implements reading of configuration files
"""

import os
from ConfigParser import ConfigParser
from collections import namedtuple


from espa_base_exception import ESPASystemError


class ESPAConfigError(ESPASystemError):
    """Specific to configuration errors"""
    pass


def get_cfg_file_path(filename):
    """Build the full path to the config file

    Args:
        filename <str>: The name of the file to append to the full path.

    Raises:
        ESPAConfigError(message)
    """

    # Use the users home directory as the base source directory for
    # configuration
    if 'HOME' not in os.environ:
        raise ESPAConfigError('[HOME] not found in environment')
    home_dir = os.environ.get('HOME')

    # Build the full path to the configuration file
    config_path = os.path.join(home_dir, '.usgs', 'espa', filename)

    return config_path


def retrieve_cfg(filename):
    """Retrieve the configuration for the cron

    Args:
        filename <str>: The name of the configuration file

    Returns:
        <ConfigParser>: Configuration

    Raises:
        ESPAConfigError(message)
    """

    # Build the full path to the configuration file
    config_path = get_cfg_file_path(filename)

    if not os.path.isfile(config_path):
        raise ESPAConfigError('Missing configuration file [{}]'
                              .format(config_path))

    # Create the object and load the configuration
    cfg = ConfigParser()
    cfg.read(config_path)

    return cfg


# Configuration information structure
FrameworkConfigInfo = namedtuple('ConfigInfo', ('zookeeper',
                                                'user',
                                                'principal',
                                                'role',
                                                'secret',
                                                'max_jobs'))


# Name of the framework configuration file
FRAMEWORK_CFG_FILENAME = 'framework.conf'


def read_fw_configuration():
    """Read the framework configuration file

    Returns:
        <FrameworkConfigInfo>: Configuration for ESPA framework
    """

    cfg = retrieve_cfg(FRAMEWORK_CFG_FILENAME)

    section = 'framework'

    return FrameworkConfigInfo(zookeeper=cfg.get(section, 'zookeeper'),
                               user=cfg.get(section, 'user'),
                               principal=cfg.get(section, 'principal'),
                               role=cfg.get(section, 'role'),
                               secret=cfg.get(section, 'secret'),
                               max_jobs=cfg.get(section, 'max_jobs'))
