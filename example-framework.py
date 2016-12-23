#! /usr/bin/env python

'''
File:
    example-framework.py

Purpose:
    Implements a Mesos Framework for processing data through ESPA.

License:
    NASA Open Source Agreement 1.3
'''


import os
import sys
import signal
import json
import logging
import ConfigParser
import urlparse
from argparse import ArgumentParser
from collections import namedtuple
from threading import Thread
from time import sleep


import kazoo.client
from mesos.interface import Scheduler as MesosScheduler
from mesos.interface import mesos_pb2 as MesosPb2
from mesos.native import MesosSchedulerDriver


SYSTEM_VERSION = 'XXX - 0.0.1'


logger = None


class XXXSystemError(Exception):
    """General system error"""
    pass


class XXX_LoggingFilter(logging.Filter):
    """Forces 'XXX' to be provided in the 'subsystem' tag of the log format
       string
    """

    def filter(self, record):
        """Provide the string for the 'subsystem' tag"""

        record.subsystem = 'XXX'

        return True


class XXX_ExceptionFormatter(logging.Formatter):
    """Modifies how exceptions are formatted
    """

    def formatException(self, exc_info):
        """Specifies how to format the exception text"""

        result = super(XXX_ExceptionFormatter,
                       self).formatException(exc_info)

        return repr(result)

    def format(self, record):
        """Specifies how to format the message text if it is an exception"""

        s = super(XXX_ExceptionFormatter, self).format(record)
        if record.exc_text:
            s = s.replace('\n', ' ')
            s = s.replace('\\n', ' ')

        return s


def setup_logging(args):
    """Configures the logging/reporting

    Args:
        args <args>: Command line arguments
    """

    global logger

    # Setup the logging level
    logging_level = logging.INFO
    if args.debug:
        logging_level = logging.DEBUG

    handler = logging.StreamHandler(sys.stdout)
    formatter = XXX_ExceptionFormatter(fmt=('%(asctime)s'
                                            ' %(subsystem)s'
                                            ' %(levelname)-8s'
                                            ' %(message)s'),
                                       datefmt='%Y-%m-%dT%H:%M:%S')

    handler.setFormatter(formatter)
    handler.addFilter(XXX_LoggingFilter())

    logger = logging.getLogger()
    logger.setLevel(logging_level)
    logger.addHandler(handler)

    # Disable annoying INFO messages from the requests module
    logging.getLogger('requests').setLevel(logging.WARNING)
    # Disable annoying INFO messages from the kazoo module
    logging.getLogger("kazoo").setLevel(logging.WARNING)


class FrameworkShutdownControl(object):
    """Captures signals to allow gracefull shutdown of the Mesos Framework
    """

    def __init__(self):
        """Basic initialization
        """

        self.flag = False
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum, frame):
        """Executed when the signals are triggered
        """

        self.flag = True
        logger.info('Shutdown Requested')


def determine_mesos_leader(args, cfg):
    """Use zookeeper to determine the Mesos leader

    Args:
        args <args>: Command line arguments
        cfg <ConfigInfo>: Configuration from the config files
    """

    parsed_url = urlparse.urlparse(cfg.zookeeper, scheme='zk',
                                   allow_fragments=False)

    zk_hosts = parsed_url.netloc
    zk_path = '/mesos'

    zk = kazoo.client.KazooClient(hosts=zk_hosts)
    zk.start()

    try:
        nodes = None
        if zk.exists(zk_path):
            nodes = [node for node in zk.get_children(zk_path)
                     if node.startswith('json.info')]

        if nodes:
            nodes.sort()
            node_path = os.path.join(zk_path, nodes[0])

            (data, stat) = zk.get(node_path)
            data = json.loads(data)

            if args.debug:
                print(json.dumps(data, indent=4))
            return '{}:{}'.format(data['address']['hostname'],
                                  data['address']['port'])

    finally:
        zk.stop()

    raise XXXSystemError('Unable to determine Mesos Leader'
                         ' - Contact System Administrator')


def determine_marathon_leader(cfg):
    """Use zookeeper to determine the Marathon leader

    Args:
        cfg <ConfigInfo>: Configuration from the config files
    """

    parsed_url = urlparse.urlparse(cfg.zookeeper, scheme='zk',
                                   allow_fragments=False)

    zk_hosts = parsed_url.netloc
    # This is the zookeeper path to the marathon leader information
    zk_path = 'marathon/leader'

    zk = kazoo.client.KazooClient(hosts=zk_hosts)
    zk.start()

    try:
        children = zk.get_children(zk_path)
        data = zk.get('{}/{}'.format(zk_path, children[-1]))
        return data[0]

    finally:
        zk.stop()

    raise XXXSystemError('Unable to determine Marathon Leader'
                         ' - Contact System Administrator')


class XXX_Scheduler(MesosScheduler):
    """Implements a Mesos framework scheduler
    """

    def __init__(self, implicitAcknowledgements, executor,
                 args, cfg, shutdown):
        """Scheduler initialization

        Args:
            implicitAcknowledgements <int>: Input (Mesos API)
            executor <ExecutorInfo>: Input (Mesos API)
            args <args>: Input (Command line arguments)
        """

        self.implicitAcknowledgements = implicitAcknowledgements
        self.executor = executor

        self.tasksLaunched = 0
        self.task_queue = list()

        self.debug = args.debug
        self.max_queued_tasks = cfg.max_queued_tasks
        self.max_running_tasks = cfg.max_running_tasks
        self.task_image = cfg.task_image
        self.docker_cfg = cfg.docker_cfg
        self.shutdown = shutdown

        self.framework_id = None
        self.master_node = None
        self.master_port = None

    def registered(self, driver, frameworkId, masterInfo):
        """The framework was registered so log it

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            frameworkId <?>: Input (Mesos API)
            masterInfo <?>: Input (Mesos API)
        """

        logger.info('Registered with framework ID {} on {}:{}'
                    .format(frameworkId.value, masterInfo.hostname,
                            masterInfo.port))

        # Update state
        self.framework_id = frameworkId.value
        self.master_node = masterInfo.hostname
        self.master_port = masterInfo.port

    def reregistered(self, driver, masterInfo):
        """The framework was re-registered so log it

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            masterInfo <?>: Input (Mesos API)
        """

        logger.info('Re-Registered to master on {}'
                    .format(masterInfo.getHostname()))

        # Update state
        self.master_node = masterInfo.hostname

    def resourceOffers(self, driver, offers):
        """Evaluate resource offerings and launch tasks or decline the
           offerings

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            offers <?>: Input (Mesos API)
        """

        # If we are in shutdown mode or at max capacity, decline all offers
        if self.shutdown.flag or self.tasksLaunched == self.max_running_tasks:
            for offer in offers:
                driver.declineOffer(offer.id)
            return

        # If not enough queued tasks, check for more
        if len(self.task_queue) < self.max_queued_tasks:
            self.task_queue.extend(get_tasks(self.max_queued_tasks,
                                             len(self.task_queue)))

        for offer in offers:
            # If we do not have any tasks, decline all offers
            if self.shutdown.flag or not self.task_queue:
                driver.declineOffer(offer.id)
                continue

            offerCpus = 0
            offerMem = 0
            offerDisk = 0

            for resource in offer.resources:
                if resource.name == 'cpus':
                    offerCpus += resource.scalar.value
                elif resource.name == 'mem':
                    offerMem += resource.scalar.value
                elif resource.name == 'disk':
                    offerDisk += resource.scalar.value

            m_tasks = list()
            while (len(self.task_queue) > 0 and
                   self.task_queue[0].check_resources(offerCpus,
                                                      offerMem,
                                                      offerDisk)):
                task = self.task_queue.pop(0)
                m_tasks.append(task.make_task(offer, self.task_image,
                                              self.docker_cfg))

                offerCpus -= task.task_info.cpus
                offerMem -= task.task_info.mem
                offerDisk -= task.task_info.disk

            if len(m_tasks) > 0:
                driver.launchTasks(offer.id, m_tasks)
                self.tasksLaunched += len(m_tasks)
            else:
                driver.declineOffer(offer.id)

            del m_tasks

        logger.debug('Queued job count {}'.format(len(self.task_queue)))
        logger.debug('Running job count {}'.format(self.tasksLaunched))

    def statusUpdate(self, driver, update):
        """Update task status

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            update <?>: Input (Mesos API)
        """

        task_id = update.task_id.value
        state = update.state

        logger.info('Task {} is in state {}'
                    .format(task_id, MesosPb2.TaskState.Name(state)))

        # Gather the container information for the running task, so it can be
        # used later during the finished or failed statuses
        if state == MesosPb2.TASK_RUNNING:
            logger.debug('master_node {}'.format(self.master_node))

        if (state == MesosPb2.TASK_FINISHED or
                state == MesosPb2.TASK_LOST or
                state == MesosPb2.TASK_KILLED or
                state == MesosPb2.TASK_ERROR or
                state == MesosPb2.TASK_FAILED):

            self.tasksLaunched -= 1

            if self.shutdown.flag and self.tasksLaunched == 0:
                logger.info('Shutdown Requested')
                driver.stop()

    def frameworkMessage(self, driver, executorId, slaveId, message):
        """Recieved a framework message so log it

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            executorId <?>: Input (Mesos API)
            slaveId <?>: Input (Mesos API)
            message <?>: Input (Mesos API)
        """

        logger.info('Received Framework Message: {} {} {}'
                    .format(executorId, slaveId, message))


TaskInfo = namedtuple('TaskInfo', ('cpus', 'mem', 'disk', 't_id'))


class Task(object):
    """Creates a Mesos task
    """

    def __init__(self, task_id):
        """Initialize the task
        """

        self.task_info = TaskInfo(cpus=.2, mem=32, disk=10,
                                  t_id=task_id)

    def check_resources(self, cpus, mem, disk):
        """Compare the job requirements against the offered resources
        """

        if (self.task_info.cpus <= cpus and
                self.task_info.mem <= mem and
                self.task_info.disk <= disk):
            return True

        return False

    def build_command_line(self):
        """Create the command line to call within the container
        """

        cmd = list()
        # Must specify the entry point, because we are using the Docker
        # containerizer mode of operation within Mesos
        #cmd.append('sleep 30')
        cmd.append('python -c from time import sleep; sleep(30)')

        logger.debug(' '.join(cmd))

        return ' '.join(cmd)

    def build_task_id(self):
        """Create a task ID
        """

        return 'XXX-{}'.format(self.task_info.t_id)

    def build_task_name(self):
        """Create a task name
        """

        return 'XXX {}'.format(self.task_info.t_id)

    def make_task(self, offer, task_image, docker_cfg):
        """Create a Mesos task from the job information
        """

        # Create the container object
        container = MesosPb2.ContainerInfo()
        container.type = 1  # MesosPb2.ContainerInfo.Type.DOCKER

        # Specify container Docker image
        docker = MesosPb2.ContainerInfo.DockerInfo()
        docker.image = task_image
        docker.network = 1  # MesosPb2.ContainerInfo.DockerInfo.Network.HOST
        docker.network = 2  # MesosPb2.ContainerInfo.DockerInfo.Network.BRIDGE
        docker.force_pull_image = False

        # Temporary hardcode group 501
        user_param = docker.parameters.add()
        user_param.key = 'user'
        user_param.value = '{}:501'.format(os.getuid())

        container.docker.MergeFrom(docker)

        # Create the task object
        task = MesosPb2.TaskInfo()
        task.task_id.value = self.build_task_id()
        task.slave_id.value = offer.slave_id.value
        task.name = self.build_task_name()

        # Add the container
        task.container.MergeFrom(container)

        # Specify the command line to execute the Docker container
        command = MesosPb2.CommandInfo()
        command.value = self.build_command_line()

        # Add the docker uri for logging into the remote repository
        command.uris.add().value = docker_cfg

        '''
        The MergeFrom allows to create an object then to use this object
        in another one.  Here we use the new CommandInfo object and specify
        to use this instance for the parameter task.command.
        '''
        task.command.MergeFrom(command)

        # Setup the resources
        cpus = task.resources.add()
        cpus.name = 'cpus'
        cpus.type = MesosPb2.Value.SCALAR
        cpus.scalar.value = self.task_info.cpus

        mem = task.resources.add()
        mem.name = 'mem'
        mem.type = MesosPb2.Value.SCALAR
        mem.scalar.value = self.task_info.mem

        disk = task.resources.add()
        disk.name = 'disk'
        disk.type = MesosPb2.Value.SCALAR
        disk.scalar.value = self.task_info.disk

        return task


TASK_ID = 0


def get_tasks(max_queued_tasks, current_count):
    """Gets new tasks if needed
    """

    global TASK_ID

    tasks = list()
    count = current_count

    if count < max_queued_tasks:
        tasks.append(Task('{0:0>5}'.format(TASK_ID)))
        count += 1
        TASK_ID += 1

    return tasks


def retrieve_command_line():
    """Read and return the command line arguments
    """

    description = 'Example Mesos Framework'
    parser = ArgumentParser(description=description)

    parser.add_argument('--version',
                        action='version',
                        version=SYSTEM_VERSION)

    parser.add_argument('--debug',
                        action='store_true',
                        dest='debug',
                        default=False,
                        help='display error information')

    return parser.parse_args()


def xxx_framework(cfg):
    """Establish framework information
    """

    framework = MesosPb2.FrameworkInfo()
    framework.user = ''
    framework.name = 'Example XXX Framework'
    framework.principal = cfg.mesos_principal

    return framework


def xxx_executor():
    """Establish executor information
    """

    executor = MesosPb2.ExecutorInfo()
    executor.executor_id.value = 'default'
    executor.name = 'XXX Executor'

    return executor


def xxx_credential(cfg):
    """Establish framework information
    """

    credential = MesosPb2.Credential()
    credential.principal = cfg.mesos_principal
    credential.secret = cfg.mesos_secret

    return credential


class XXXMissingConfigError(XXXSystemError):
    """Specific to configuration errors"""
    pass


# Configuration information structure
ConfigInfo = namedtuple('ConfigInfo', ('zookeeper',
                                       'max_queued_tasks',
                                       'max_running_tasks',
                                       'task_image',
                                       'docker_cfg',
                                       'mesos_principal',
                                       'mesos_secret',
                                       'mesos_role'))


def read_configuration():
    """Reads configuration from the config file and returns it

    Returns:
        <ConfigInfo>: Populated with configuration read from the config file
    """

    base_cfg_path = '/usr/local/usgs/bridge'

    config_file = os.path.join(base_cfg_path, 'pgs.conf')

    if not os.path.isfile(config_file):
        raise XXXMissingConfigError('Missing {}'.format(config_file))

    cfg = ConfigParser.ConfigParser()
    cfg.read(config_file)

    xxx_section = 'pgs'
    xxx_fw_section = 'pgs_framework'

    return ConfigInfo(zookeeper=cfg.get(xxx_section, 'zookeeper'),
                      max_queued_tasks=10,
                      max_running_tasks=2,
                      task_image='python:3',
                      docker_cfg=cfg.get(xxx_fw_section, 'docker_pkg'),
                      mesos_principal=cfg.get(xxx_section, 'mesos_principal'),
                      mesos_secret=cfg.get(xxx_section, 'mesos_secret'),
                      mesos_role=cfg.get(xxx_section, 'mesos_role'))


def run_scheduler_driver(driver):
    """Runs the driver
    """

    status = 0
    if driver.run() != MesosPb2.DRIVER_STOPPED:
        status = 1

    # TODO TODO TODO - Is this stop even needed
    driver.stop()

    return status


def main():
    """Main processing routine for the application
    """

    # Determine command line arguments
    args = retrieve_command_line()

    # Configure the logging
    setup_logging(args)

    # Read the configuration
    cfg = read_configuration()

    framework = xxx_framework(cfg)
    executor = xxx_executor()
    credential = xxx_credential(cfg)

    implicitAcknowledgements = 1

    shutdown = FrameworkShutdownControl()

    mesos_scheduler = XXX_Scheduler(implicitAcknowledgements, executor,
                                    args, cfg, shutdown)
    driver = MesosSchedulerDriver(mesos_scheduler, framework,
                                  cfg.zookeeper, implicitAcknowledgements,
                                  credential)

    fw_thread = Thread(target=run_scheduler_driver, args=[driver])
    fw_thread.start()

    while fw_thread.is_alive():
        sleep(5)

    return 0


if __name__ == '__main__':
    main()
