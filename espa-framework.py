#! /usr/bin/env python

'''
File:
    espa-framework.py

Purpose:
    Implements a Mesos Framework for processing data through ESPA.

License:
    NASA Open Source Agreement 1.3
'''


import os
import sys
import json
import uuid
import copy
import logging
import requests
from argparse import ArgumentParser


from mesos.interface import Scheduler as MesosScheduler
from mesos.interface import mesos_pb2 as MesosPb2
from mesos.native import MesosSchedulerDriver


# Setup the default logger format and level.  Log to STDOUT.
logging.basicConfig(format=('%(asctime)s.%(msecs)03d %(process)d'
                            ' %(levelname)-8s'
                            ' %(filename)s:%(lineno)d:'
                            '%(funcName)s -- %(message)s'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO,
                    stream=sys.stdout)


def get_mesos_http_api_content(url):
    """Returns the json content from the specified url as a dictionary
    """

    session = requests.Session()

    session.mount('http://', requests.adapters.HTTPAdapter(max_retries=3))
    session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))

    req = session.get(url=url)
    if not req.ok:
        logger = logging.getLogger(__name__)
        logger.error('HTTP Content Retrieval - Failed')
        req.reaise_for_status()

    data = json.loads(req.content)

    del req
    del session

    return data


def get_agent_hostname(master_node, master_port, agent_id):
    """Retrieves the hostname for the specified Agent ID

    Args:
        master_node <str>: Master node IP address
        master_port <str>: Master node port
        agent_id <str>: Agent ID to ge the hostname for

    Returns:
        <str>: Hostname for the Agent
    """

    data = get_mesos_http_api_content(url='http://{}:{}/slaves'
                                      .format(master_node, master_port))

    hostname = None

    for agent in data['slaves']:
        if agent['id'] == agent_id:
            hostname = agent['hostname']

    return hostname


def get_agent_id(master_node, master_port, framework_id, task_id):
    """Retrieves the Agent ID information for the specified Task ID

    Args:
        master_node <str>: Master node IP address
        master_port <str>: Master node port
        framework_id <str>: Framework ID for the Task ID
        task_id <str>: Task ID we are looking for

    Returns:
        <str>: Agent ID
    """

    data = get_mesos_http_api_content(url='http://{}:{}/tasks'
                                      .format(master_node, master_port))

    agent_id = None

    for task in data['tasks']:
        if (task['framework_id'] == framework_id and
                task['id'] == task_id):

            agent_id = task['slave_id']
            break

    return agent_id


class ESPA_Scheduler(MesosScheduler):
    """Implements a Mesos framework scheduler
    """

    def __init__(self, implicitAcknowledgements, executor, args):
        """Scheduler initialization

        Args:
            implicitAcknowledgements <int>: Input (Mesos API)
            executor <ExecutorInfo>: Input (Mesos API)
            args <args>: Input (Command line arguments)
        """

        self.implicitAcknowledgements = implicitAcknowledgements
        self.executor = executor

        self.shutdownRequest = False

        self.tasksLaunched = 0
        self.job_queue = list()
        self.tasks = dict()

        self.job_filename = args.job_filename
        self.framework_id = None
        self.master_node = None
        self.master_port = None
        self.verbose = args.verbose

    def registered(self, driver, frameworkId, masterInfo):
        """The framework was registered so log it

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            frameworkId <?>: Input (Mesos API)
            masterInfo <?>: Input (Mesos API)
        """

        logger = logging.getLogger(__name__)
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

        logger = logging.getLogger(__name__)
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

        logger = logging.getLogger(__name__)

        # Check whether a shoutdown request has been issued
        if not self.shutdownRequest:
            if os.path.isfile('shutdown_framework'):
                self.shutdownRequest = True
                os.unlink('shutdown_framework')
                logger.info('Shutdown Requested')
        elif self.tasksLaunched == 0:
            logger.info('Tasks Complete - Shutting Down Framework')
            driver.stop()

        # If not enough queued jobs, check for more
        # TODO TODO TODO - Make the job queue size configurable
        if len(self.job_queue) < 50:
            self.job_queue.extend(get_jobs(self.job_filename))
            # TODO TODO TODO - Call the ESPA API server to update the state
            #                  Do we need to set to a queued state for ESPA?

        for offer in offers:
            if not self.job_queue or self.shutdownRequest:
                driver.declineOffer(offer.id)
                break

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

            tasks = list()
            while (len(self.job_queue) > 0 and
                   self.job_queue[0].check_resources(offerCpus,
                                                     offerMem,
                                                     offerDisk)):
                job = self.job_queue.pop(0)
                tasks.append(job.make_task(offer))

                offerCpus -= job.job_info['cpus']
                offerMem -= job.job_info['mem']
                offerDisk -= job.job_info['disk']

                # TODO TODO TODO - Call the ESPA API server to update the
                #                  state
                #     Set PROCESSING

            if len(tasks) > 0:
                driver.launchTasks(offer.id, tasks)
                self.tasksLaunched += len(tasks)
            else:
                driver.declineOffer(offer.id)

            del tasks

        if self.verbose:
            logger.info('Queued job count {}'.format(len(self.job_queue)))
            logger.info('Running job count {}'.format(self.tasksLaunched))


    def statusUpdate(self, driver, update):
        """Update task status

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            update <?>: Input (Mesos API)
        """

        task_id = update.task_id.value
        state = update.state

        logger = logging.getLogger(__name__)
        logger.info('Task {} is in state {}'
                    .format(task_id, MesosPb2.TaskState.Name(state)))

#        if update.HasField('container_status'):
#            logger.info('Field [container_status] {}'.format(update.container_status))
#        if update.HasField('data'):
#            logger.info('Field [data] {}'.format(update.data))
#        if update.HasField('executor_id'):
#            logger.info('Field [executor_id] {}'.format(update.executor_id))
#        if update.HasField('healthy'):
#            logger.info('Field [healthy] {}'.format(update.healthy))
#        if update.HasField('labels'):
#            logger.info('Field [labels] {}'.format(update.labels))
        if update.HasField('message'):
            logger.info('Field [message] {}'.format(update.message))
#        if update.HasField('reason'):
#            logger.info('Field [reason] {}'.format(update.reason))
#        if update.HasField('slave_id'):
#            logger.info('Field [slave_id] {}'.format(update.slave_id))
#        if update.HasField('source'):
#            logger.info('Field [source] {}'.format(update.source))
#        if update.HasField('state'):
#            logger.info('Field [state] {}'.format(update.state))
#        if update.HasField('task_id'):
#            logger.info('Field [task_id] {}'.format(update.task_id))
#        if update.HasField('timestamp'):
#            logger.info('Field [timestamp] {}'.format(update.timestamp))
#        if update.HasField('uuid'):
#            logger.info('Field [uuid] {}'
#                        .format(str(uuid.UUID(bytes=update.uuid))))

        # Gather the container information for the running task, so it can be
        # used later during the finished or failed statuses
        if state == MesosPb2.TASK_RUNNING:
            self.tasks[task_id] = json.loads('{{"data": {} }}'
                                             .format(update.data))

            logger.info('master_node {}'.format(self.master_node))
            agent_id = get_agent_id(self.master_node, self.master_port,
                                    self.framework_id, task_id)
            logger.info('agent_id {}'.format(agent_id))

        #for field in update.ListFields():
        #    logger.info('Field {}'.format(field))

        # This is an example of the task name built earlier
        # Which can be used to specify to the database what to update
        ### ESPA-docker-mode-LT05_L1TP_045028_20000403_20161005_01_A1

        if (state == MesosPb2.TASK_FINISHED or
                state == MesosPb2.TASK_LOST or
                state == MesosPb2.TASK_KILLED or
                state == MesosPb2.TASK_ERROR or
                state == MesosPb2.TASK_FAILED):

            for mount in self.tasks[task_id]['data'][0]['Mounts']:
                if mount['Destination'] == '/mnt/mesos/sandbox':
                    logger.info('Logfile Location = {}'
                                .format(mount['Source']))
            #logger.info('Data {}'
            #            .format(json.dumps(self.tasks[task_id], indent=4)))

            self.tasksLaunched -= 1
            del self.tasks[task_id]

            agent_id = get_agent_id(self.master_node, self.master_port,
                                    self.framework_id, task_id)
            logger.info('agent_id {}'.format(agent_id))
            agent_hostname = get_agent_hostname(self.master_node,
                                                self.master_port, agent_id)
            logger.info('agent_hostname {}'.format(agent_hostname))


            # TODO TODO TODO - Call the ESPA API server to update the state
            # if state == MesosPb2.TASK_FINISHED:
            #     Set SUCCESS
            # else:
            #     Set ERROR/FAILURE

            if (self.shutdownRequest and (self.tasksLaunched == 0)):
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

        logger = logging.getLogger(__name__)
        logger.info('Received Framework Message: {} {} {}'
                    .format(executorId, slaveId, message))


class Job(object):
    """Stores information for a job and can create a Mesos task
    """

    def __init__(self, job_info):
        """Initialize the job information
        """
        # TODO TODO TODO - Maybe switch to using a named tuple for job_info

        self.job_info = job_info

    def check_resources(self, cpus, mem, disk):
        # Compare the job requirements against the offered resources

        if (self.job_info['cpus'] <= cpus and
                self.job_info['mem'] <= mem and
                self.job_info['disk'] <= disk):
            return True

        return False

    def build_command_line(self):
        # TODO TODO TODO - Make this work

        logger = logging.getLogger(__name__)

        # TODO TODO TODO - Somewhere perform order validation
        # Maybe validation isn't needed, because the json format is our API and
        # some code somewhere is writing it and validated through testing
        # so developers can be the only ones causing errors.
        # TODO TODO TODO - Somewhere perform order validation
        # TODO TODO TODO - Somewhere perform order validation
        # TODO TODO TODO - Somewhere perform order validation

        # Create some shortcuts
        order = self.job_info['order']
        order_id = order['order-id']
        customization_options = order['customization-options']
        product_options = order['product-options']

        product_id = order['input-product-id'].replace('_', '-')

        cmd = list()

        # TODO TODO TODO - Get a bunch of this stuff from a configuration file

        # Must specify the entry point, because we are using the Docker
        # containerizer mode of operation within Mesos
        cmd.append('/entrypoint.sh')
        cmd.extend(['cli.py',
                    '--order-id', order['order-id'],
                    '--input-product-id', order['input-product-id'],
                    '--product-type', order['product-type'],
                    '--output-format', customization_options['output-format'],
                    '--input-url', order['input-url']])

        if order['bridge-mode']:
            cmd.append('--bridge-mode')

        if 'customized-source-data' in product_options:
            cmd.append('--include-customized-source-data')

        if 'top-of-atmosphere' in product_options:
            cmd.append('--include-top-of-atmosphere')

        if 'brightness-temperature' in product_options:
            cmd.append('--include-brightness-temperature')

        if 'surface-reflectance' in product_options:
            cmd.append('--include-surface-reflectance')

        # Add in any developer options
        if 'developer-options' in order:
            developer_options = order['developer-options']

            if 'dev-mode' in developer_options:
                cmd.append('--dev-mode')
            if 'dev-intermediate' in developer_options:
                cmd.append('--dev-intermediate')
            if 'debug' in developer_options:
                cmd.append('--debug')

        logger.info(' '.join(cmd))

        return ' '.join(cmd)

    def build_task_id(self):

        order = self.job_info['order']
        return ('ESPA-{}-{}'.format(order['order-id'],
                                    order['input-product-id']))

    def build_task_name(self):

        order = self.job_info['order']
        return ('ESPA {} {}'.format(order['order-id'],
                                    order['input-product-id']))

    def make_task(self, offer):
        """Create a Mesos task from the job information
        """

        # Create some shortcuts
        order = self.job_info['order']

        # Create the container object
        container = MesosPb2.ContainerInfo()
        container.type = 1  # MesosPb2.ContainerInfo.Type.DOCKER

        # TODO TODO TODO - Get a bunch of this stuff from a configuration file
        # Create container volumes
        work_volume = container.volumes.add()
        work_volume.host_path = '/data2/dilley/work-dir'
        work_volume.container_path = '/home/espa/work-dir'
        work_volume.mode = 1  # MesosPb2.Volume.Mode.RW

        output_volume = container.volumes.add()
        output_volume.host_path = '/data2/dilley/output-data'
        output_volume.container_path = '/home/espa/output-data'
        output_volume.mode = 1  # MesosPb2.Volume.Mode.RW

        aux_volume = container.volumes.add()
        aux_volume.host_path = '/usr/local/auxiliaries'
        aux_volume.container_path = '/usr/local/auxiliaries'
        aux_volume.mode = 2  # MesosPb2.Volume.Mode.RO

        input_volume = container.volumes.add()
        input_volume.host_path = '/data2/dilley/input-data'
        input_volume.container_path = '/home/espa/input-data'
        input_volume.mode = 2  # MesosPb2.Volume.Mode.RO

        config_volume = container.volumes.add()
        config_volume.host_path = '/home/dilley/.usgs'
        config_volume.container_path = '/home/espa/.usgs'
        config_volume.mode = 2  # MesosPb2.Volume.Mode.RO

        # Specify container Docker image
        docker_cfg = self.job_info['docker']
        docker = MesosPb2.ContainerInfo.DockerInfo()
        docker.image = ':'.join([docker_cfg['image'], docker_cfg['tag']])
        docker.network = 2  # MesosPb2.ContainerInfo.DockerInfo.Network.BRIDGE
        docker.force_pull_image = False

        # Temporary hardcode group 501
        user_param = docker.parameters.add()
        user_param.key = 'user'
        user_param.value = '{}:501'.format(os.getuid())

        workdir_param = docker.parameters.add()
        workdir_param.key = 'workdir'
        workdir_param.value = '/home/espa/work-dir'

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
        command.uris.add().value = '/home/dilley/dockercfg.tar.gz'

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
        cpus.scalar.value = self.job_info['cpus']

        mem = task.resources.add()
        mem.name = 'mem'
        mem.type = MesosPb2.Value.SCALAR
        mem.scalar.value = self.job_info['mem']

        disk = task.resources.add()
        disk.name = 'disk'
        disk.type = MesosPb2.Value.SCALAR
        disk.scalar.value = self.job_info['disk']

        return task


def get_jobs(job_filename):
    """Reads jobs from a known job file location
    """

    jobs = list()

    if job_filename and os.path.isfile(job_filename):
        with open(job_filename, 'r') as input_fd:
            data = input_fd.read()

        job_dict = json.loads(data)
        del data

        for job in job_dict['jobs']:
            jobs.append(Job(job))

        os.unlink(job_filename)

    return jobs


def retrieve_command_line():
    """Read and return the command line arguments
    """

    description = 'ESPA Mesos Framework'
    parser = ArgumentParser(description=description)

    parser.add_argument('--master-node',
                        action='store',
                        dest='master_node',
                        required=True,
                        metavar='IP',
                        help='IP address of the Mesos master node')

    parser.add_argument('--job-filename',
                        action='store',
                        dest='job_filename',
                        required=False,
                        metavar='TEXT',
                        help='JSON job file to use')

    parser.add_argument('--verbose',
                        action='store_true',
                        dest='verbose',
                        required=False,
                        default=False,
                        help='Log verbose messages')

    return parser.parse_args()


def espa_framework():
    """Establish framework information
    """

    framework = MesosPb2.FrameworkInfo()
    framework.user = ''
    framework.name = 'ESPA Framework'
    framework.principal = 'espa-mesos-framework'

    return framework


def espa_executor():
    """Establish executor information
    """

    executor = MesosPb2.ExecutorInfo()
    executor.executor_id.value = 'default'
    executor.name = 'ESPA Executor'

    return executor


def main():
    """Main processing routine for the application
    """

    # Disable annoying INFO messages from the requests module
    logging.getLogger("requests").setLevel(logging.WARNING)

    args = retrieve_command_line()

    framework = espa_framework()
    executor = espa_executor()

    mesos_scheduler = ESPA_Scheduler(1, executor, args)
    driver = MesosSchedulerDriver(mesos_scheduler, framework,
                                  args.master_node)

    status = 0
    if driver.run() != MesosPb2.DRIVER_STOPPED:
        status = 1

    driver.stop()

    return status


if __name__ == '__main__':
    main()
