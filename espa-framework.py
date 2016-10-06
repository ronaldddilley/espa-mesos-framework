#! /usr/bin/env python

'''
File:
    espa-framework.py

Purpose:
    Implements the Mesos Framework for processing data through ESPA.

License:
    NASA Open Source Agreement 1.3
'''


import os
import sys
import json
import logging
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


class ESPA_Scheduler(MesosScheduler):
    """Implements a Mesos framework scheduler
    """

    def __init__(self, implicitAcknowledgements, executor, job_filename):
        """Scheduler initialization

        Args:
            implicitAcknowledgements <int>: Input (Mesos API)
            executor <ExecutorInfo>: Input (Mesos API)
            job_filename <str>: Input (filename to get the jobs from)
        """

        self.implicitAcknowledgements = implicitAcknowledgements
        self.executor = executor

        self.shutdownRequest = False

        self.tasksLaunched = 0
        self.job_queue = list()

        self.job_filename = job_filename

    def registered(self, driver, frameworkId, masterInfo):
        """The framework was registered so log it

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            frameworkId <?>: Input (Mesos API)
            masterInfo <?>: Input (Mesos API)
        """

        logger = logging.getLogger(__name__)
        logger.info('Registered with framework ID {} on {}'
                    .format(frameworkId.value, masterInfo.hostname))

    def reregistered(self, driver, masterInfo):
        """The framework was registered so log it

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            masterInfo <?>: Input (Mesos API)
        """

        logger = logging.getLogger(__name__)
        logger.info('Re-Registered to master on {}'
                    .format(masterInfo.getHostname()))

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

        # If the job queue is empty, check for more
        if not self.job_queue:
            self.job_queue.extend(get_jobs(self.job_filename))

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

            if self.job_queue[0].check_resources(offerCpus,
                                                 offerMem,
                                                 offerDisk):
                job = self.job_queue.pop(0)
                task = job.make_task(offer)
                self.tasksLaunched += 1
                driver.launchTasks(offer.id, [task])
                job.submitted = True

                # TODO TODO TODO - Call the ESPA API server to update the state
                #     Set PROCESSING

            else:
                driver.declineOffer(offer.id)

        logger.info('Queued job count {}'.format(len(self.job_queue)))
        logger.info('Running job count {}'.format(self.tasksLaunched))


    def statusUpdate(self, driver, update):
        """Update task status

        Args:
            driver <MesosSchedulerDriver>: Input (Mesos API)
            update <?>: Input (Mesos API)
        """

        logger = logging.getLogger(__name__)
        logger.info('Task {} is in state {}'
                    .format(update.task_id.value,
                            MesosPb2.TaskState.Name(update.state)))

        if (update.state == MesosPb2.TASK_FINISHED or
                update.state == MesosPb2.TASK_LOST or
                update.state == MesosPb2.TASK_KILLED or
                update.state == MesosPb2.TASK_ERROR or
                update.state == MesosPb2.TASK_FAILED):

            self.tasksLaunched -= 1

            # TODO TODO TODO - Call the ESPA API server to update the state
            # if update.state == MesosPb2.TASK_FINISHED:
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

    def __init__(self, job_info=None):
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
        # TODO TODO TODO - Somewhere perform order validation
        # TODO TODO TODO - Somewhere perform order validation
        # TODO TODO TODO - Somewhere perform order validation

        # Create some shortcuts
        order = self.job_info['order']
        docker_cfg = self.job_info['docker']
        order_id = order['order-id']
        customization_options = order['customization-options']
        product_options = order['product-options']

        product_id = order['input-product-id'].replace('_', '-')

        # TODO TODO TODO - Get a bunch of this stuff from a configuration file
        #       '--user', '{}:{}'.format(os.getuid(), os.getgid()),
        # Temporary use group 501
        cmd = ['docker', 'run', '--rm',
               '--user', '{}:501'.format(os.getuid()),
               '--tty',
               '--hostname {}-cli'.format('-'.join([product_id, order_id])),
               '--name {}-cli'.format('-'.join([product_id, order_id])),
               '--volume /data2/dilley/work-dir:/home/espa/work-dir:rw',
               '--volume /data2/dilley/output-data:/home/espa/output-data:rw',
               '--volume /usr/local/auxiliaries:/usr/local/auxiliaries:ro',
               '--volume /data2/dilley/input-data:/home/espa/input-data:ro',
               '--volume /home/dilley/.usgs:/home/espa/.usgs:ro',
               '--workdir /home/espa/work-dir']

        cmd.append(':'.join([docker_cfg['image'], docker_cfg['tag']]))

        cmd.extend(['cli.py',
                    '--order-id', order['order-id'],
                    '--input-product-id', order['input-product-id'],
                    '--product-type', order['product-type'],
                    '--output-format', customization_options['output-format'],
                    '--input-url', order['input-url']])

        if 'customized-source-data' in product_options:
            cmd.append('--include-customized-source-data')

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

#        # Create the container object
#        container = MesosPb2.ContainerInfo()
#        container.type = 1  # MesosPb2.ContainerInfo.Type.DOCKER

#        # TODO TODO TODO - Get a bunch of this stuff from a configuration file
#        # Create container volumes
#        work_volume = container.volumes.add()
#        work_volume.host_path = '/data2/dilley/work-dir'
#        work_volume.container_path = '/home/espa/work-dir'
#        work_volume.mode = 1  # MesosPb2.Volume.Mode.RW

#        output_volume = container.volumes.add()
#        output_volume.host_path = '/data2/dilley/output-data'
#        output_volume.container_path = '/home/espa/output-data'
#        output_volume.mode = 1  # MesosPb2.Volume.Mode.RW

#        aux_volume = container.volumes.add()
#        aux_volume.host_path = '/usr/local/auxiliaries'
#        aux_volume.container_path = '/usr/local/auxiliaries'
#        aux_volume.mode = 2  # MesosPb2.Volume.Mode.RO

#        input_volume = container.volumes.add()
#        input_volume.host_path = '/data2/dilley/input-data'
#        input_volume.container_path = '/home/espa/input-data'
#        input_volume.mode = 2  # MesosPb2.Volume.Mode.RO

#        config_volume = container.volumes.add()
#        config_volume.host_path = '/home/dilley/.usgs'
#        config_volume.container_path = '/home/espa/.usgs'
#        config_volume.mode = 2  # MesosPb2.Volume.Mode.RO

#        # Specify container Docker image
#        docker_cfg = self.job_info['docker']
#        docker = MesosPb2.ContainerInfo.DockerInfo()
#        docker.image = ':'.join([docker_cfg['image'], docker_cfg['tag']])
#        docker.network = 2  # MesosPb2.ContainerInfo.DockerInfo.Network.BRIDGE
#        docker.force_pull_image = False

#        container.docker.MergeFrom(docker)

        # Create the task object
        task = MesosPb2.TaskInfo()
        task.task_id.value = self.build_task_id()
        task.slave_id.value = offer.slave_id.value
        task.name = self.build_task_name()

#        # Add the container
#        task.container.MergeFrom(container)


        # Specify the command line to execute the Docker container
        command = MesosPb2.CommandInfo()
        command.value = self.build_command_line()

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

    args = retrieve_command_line()

    framework = espa_framework()
    executor = espa_executor()

    mesos_scheduler = ESPA_Scheduler(1, executor, args.job_filename)
    driver = MesosSchedulerDriver(mesos_scheduler, framework,
                                  args.master_node)

    status = 0
    if driver.run() != MesosPb2.DRIVER_STOPPED:
        status = 1

    driver.stop()

    return status


if __name__ == '__main__':
    main()
