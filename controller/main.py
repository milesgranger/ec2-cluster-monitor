import sys
import boto3
import socket
import time

from datetime import datetime, timedelta
from operator import itemgetter


class Controller(object):

    def __init__(self, instance_name_tags, boto3_profile='default', cpu_thresh=0.5, shutdown_cpu_thresh=0.005,
                 cool_down_mins=5, lookback_mins=10, time_between_checks_mins=1):
        """
        Control nodes based on CPU usage with the supplied parameters.

        **IMPORTANT**
        It is assumed the central node, which is never shutdown, is the first name tag in the list "instance_name_tags"
        All other nodes will be controlled based on CPU usage.

        :param instance_name_tags: list of name tags given to the instances
        :param boto3_profile: string name of the boto3 profile to use in the ~/.aws credentials file
        :param cpu_thresh: float between 0-1 for % of CPU load required to initiate starting another node
        :param shutdown_cpu_thresh: float between 0-1 for % required for cool_down_mins before node is shutdown
        :param cool_down_mins: float/int for number of minutes a node has to be under shutdown_cpu_thresh before being shutdown
        :param lookback_mins: float/int number of minutes to look back at a node's CPU history
        :param time_between_checks_mins: how long program should sleep before rechecking all nodes
        """
        self.session = boto3.Session(profile_name=boto3_profile)
        self.ec2 = self.session.resource('ec2')
        self.cloud_watch = self.session.client('cloudwatch')
        self.instance_name_tags = instance_name_tags
        self.boto3_profile = boto3_profile
        self.cpu_thresh = cpu_thresh
        self.shutdown_cpu_thresh = shutdown_cpu_thresh
        self.cool_down_mins = cool_down_mins
        self.host_name = socket.getfqdn()
        self.instances = []
        self.lookback_mins = lookback_mins
        self.time_between_checks_mins = time_between_checks_mins

        self.acquire_instances()


    def acquire_instances(self):
        """Get a list of the instances"""
        # Create instance and get instance id

        # Find the instance, one by one to ensure error if one is missed; letting the user know.
        for name_tag in self.instance_name_tags:
            instance = [inst for inst in self.ec2.instances.filter(Filters=[
                {'Name': 'tag:Name', 'Values': [name_tag, ]}
            ])]
            instance = instance[0] if instance else None
            if instance is None:
                raise AttributeError('Could not find the instance with Name tag "{}"'.format(name_tag))
            else:
                sys.stdout.write('[{host_name}:] Found instance "{name_tag}" --> {state}\n'
                                 .format(host_name=self.host_name, name_tag=name_tag, state=instance.state.get('Name')))
                self.instances.append(instance)

        # Turn on detailed monitoring so we can get < 10 min updates on CPU usage.
        [inst.monitor() for inst in self.instances]


    def get_useage(self, instance_id):
        """
        Return the CPU usage over last self.n_lookback_mins given instance_id
        returns float (percent of cpu use) or -1 if no results were returned.
        """
        now = datetime.utcnow()
        past = now - timedelta(minutes=self.lookback_mins)
        future = now + timedelta(minutes=1)
        results = self.cloud_watch.get_metric_statistics(
            Namespace='AWS/EC2',
            MetricName='CPUUtilization',
            Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
            StartTime=past,
            EndTime=future,
            Period=300,
            Statistics=['Average']
        )
        datapoints = results['Datapoints']
        if datapoints:
            last_datapoint = sorted(datapoints, key=itemgetter('Timestamp'))[-1]
            cpu_load = last_datapoint['Average'] / 100.0
            return cpu_load
        else:
            return -1


    def start_or_stop_instance(self, instance, max_wait=180, operation='start'):
        """
        Given the value of the "Name" tag for an instance, will start this instance.
        :param instance (obj): Boto3 ec2 recource object to a specific instance
        :param max_wait (int): Max seconds to wait before raising an error waiting for instance to start.
        :param operation (str): String of either 'start', 'stop' or 'get'. with get, the instance is returned only not stop or starting done do it.
        :return: True if successful, raises error if unable to find or start instance.
        """
        # Start the instance
        if operation.lower().strip() == 'start':
            if not instance.state.get('Name') == 'running':
                instance.start()

                # Wait for instance to be ready before returning.
                s = time.clock()
                while instance.state.get('Name') != 'running':
                    if time.clock() - s > max_wait:
                        raise RuntimeError(
                            'Found instance: {}, but waited more than max_wait of {} for it to start; never started'
                            .format(instance.private_dns_name, max_wait))
                    time.sleep(5)
                    instance.reload()
                sys.stdout.write('[{}:] Remote instance {} is now in running state!\n'
                                 .format(self.host_name, instance.private_dns_name))
            else:
                sys.stdout.write('[{}:] Remote instance {} already in running state!\n'
                                 .format(self.host_name, instance.private_dns_name))

        # Turn off the instance
        else:
            instance.stop(Force=True)
            sys.stdout.write('[{}:] Stopped instance {}.\n'.format(self.host_name, instance.private_dns_name))

        return instance


    def monitor(self):
        """
        Never ending loop to controller and control instances based on instance settings
        """
        sys.stdout.write('[{}:] Starting monitoring service..\n'.format(self.host_name))
        # List of timestamps or None types for flagging that an instance is scheduled to shutdown
        stop_flags = {inst.id: None for inst in self.instances}
        while True:
            [inst.reload() for inst in self.instances]



            # Loop for starting instances...
            for i, instance in enumerate(self.instances):

                # If this instance is over cpu useage, and all other running ones are as well, start another one.
                if all([self.get_useage(inst.id) > self.cpu_thresh for inst in self.instances if inst.state.get('Name') == 'running']):

                    # Find an instance that isn't running and start it, or log there are no instances left.
                    instance_to_start = next(iter([inst for inst in self.instances if inst.state.get('Name') == 'stopped']), None)
                    if instance_to_start is not None:
                        cpu_load = self.get_useage(instance_id=instance.id)
                        sys.stdout.write('[{}:] Instance {} over CPU thresh: {} now at {}, starting another instance: {}\n'
                                         .format(self.host_name, instance.private_dns_name, self.cpu_thresh, cpu_load, instance_to_start.private_dns_name)
                                         )
                        self.start_or_stop_instance(instance_to_start)
                        stop_flags[instance_to_start.id] = None  # Ensure the stop flag is none for now
                        time.sleep(60 * 5)  # Sleep for 5 minutes, give vm time to start and cpu to settle.
                    else:
                        sys.stdout.write('[{hostname}:] All nodes are currently over CPU threshold!\n'
                                         .format(hostname=self.host_name))
                    break  # fundamental change to cluster took place.. break out to give time to adjust.




            # Loop for stopping instances
            for i, instance in enumerate(self.instances):
                if i == 0 or instance.state.get('Name') != 'running': continue  # Never shutdown central node.

                # If the usage is under the shutdown thresh, has been for over 'cool_down_mins',
                # and at least one other node is below cpu_thresh as well, then shut it down.
                if (self.get_useage(instance_id=instance.id) <= self.shutdown_cpu_thresh
                    and any([self.get_useage(inst.id) < self.cpu_thresh for inst in self.instances if inst.state.get('Name') == 'running' and inst.id != instance.id])
                    ):
                    if stop_flags[instance.id] is not None and (time.clock() - stop_flags[instance.id]) / 60 > self.cool_down_mins:
                        self.start_or_stop_instance(instance, operation='stop')
                    elif stop_flags[instance.id] is None:
                        sys.stdout.write('[{}:] Placing instance {} into cool down queue, will shutoff in {} mins if other cluster nodes\' CPUs stay below cpu_thresh.\n'
                                         .format(self.host_name, instance.private_dns_name, self.cool_down_mins))
                        stop_flags[instance.id] = time.clock()

                # If it's over the min_cpu_thresh, make sure it has no stop time, in case it was put into the shutdown
                # queue during some point before cpu going back up.
                else:
                    if stop_flags[instance.id] is not None:  # put into the queue before but taken out because it might have been needed again
                        sys.stdout.write('[{}:] Taking instance {} out of shutdown queue.\n'
                                         .format(self.host_name, instance.private_dns_name))
                    stop_flags[instance.id] = None

            # Sleep before starting the next round of checks
            time.sleep(self.time_between_checks_mins)