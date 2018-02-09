# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

try:
    import mesos.interface
    from mesos.interface import mesos_pb2
    import mesos.native
    from airflow.contrib.executors.mesos_executor import AirflowMesosScheduler
    mock_mesos = True
except ImportError:
    mock_mesos = None

from airflow import configuration
from queue import Queue


class MesosExecutorTest(unittest.TestCase):
    FRAMEWORK_ID = 'fake_framework_id'

    @unittest.skipIf(mock_mesos is None, "mesos python eggs are not present")
    def setUp(self):
        configuration.load_test_config()
        self.framework_id = mesos_pb2.FrameworkID(value=self.FRAMEWORK_ID)
        self.framework_info = mesos_pb2.FrameworkInfo(
            user='fake_user',
            name='fake_framework_name',
        )
        self.command_info = mesos_pb2.CommandInfo(value='fake-command')
        self.executor_id = mesos_pb2.ExecutorID(value='fake-executor-id')
        self.executor_info = mesos_pb2.ExecutorInfo(
            executor_id=self.executor_id,
            framework_id=self.framework_id,
            command=self.command_info,
        )
        self.slave_id = mesos_pb2.SlaveID(value='fake-slave-id')
        self.offer_id = mesos_pb2.OfferID(value='1')

    @unittest.skipIf(mock_mesos is None, "mesos python eggs are not present")
    @mock.patch('mesos.native.MesosSchedulerDriver')
    def test_mesos_executor(self, driver):
        # create task queue, empty result queue, task_cpu and task_memory
        tasks_queue = Queue()
        fake_af_task1 = {"key1", "airflow run tutorial templated "
                                "2018-01-12T09:48:37.823347 "
                                "--local -sd /tmp/tutorial.py "}
        fake_af_task2 = {"key2", "airflow run tutorial templated "
                                 "2018-01-12T09:48:37.823347 "
                                 "--local -sd /tmp/tutorial.py "}
        tasks_queue.put(fake_af_task1)
        tasks_queue.put(fake_af_task2)
        results_queue = Queue()
        task_cpu = 2
        task_memory = 4
        scheduler = AirflowMesosScheduler(tasks_queue,
                                          results_queue,
                                          task_cpu,
                                          task_memory)
        # Create Offers
        resources = []
        fake_cpu_resource = mesos_pb2.Resource(
            name='cpus',
            type=mesos_pb2.Value.SCALAR,
        )
        fake_cpu_resource.scalar.value = task_cpu
        fake_mem_resource = mesos_pb2.Resource(
            name='mem',
            type=mesos_pb2.Value.SCALAR,
        )
        fake_mem_resource.scalar.value = task_memory
        resources.append(fake_cpu_resource)
        resources.append(fake_mem_resource)
        fake_offer = mesos_pb2.Offer(
            id=self.offer_id,
            framework_id=self.framework_id,
            slave_id=self.slave_id,
            hostname='fake-host',
            resources=resources
        )
        scheduler.resourceOffers(driver, [fake_offer])

        #assertions
        self.assertTrue(driver.launchTasks.called)

        # Create tasks that the driver is launched with
        task = mesos_pb2.TaskInfo()
        tid = 0
        task.task_id.value = str(tid)
        task.slave_id.value = self.slave_id.value
        task.name = "AirflowTask %d" % tid

        cpus = mesos_pb2.Resource()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = task_cpu

        mem = mesos_pb2.Resource()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = task_memory

        task.resources.extend([cpus, mem])

        # test config contains the puckel/docker-airflow image
        container = mesos_pb2.ContainerInfo(
            type = mesos_pb2.ContainerInfo.DOCKER,
            docker = mesos_pb2.ContainerInfo.DockerInfo(
                image='test/docker-airflow'
            )
        )
        task.container.MergeFrom(container)

        # assert that driver is called with this task info
        driver.assert_called_with(self.offer_id.value, [task])


if __name__ == '__main__':
    unittest.main()
