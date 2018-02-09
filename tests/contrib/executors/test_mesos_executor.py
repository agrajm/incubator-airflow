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
from unittest import mock

from airflow import configuration
from queue import Queue

from airflow.contrib.executors.mesos_executor import AirflowMesosScheduler

try:
    import mesos.interface
    from mesos.interface import mesos_pb2
    import mesos.native
except ImportError:
    mock_mesos = None


class MesosExecutorTest(unittest.TestCase):
    FRAMEWORK_ID = 'fake_framework_id'

    @unittest.skipIf(mock_mesos is None, "mesos python eggs are not present")
    @mock_mesos
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

    @unittest.skipIf(mock_mesos is None, "mesos python eggs are not present")
    @mock.patch('mesos.native.MesosSchedulerDriver')
    @mock_mesos
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
            name='fake-cpu-resource',
            type=mesos_pb2.Value(
                type=mesos_pb2.Value.Type(0),  # or SCALAR
                scalar=mesos_pb2.Value.Scalar(
                    value=2
                )
            )
        )
        fake_mem_resource = mesos_pb2.Resource(
            name='fake-mem-resource',
            type=mesos_pb2.Value(
                type=mesos_pb2.Value.Type('SCALAR'),
                scalar=mesos_pb2.Value.Scalar(
                    value=4
                )
            )
        )
        resources.append(self, fake_cpu_resource)
        resources.append(self, fake_mem_resource)
        fake_offer = mesos_pb2.Offer(
            id=mesos_pb2.OfferId(value=1),
            framework_id=self.framework_id,
            slave_id=mesos_pb2.SlaveID(value='fake-slave-id'),
            hostname='fake-host',
            resources=resources
        )
        scheduler.resourceOffers(driver, [fake_offer])

        #assertions
        self.assertTrue(driver.launchTasks.called)


if __name__ == '__main__':
    unittest.main()
