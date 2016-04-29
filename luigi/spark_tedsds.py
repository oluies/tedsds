# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import random

import luigi
import luigi.format
import luigi.contrib.hdfs
from luigi.contrib.spark import SparkSubmitTask


class TEDSDSPrepareData(SparkSubmitTask):
    """
    This task runs a :py:class:`luigi.contrib.spark.SparkSubmitTask` task
    over the target data returned by :py:meth:`~/.UserItemMatrix.output` and
    writes the result into its :py:meth:`~.SparkALS.output` target (a file in HDFS).

    This class uses :py:meth:`luigi.contrib.spark.SparkSubmitTask.run`.

    Example luigi configuration::

        [spark]
        spark-submit: /usr/local/spark/bin/spark-submit
        master: yarn-client

    """
    inputfile = luigi.Parameter(default='/share/tedsds/input/train_FD001.txt')
    outputfile = luigi.Parameter(default='/share/tedsds/scaleddf')

    driver_memory = '2g'
    executor_memory = '3g'
    num_executors = luigi.IntParameter(default=100)

    app = '/home/xadmin/src/combient/tedsds/target/scala-2.10/tedsds-assembly-1.0.jar'
    entry_class = 'com.combient.sparkjob.tedsds.PrepareData'

    def app_options(self):
        # These are passed to the Spark main args in the defined order.
        return [self.input().path, self.output().path]

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in HDFS.

        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        # The corresponding Spark job outputs as GZip format.
        return luigi.contrib.hdfs.HdfsTarget(self.outputfile)

    def input(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in HDFS.

        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        # The corresponding Spark job outputs as GZip format.
        return luigi.contrib.hdfs.HdfsTarget(self.inputfile)

class TEDSDSMulticlassMetricsFortedsds(SparkSubmitTask):
    """
    This task runs a :py:class:`luigi.contrib.spark.SparkSubmitTask` task
    over the target data returned by :py:meth:`~/.UserItemMatrix.output` and
    writes the result into its :py:meth:`~.SparkALS.output` target (a file in HDFS).

    This class uses :py:meth:`luigi.contrib.spark.SparkSubmitTask.run`.

    Example luigi configuration::

        [spark]
        spark-submit: /usr/local/spark/bin/spark-submit
        master: yarn-client

    """
    inputfile = luigi.Parameter(default='/share/tedsds/scaleddf')
    outputfile = luigi.Parameter(default=' /share/tedsds/savedmodel')

    driver_memory = '2g'
    executor_memory = '3g'
    num_executors = luigi.IntParameter(default=100)

    app = '/home/xadmin/src/combient/tedsds/target/scala-2.10/tedsds-assembly-1.0.jar'
    entry_class = 'com.combient.sparkjob.tedsds.MulticlassMetricsFortedsds'

    def app_options(self):
        # These are passed to the Spark main args in the defined order.
        return [self.input().path, self.output().path]

    def requires(self):
        """
        This task's dependencies:

        * :py:class:`~.UserItemMatrix`

        :return: object (:py:class:`luigi.task.Task`)
        """
        return TEDSDSPrepareData()

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in HDFS.

        :return: the target output for this task.
        :rtype: object (:py:class:`~luigi.target.Target`)
        """
        # The corresponding Spark job outputs as GZip format.
        return luigi.contrib.hdfs.HdfsTarget(self.outputfile)

class PrepareAllModels(luigi.WrapperTask):
    def requires(self):
        yield TEDSDSMulticlassMetricsFortedsds('/share/tedsds/scaleddftest_*','/share/tedsds/modeltest_all')

class PrepareAllData(luigi.WrapperTask):
    def requires(self):
        return [TEDSDSPrepareData('/share/tedsds/input/test_FD001.txt','/share/tedsds/scaleddftest_FD001/'),
            TEDSDSPrepareData('/share/tedsds/input/test_FD002.txt','/share/tedsds/scaleddftest_FD002/'),
            TEDSDSPrepareData('/share/tedsds/input/test_FD003.txt','/share/tedsds/scaleddftest_FD003/'),
            TEDSDSPrepareData('/share/tedsds/input/test_FD004.txt','/share/tedsds/scaleddftest_FD004/'),
            TEDSDSPrepareData('/share/tedsds/input/train_FD001.txt','/share/tedsds/scaleddftrain_FD001/'),
            TEDSDSPrepareData('/share/tedsds/input/train_FD002.txt','/share/tedsds/scaleddftrain_FD002/'),
            TEDSDSPrepareData('/share/tedsds/input/train_FD003.txt','/share/tedsds/scaleddftrain_FD003/'),
            TEDSDSPrepareData('/share/tedsds/input/train_FD004.txt','/share/tedsds/scaleddftrain_FD004/')]



